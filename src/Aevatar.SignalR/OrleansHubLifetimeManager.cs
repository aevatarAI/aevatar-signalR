using Aevatar.Core.Abstractions.Extensions;
using Aevatar.SignalR.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Newtonsoft.Json;
using Orleans.Streams;

namespace Aevatar.SignalR;

// TODO: Is this thing called in a threadsafe manner by signalR? 
public sealed class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable where THub : Hub
{
    private Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private readonly IClusterClient _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);

    // 不要改, 这个是官方实现
    private readonly HubConnectionStore _connections = new();

    private IStreamProvider? _streamProvider;
    private IAsyncStream<ClientMessage> _serverStream = default!;
    private IAsyncStream<AllMessage> _allStream = default!;
    private Timer _timer = default!;

    public OrleansHubLifetimeManager(
        ILogger<OrleansHubLifetimeManager<THub>> logger,
        IClusterClient clusterClient
    )
    {
        var hubType = typeof(THub).BaseType?.GenericTypeArguments.FirstOrDefault() ?? typeof(THub);
        _hubName = hubType.IsInterface && hubType.Name[0] == 'I'
            ? hubType.Name[1..]
            : hubType.Name;

        _logger = logger;
        _clusterClient = clusterClient;

        _logger.LogDebug("Created Orleans HubLifetimeManager {hubName})", _hubName);
    }

    private Task HeartbeatCheck()
    {
        _logger.LogInformation("Heartbeat check for Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
            _hubName, _serverId);
        return _clusterClient.GetServerDirectoryGrain().Heartbeat(_serverId);
    }

    private async Task EnsureStreamSetup()
    {
        if (_streamProvider is not null)
        {
            _logger.LogDebug("Stream setup already complete for Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
            return;
        }

        await _streamSetupLock.WaitAsync();

        try
        {
            if (_streamProvider is not null)
                return;

            _serverId = _serverId == Guid.Empty ? Guid.NewGuid() : _serverId;

            _logger.LogInformation(
                "Initializing: Orleans HubLifetimeManager {hubName} (serverId: {serverId})...",
                _hubName, _serverId);

            _streamProvider = _clusterClient.GetOrleansSignalRStreamProvider();
            _serverStream = _streamProvider.GetServerStream(_serverId);
            _allStream = _streamProvider.GetAllStream(_hubName);

            _timer = new Timer(
                async _ => await HeartbeatCheck(), null, TimeSpan.FromSeconds(0),
                TimeSpan.FromMinutes(SignalROrleansConstants.ServerHeartbeatPulseInMinutes));

            var allMessageObserver = new AllMessageObserver(ProcessAllMessage);
            var allStreamHandle = await _allStream.SubscribeAsync(allMessageObserver);
            _logger.LogDebug("Subscribed to all stream: StreamId - {streamId}, HandleId - {handleId}, ProviderName - {providerName}",
                allStreamHandle.StreamId, allStreamHandle.HandleId, allStreamHandle.ProviderName);
            var clientMessageObserver = new ClientMessageObserver(ProcessServerMessage);
            var serverStreamHandle = await _serverStream.SubscribeAsync(clientMessageObserver);
            _logger.LogDebug("Subscribed to server stream: StreamId - {streamId}, HandleId - {handleId}, ProviderName - {providerName}",
                serverStreamHandle.StreamId, serverStreamHandle.HandleId, serverStreamHandle.ProviderName);

            _logger.LogInformation(
                "Initialized complete: Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    private Task ProcessAllMessage(AllMessage allMessage)
    {
        var allTasks = new List<Task>(_connections.Count);
        var payload = allMessage.Message!;
        var eligibleConnections = new List<HubConnectionContext>(_connections.Count);

        // 第一阶段：先筛选出需要发送的连接
        foreach (var connection in _connections)
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
                continue;

            if (allMessage.ExcludedIds == null || !allMessage.ExcludedIds.Contains(connection.ConnectionId))
                eligibleConnections.Add(connection);
        }

        // 如果连接数量很大，考虑使用并行处理
        if (eligibleConnections.Count > 100)
        {
            // 分批处理，避免创建过多并发任务
            const int batchSize = 100;
            for (int i = 0; i < eligibleConnections.Count; i += batchSize)
            {
                var batch = eligibleConnections.Skip(i).Take(batchSize).ToList();
                allTasks.Add(Task.WhenAll(batch.Select(conn => 
                    SendLocal(conn, new ClientNotification(payload.Target, payload.Arguments!.ToStrings())))));
            }
        }
        else
        {
            // 连接数较少时直接处理
            foreach (var connection in eligibleConnections)
                allTasks.Add(SendLocal(connection, new ClientNotification(payload.Target, payload.Arguments!.ToStrings())));
        }

        return Task.WhenAll(allTasks);
    }

    private Task ProcessServerMessage(ClientMessage clientMessage)
    {
        var connection = _connections[clientMessage.ConnectionId];
        _logger.LogDebug("Processing server message for connection {connectionId} on hub {hubName} (serverId: {serverId}) with connection available: {connectionAvailable}",
            clientMessage.ConnectionId, _hubName, _serverId, connection != null);
        return connection == null ? Task.CompletedTask : SendLocal(connection, clientMessage.Message);
    }

    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));

        await EnsureStreamSetup();

        // 使用局部变量记录是否已添加连接，以便在出错时正确清理
        bool connectionAdded = false;
        try
        {
            _connections.Add(connection);
            connectionAdded = true;

            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            
            _logger.LogDebug("Handle connection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);
            
            await client.OnConnect(_serverId);

            if (connection.User?.Identity?.IsAuthenticated == true && !string.IsNullOrEmpty(connection.UserIdentifier))
            {
                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier);
                await user.Add(connection.ConnectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "An error has occurred 'OnConnectedAsync' while adding connection {connectionId} [hub: {hubName} (serverId: {serverId})]",
                connection.ConnectionId, _hubName, _serverId);
            
            // 只有在成功添加连接后才尝试移除
            if (connectionAdded)
                _connections.Remove(connection);
                
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        try
        {
            _logger.LogDebug("Handle disconnection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);
            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            await client.OnDisconnect("hub-disconnect");
        }
        finally
        {
            _connections.Remove(connection);
        }
    }

    public override Task SendAllAsync(string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message));
    }

    public override Task SendAllExceptAsync(string methodName, object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message, excludedConnectionIds));
    }

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionId)) throw new ArgumentNullException(nameof(connectionId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var message = new InvocationMessage(methodName, args);

        var connection = _connections[connectionId];
        if (connection != null)
        {
            return SendLocal(connection, new ClientNotification(methodName, args!.ToStrings()));
        }

        return SendExternal(connectionId, message);
    }

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = connectionIds.Select(c => SendConnectionAsync(c, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Send(methodName, args);
    }

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = groupNames.Select(g => SendGroupAsync(g, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.SendExcept(methodName, args, excludedConnectionIds);
    }

    public override Task SendUserAsync(string userId, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(userId)) throw new ArgumentNullException(nameof(userId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var user = _clusterClient.GetUserGrain(_hubName, userId);
        return user.Send(methodName, args);
    }

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = userIds.Select(u => SendGroupAsync(u, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task AddToGroupAsync(string connectionId, string groupName,
        CancellationToken cancellationToken = default)
    {
        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Add(connectionId);
    }

    public override Task RemoveFromGroupAsync(string connectionId, string groupName,
        CancellationToken cancellationToken = default)
    {
        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Remove(connectionId);
    }

    private Task SendLocal(HubConnectionContext connection, ClientNotification notification)
    {
        _logger.LogInformation(
            "Sending local message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
            connection.ConnectionId, _hubName, _serverId);
        // ReSharper disable once CoVariantArrayConversion
        return connection.WriteAsync(new InvocationMessage(SignalROrleansConstants.ResponseMethodName, notification.Arguments))
            .AsTask();
    }

    private Task SendExternal(string connectionId, InvocationMessage hubMessage)
    {
        var client = _clusterClient.GetClientGrain(_hubName, connectionId);
        return client.Send(hubMessage);
    }

    public void Dispose()
    {
        _logger.LogDebug("Disposing Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
            _hubName, _serverId);

        _timer?.Dispose();

        var toUnsubscribe = new List<Task>();
        if (_serverStream is not null)
        {
            toUnsubscribe.Add(UnsubscribeFromStream(_serverStream));
        }

        if (_allStream is not null)
        {
            toUnsubscribe.Add(UnsubscribeFromStream(_allStream));
        }

        var serverDirectoryGrain = _clusterClient.GetServerDirectoryGrain();
        toUnsubscribe.Add(serverDirectoryGrain.Unregister(_serverId));

        try
        {
            Task.WhenAll(toUnsubscribe).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during unsubscription in Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
        }
    }

    private static Task UnsubscribeFromStream<T>(IAsyncStream<T> stream)
    {
        return Task.Run(async () =>
        {
            try
            {
                var subscriptions = await stream.GetAllSubscriptionHandles();
                await Task.WhenAll(subscriptions.Select(s => s.UnsubscribeAsync()));
            }
            catch (Exception)
            {
                // 流可能已经关闭，忽略异常
            }
        });
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        _logger.LogInformation("Participating in the lifecycle of the silo.");
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => 
           {
               try
               {
                   await EnsureStreamSetup().ConfigureAwait(false);
               }
               catch (Exception ex) when (cts.IsCancellationRequested)
               {
                   _logger.LogWarning(ex, "Setup was cancelled during initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
               catch (Exception ex)
               {
                   _logger.LogError(ex, "Error during initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
           });
    }
}
