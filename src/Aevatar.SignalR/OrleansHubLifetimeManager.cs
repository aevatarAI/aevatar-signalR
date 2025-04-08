using Aevatar.Core.Abstractions.Extensions;
using Aevatar.SignalR.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Newtonsoft.Json;
using Orleans.Streams;
using System.Collections.Concurrent;

namespace Aevatar.SignalR;

public sealed class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable where THub : Hub
{
    private Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private readonly IClusterClient _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);
    private readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new();
    private readonly ConcurrentDictionary<string, byte> _activeTransfers = new();
    private readonly int _maxParallelTransfers;

    private IStreamProvider? _streamProvider;
    private IAsyncStream<ClientMessage> _serverStream = default!;
    private IAsyncStream<AllMessage> _allStream = default!;
    private Timer _timer = default!;

    private static readonly ConcurrentDictionary<string, (int Count, DateTime LastReset)> _ipConnectionCounter = new();
    private const int MaxConnectionsPerSecond = 5;

    private readonly string _instanceId = Guid.NewGuid().ToString("N")[..8];

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
        _maxParallelTransfers = Environment.ProcessorCount * 2; // 基于处理器数量设置并行传输上限

        _logger.LogDebug("Created Orleans HubLifetimeManager - Instance: {InstanceId}, Hub: {HubName}",
            _instanceId, _hubName);
    }

    private Task HeartbeatCheck()
    {
        try
        {
            _logger.LogInformation(
                "Heartbeat check - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}",
                _instanceId, _hubName, _serverId);
            return _clusterClient.GetServerDirectoryGrain().Heartbeat(_serverId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during heartbeat check for hub {hubName} (serverId: {serverId})",
                _hubName, _serverId);
            return Task.CompletedTask;
        }
    }

    private async Task EnsureStreamSetup()
    {
        if (_streamProvider is not null)
        {
            _logger.LogDebug(
                "Stream setup already complete - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}",
                _instanceId, _hubName, _serverId);
            return;
        }

        try
        {
            await _streamSetupLock.WaitAsync();

            if (_streamProvider is not null)
                return;

            _serverId = _serverId == Guid.Empty ? Guid.NewGuid() : _serverId;

            _logger.LogInformation(
                "Initializing Orleans HubLifetimeManager - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}",
                _instanceId, _hubName, _serverId);

            _streamProvider = _clusterClient.GetOrleansSignalRStreamProvider();
            _serverStream = _streamProvider.GetServerStream(_serverId);
            _allStream = _streamProvider.GetAllStream(_hubName);

            _timer = new Timer(
                _ => Task.Run(HeartbeatCheck),
                null, TimeSpan.FromSeconds(0),
                TimeSpan.FromMinutes(SignalROrleansConstants.ServerHeartbeatPulseInMinutes));

            var allMessageObserver = new AllMessageObserver(ProcessAllMessage);
            var allStreamHandle = await _allStream.SubscribeAsync(allMessageObserver);
            _logger.LogDebug(
                "Subscribed to all stream - Instance: {InstanceId}, StreamId: {StreamId}, HandleId: {HandleId}, ProviderName: {ProviderName}",
                _instanceId, allStreamHandle.StreamId, allStreamHandle.HandleId, allStreamHandle.ProviderName);
            var clientMessageObserver = new ClientMessageObserver(ProcessServerMessage);
            var serverStreamHandle = await _serverStream.SubscribeAsync(clientMessageObserver);
            _logger.LogDebug(
                "Subscribed to server stream - Instance: {InstanceId}, StreamId: {StreamId}, HandleId: {HandleId}, ProviderName: {ProviderName}",
                _instanceId, serverStreamHandle.StreamId, serverStreamHandle.HandleId, serverStreamHandle.ProviderName);

            _logger.LogInformation(
                "Initialization complete - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}",
                _instanceId, _hubName, _serverId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Failed to initialize Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
            throw;
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    private Task ProcessAllMessage(AllMessage allMessage)
    {
        if (allMessage.Message == null) 
            return Task.CompletedTask;
        
        var payload = allMessage.Message;
        var allTasks = new List<Task>();

        var connections = _connections.Values.ToList();
        
        foreach (var connection in connections)
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
                continue;

            if (allMessage.ExcludedIds == null || !allMessage.ExcludedIds.Contains(connection.ConnectionId))
            {
                var task = SendLocal(connection, new ClientNotification(payload.Target, payload.Arguments!.ToStrings()));
                allTasks.Add(task);
            }
        }

        return Task.WhenAll(allTasks);
    }

    private Task ProcessServerMessage(ClientMessage clientMessage)
    {
        // 使用TryGetValue避免KeyNotFoundException
        if (_connections.TryGetValue(clientMessage.ConnectionId, out var connection) && 
            !connection.ConnectionAborted.IsCancellationRequested)
        {
            _logger.LogDebug(
                "Processing server message - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}, ConnectionId: {ConnectionId}, Available: {ConnectionAvailable}",
                _instanceId,
                _hubName,
                _serverId,
                clientMessage.ConnectionId,
                connection != null);

            return SendLocal(connection, clientMessage.Message);
        }
        
        return Task.CompletedTask;
    }
    
    private bool IsIpRateLimited(string ipAddress)
    {
        var now = DateTime.UtcNow;
        var (count, lastReset) = _ipConnectionCounter.GetOrAdd(ipAddress, _ => (0, now));
        
        if ((now - lastReset).TotalSeconds >= 1)
        {
            _ipConnectionCounter.TryUpdate(ipAddress, (1, now), (count, lastReset));
            return false;
        }
        
        if (count >= MaxConnectionsPerSecond)
        {
            _logger.LogDebug(
                "IP rate limit exceeded - IP: {IpAddress}, Connections in last second: {Count}, Max allowed: {MaxAllowed}",
                ipAddress,
                count,
                MaxConnectionsPerSecond);
            return true;
        }
        
        _ipConnectionCounter.TryUpdate(ipAddress, (count + 1, lastReset), (count, lastReset));
        return false;
    }

    private async Task<bool> TryAcquireTransferSlot(string connectionId)
    {
        // 如果已存在，则已获取槽位
        if (_activeTransfers.TryGetValue(connectionId, out _))
            return true;
            
        // 如果活动传输数已达上限，则拒绝新的传输
        if (_activeTransfers.Count >= _maxParallelTransfers)
            return false;
            
        // 尝试添加新传输
        return _activeTransfers.TryAdd(connectionId, 1);
    }
    
    private void ReleaseTransferSlot(string connectionId)
    {
        _activeTransfers.TryRemove(connectionId, out _);
    }

    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));
            
        await EnsureStreamSetup();

        var connectionId = connection.ConnectionId;
        
        try
        {
            // 如果无法获取传输槽位，则延迟处理
            if (!(await TryAcquireTransferSlot(connectionId)))
            {
                _logger.LogWarning("Connection processing delayed due to high load: {connectionId}", connectionId);
                await Task.Delay(100);
                
                // 再次尝试获取槽位
                if (!(await TryAcquireTransferSlot(connectionId)))
                {
                    throw new HubException("Server is currently handling too many connections. Please try again later.");
                }
            }
            
            // 添加到本地连接字典
            if (!_connections.TryAdd(connectionId, connection))
            {
                _logger.LogWarning("Connection {connectionId} already exists in dictionary", connectionId);
                var existingConnection = _connections[connectionId];
                // 如果连接已中止，则用新连接替换
                if (existingConnection.ConnectionAborted.IsCancellationRequested)
                {
                    _connections[connectionId] = connection;
                }
            }
            
            

            // 告知 Orleans Grain 系统新连接已建立
            var client = _clusterClient.GetClientGrain(_hubName, connectionId);
            await client.OnConnect(_serverId);

            // 如果用户已验证，则添加到用户组
            if (connection.User?.Identity?.IsAuthenticated == true && !string.IsNullOrEmpty(connection.UserIdentifier))
            {
                _logger.LogDebug(
                    "Orleans Hub - Authenticated user connected - Instance: {InstanceId}, Hub: {HubName}, ConnectionId: {ConnectionId}, User: {UserIdentity}, UserIdentifier: {UserIdentifier}",
                    _instanceId,
                    _hubName,
                    connection.ConnectionId,
                    connection.User.Identity.Name,
                    connection.UserIdentifier);

                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier);
                await user.Add(connectionId);
            }
            
            _logger.LogInformation("Connection {connectionId} successfully established on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "An error has occurred 'OnConnectedAsync' while adding connection - Instance: {InstanceId}, ConnectionId: {ConnectionId}, Hub: {HubName}, ServerId: {ServerId}",
                _instanceId,
                connection?.ConnectionId,
                _hubName,
                _serverId);

            // 确保连接从字典中移除
            _connections.TryRemove(connectionId, out _);
            
            // 如果发生错误，尝试通知客户端并关闭连接
            try
            {
                await connection.WriteAsync(new CloseMessage("Connection failed due to server error"));
            }
            catch (Exception writeEx)
            {
                _logger.LogError(writeEx, "Failed to write error message to connection {connectionId}", connectionId);
            }
            
            throw;
        }
        finally
        {
            // 释放传输槽位
            ReleaseTransferSlot(connectionId);
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        var connectionId = connection.ConnectionId;
        
        try
        {
            _logger.LogDebug(
                "Handle disconnection - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}, ConnectionId: {ConnectionId}",
                _instanceId,
                _hubName,
                _serverId,
                connection.ConnectionId);

            // 从本地连接字典中移除
            _connections.TryRemove(connectionId, out _);

            // 通知 Orleans Grain 系统连接已断开
            var client = _clusterClient.GetClientGrain(_hubName, connectionId);
            await client.OnDisconnect("hub-disconnect");
            
            // 如果用户已验证，则从用户组中移除
            if (connection.User?.Identity?.IsAuthenticated == true && !string.IsNullOrEmpty(connection.UserIdentifier))
            {
                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier);
                await user.Remove(connectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Error while processing disconnection {connectionId} on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);
        }
        finally
        {
            // 释放传输槽位
            ReleaseTransferSlot(connectionId);
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

        if (_connections.TryGetValue(connectionId, out var connection))
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
        var tasks = userIds.Select(u => SendUserAsync(u, methodName, args, cancellationToken));
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
            "Sending local message - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}, ConnectionId: {ConnectionId}",
            _instanceId,
            _hubName,
            _serverId,
            connection.ConnectionId);
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
        _logger.LogDebug(
            "Disposing Orleans HubLifetimeManager - Instance: {InstanceId}, Hub: {HubName}, ServerId: {ServerId}",
            _instanceId,
            _hubName,
            _serverId);

        _timer?.Dispose();

        var toUnsubscribe = new List<Task>();
        if (_serverStream is not null)
        {
            toUnsubscribe.Add(Task.Factory.StartNew(async () =>
            {
                var subscriptions = await _serverStream.GetAllSubscriptionHandles();
                var subs = new List<Task>();
                subs.AddRange(subscriptions.Select(s => s.UnsubscribeAsync()));
                await Task.WhenAll(subs);
            }));
        }

        if (_allStream is not null)
        {
            toUnsubscribe.Add(Task.Factory.StartNew(async () =>
            {
                var subscriptions = await _allStream.GetAllSubscriptionHandles();
                var subs = new List<Task>();
                subs.AddRange(subscriptions.Select(s => s.UnsubscribeAsync()));
                await Task.WhenAll(subs);
            }));
        }

        var serverDirectoryGrain = _clusterClient.GetServerDirectoryGrain();
        toUnsubscribe.Add(serverDirectoryGrain.Unregister(_serverId));
        
        try
        {
            Task.WhenAll(toUnsubscribe.ToArray()).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error unsubscribing from streams during disposal");
        }
        
        _streamSetupLock.Dispose();
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        _logger.LogDebug(
            "Participating in silo lifecycle - Instance: {InstanceId}, Hub: {HubName}",
            _instanceId,
            _hubName);
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => await Task.Run(EnsureStreamSetup, cts));
    }
}
