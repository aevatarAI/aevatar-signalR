using Aevatar.Core.Abstractions.Extensions;
using Aevatar.SignalR.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Newtonsoft.Json;
using Orleans.Streams;
using System.Collections.Concurrent;

namespace Aevatar.SignalR;

// Orleans是单线程的Grain模型，但HubLifetimeManager是多线程的
public sealed class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable where THub : Hub
{
    private Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private readonly IClusterClient _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);
    private readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _activeTransfers = new(StringComparer.Ordinal);
    private readonly int _maxParallelTransfers;
    private readonly SemaphoreSlim[] _connectionLocks;
    private readonly int _lockCount = 32; // 使用32个锁划分，减少锁冲突
    private readonly TimeSpan _connectionTimeout = TimeSpan.FromSeconds(10);

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
        _maxParallelTransfers = Environment.ProcessorCount * 2; // 基于处理器数量设置并行传输上限
        
        // 初始化锁数组
        _connectionLocks = new SemaphoreSlim[_lockCount];
        for (int i = 0; i < _lockCount; i++)
        {
            _connectionLocks[i] = new SemaphoreSlim(1, 1);
        }

        _logger.LogDebug("Created Orleans HubLifetimeManager {hubName})", _hubName);
    }

    private Task HeartbeatCheck()
    {
        try
        {
            _logger.LogInformation("Heartbeat check for Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
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
            return;
        }

        try
        {
            await _streamSetupLock.WaitAsync();

            if (_streamProvider is not null)
                return;

            _serverId = _serverId == Guid.Empty ? Guid.NewGuid() : _serverId;

            _logger.LogInformation(
                "Initializing: Orleans HubLifetimeManager {hubName} (serverId: {serverId})...",
                _hubName, _serverId);

            // 使用 Orleans 提供的流提供程序
            _streamProvider = _clusterClient.GetOrleansSignalRStreamProvider();
            _serverStream = _streamProvider.GetServerStream(_serverId);
            _allStream = _streamProvider.GetAllStream(_hubName);

            // 设置心跳定时器，定期向服务器目录报告活跃状态
            _timer = new Timer(
                _ => Task.Run(HeartbeatCheck),
                null,
                TimeSpan.FromSeconds(0),
                TimeSpan.FromMinutes(SignalROrleansConstants.ServerHeartbeatPulseInMinutes));

            // 订阅所有消息流
            var allMessageObserver = new AllMessageObserver(ProcessAllMessage);
            await _allStream.SubscribeAsync(allMessageObserver);

            // 订阅服务器特定的消息流
            var clientMessageObserver = new ClientMessageObserver(ProcessServerMessage);
            await _serverStream.SubscribeAsync(clientMessageObserver);

            _logger.LogInformation(
                "Initialization complete: Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
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
        var tasks = new List<Task>(Math.Min(100, _connections.Count)); // 预分配合理的大小
        
        // 创建连接的快照以避免枚举时修改集合
        var connections = _connections.Values.ToArray();
        
        foreach (var connection in connections)
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
                continue;

            if (allMessage.ExcludedIds == null || !allMessage.ExcludedIds.Contains(connection.ConnectionId))
            {
                tasks.Add(SendLocal(connection, new ClientNotification(payload.Target, payload.Arguments!.ToStrings())));
            }
        }

        // 避免为空列表创建Task.WhenAll
        return tasks.Count > 0 ? Task.WhenAll(tasks) : Task.CompletedTask;
    }

    private Task ProcessServerMessage(ClientMessage clientMessage)
    {
        // 使用TryGetValue避免KeyNotFoundException
        if (_connections.TryGetValue(clientMessage.ConnectionId, out var connection) && 
            !connection.ConnectionAborted.IsCancellationRequested)
        {
            _logger.LogDebug("Processing server message for connection {connectionId} on hub {hubName} (serverId: {serverId})",
                clientMessage.ConnectionId, _hubName, _serverId);
            
            return SendLocal(connection, clientMessage.Message);
        }
        
        return Task.CompletedTask;
    }

    private async Task<bool> TryAcquireTransferSlot(string connectionId, TimeSpan timeout)
    {
        // 获取对应的锁索引
        var lockIndex = Math.Abs(connectionId.GetHashCode() % _lockCount);
        var lockObj = _connectionLocks[lockIndex];
        
        // 如果已存在，则已获取槽位
        if (_activeTransfers.TryGetValue(connectionId, out _))
            return true;
            
        // 尝试获取连接锁
        if (!await lockObj.WaitAsync(timeout))
            return false;
            
        try
        {
            // 再次检查，避免在获取锁的过程中状态变化
            if (_activeTransfers.TryGetValue(connectionId, out _))
                return true;
                
            // 如果活动传输数已达上限，则拒绝新的传输
            if (_activeTransfers.Count >= _maxParallelTransfers)
                return false;
                
            // 尝试添加新传输
            return _activeTransfers.TryAdd(connectionId, 1);
        }
        finally
        {
            lockObj.Release();
        }
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
            // 如果无法获取传输槽位，则延迟处理并重试
            if (!(await TryAcquireTransferSlot(connectionId, _connectionTimeout)))
            {
                _logger.LogWarning("Connection processing delayed due to high load: {connectionId}", connectionId);
                
                // 使用指数退避重试
                TimeSpan delay = TimeSpan.FromMilliseconds(100);
                for (int i = 0; i < 3; i++) // 最多重试3次
                {
                    await Task.Delay(delay);
                    if (await TryAcquireTransferSlot(connectionId, _connectionTimeout))
                        break;
                        
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 1000)); // 最长等待1秒
                }
                
                if (!_activeTransfers.ContainsKey(connectionId))
                {
                    throw new HubException("Server is currently handling too many connections. Please try again later.");
                }
            }
            
            // 添加到本地连接字典
            _connections[connectionId] = connection; // 简化添加逻辑，减少判断

            // 告知 Orleans Grain 系统新连接已建立
            var client = _clusterClient.GetClientGrain(_hubName, connectionId);
            await client.OnConnect(_serverId);

            // 如果用户已验证，则添加到用户组
            if (connection.User?.Identity?.IsAuthenticated == true && !string.IsNullOrEmpty(connection.UserIdentifier))
            {
                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier);
                await user.Add(connectionId);
            }
            
            _logger.LogInformation("Connection {connectionId} successfully established on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error while processing connection {connectionId} on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);
                
            // 确保在任何情况下释放资源
            ReleaseTransferSlot(connectionId);
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        var connectionId = connection.ConnectionId;
        
        try
        {
            _logger.LogDebug("Handling disconnection {connectionId} on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);
                
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

        // 线程安全地获取连接
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
        try
        {
            _timer?.Dispose();
            
            // 释放所有锁资源
            if (_connectionLocks != null)
            {
                foreach (var lockObj in _connectionLocks)
                {
                    lockObj?.Dispose();
                }
            }
            
            // 通知服务器目录此服务器不再活跃
            if (_serverId != Guid.Empty && _clusterClient is { } client)
            {
                try
                {
                    // 使用FireAndForgetExtension确保不阻塞
                    client.GetServerDirectoryGrain().RemoveServer(_serverId).FireAndForget();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error removing server from directory during disposal");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during HubLifetimeManager disposal");
        }
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        _logger.LogInformation("Participating in the lifecycle of the silo.");
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => await Task.Run(EnsureStreamSetup, cts));
    }
}
