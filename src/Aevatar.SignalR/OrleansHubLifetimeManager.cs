using Aevatar.Core.Abstractions.Extensions;
using Aevatar.SignalR.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Newtonsoft.Json;
using Orleans.Streams;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace Aevatar.SignalR;

/// <summary>
/// Orleans实现的SignalR HubLifetimeManager，支持高性能分布式消息传递
/// 通过Orleans Grain和Streaming实现跨节点的SignalR消息分发
/// 设计用于处理高并发场景，包括大量客户端连接、多组广播和用户定向消息
/// </summary>
/// <remarks>
/// 此实现优化了以下几个方面：
/// 1. 高效并发处理：使用并行处理模式，动态负载均衡
/// 2. 资源控制：限制并发任务数，避免资源过度消耗
/// 3. 取消支持：支持优雅取消长时间运行的操作
/// 4. 线程安全：关键操作都加上了线程同步机制
/// 5. 异常处理：优雅处理异常，确保稳定性
/// </remarks>
public sealed class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable where THub : Hub
{
    private Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private readonly IClusterClient _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);
    private readonly SemaphoreSlim _operationLock = new(1, 1);

    // 不要改, 这个是官方实现
    private readonly HubConnectionStore _connections = new();

    private IStreamProvider? _streamProvider;
    private IAsyncStream<ClientMessage> _serverStream = default!;
    private IAsyncStream<AllMessage> _allStream = default!;
    private Timer _timer = default!;
    
    // 添加取消令牌源，用于取消正在处理的任务
    private readonly CancellationTokenSource _disposalTokenSource = new();

    // 用于对批处理任务进行限流
    private readonly SemaphoreSlim _throttleSemaphore = new(8, 8); // 最多8个并发批次处理
    
    // 本地连接缓存，用于快速查找
    private readonly ConcurrentDictionary<string, HubConnectionContext> _connectionCache = new();

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
                "Initializing streams: Orleans HubLifetimeManager {hubName} (serverId: {serverId})...",
                _hubName, _serverId);

            // 添加额外的错误处理和重试逻辑
            int retryCount = 0;
            const int maxRetries = 3;
            
            while (retryCount < maxRetries)
            {
                try
                {
                    _streamProvider = _clusterClient.GetOrleansSignalRStreamProvider();
                    
                    // 验证流提供程序是否有效
                    if (_streamProvider == null)
                    {
                        _logger.LogWarning("Stream provider returned null, retrying...");
                        await Task.Delay(500); // 短暂延迟后重试
                        retryCount++;
                        continue;
                    }
                    
                    _serverStream = _streamProvider.GetServerStream(_serverId);
                    _allStream = _streamProvider.GetAllStream(_hubName);
                    
                    break; // 成功获取流，退出循环
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        _logger.LogError(ex, "Failed to initialize stream provider after {maxRetries} attempts", maxRetries);
                        throw;
                    }
                    
                    _logger.LogWarning(ex, "Error getting stream provider, retry {retryCount}/{maxRetries}", retryCount, maxRetries);
                    await Task.Delay(500 * retryCount); // 每次重试增加延迟
                }
            }

            // 计时器已在Participate方法中创建，这里不需要重新创建

            _logger.LogInformation("Setting up stream observers for hub {hubName}", _hubName);
            
            var allMessageObserver = new AllMessageObserver(ProcessAllMessage);
            var allStreamHandle = await _allStream.SubscribeAsync(allMessageObserver);
            _logger.LogDebug("Subscribed to all stream: StreamId - {streamId}, HandleId - {handleId}, ProviderName - {providerName}",
                allStreamHandle.StreamId, allStreamHandle.HandleId, allStreamHandle.ProviderName);
                
            var clientMessageObserver = new ClientMessageObserver(ProcessServerMessage);
            var serverStreamHandle = await _serverStream.SubscribeAsync(clientMessageObserver);
            _logger.LogDebug("Subscribed to server stream: StreamId - {streamId}, HandleId - {handleId}, ProviderName - {providerName}",
                serverStreamHandle.StreamId, serverStreamHandle.HandleId, serverStreamHandle.ProviderName);

            _logger.LogInformation(
                "Stream initialization complete: Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    private async Task ProcessAllMessage(AllMessage allMessage)
    {
        try 
        {
            var payload = allMessage.Message!;
            var excludedIds = allMessage.ExcludedIds;
            
            // 使用ConcurrentBag存储符合条件的连接，避免多线程安全问题
            var eligibleConnections = new ConcurrentBag<HubConnectionContext>();
            
            // 获取连接集合的快照
            List<HubConnectionContext> connectionSnapshots = new List<HubConnectionContext>();
            await _operationLock.WaitAsync();
            try 
            {
                // HubConnectionStore不支持ToList()，直接使用foreach遍历
                foreach (var connection in _connections)
                {
                    connectionSnapshots.Add(connection);
                }
            }
            finally 
            {
                _operationLock.Release();
            }
            
            // 使用并行过滤，提高大量连接时的性能
            Parallel.ForEach(connectionSnapshots, connection => 
            {
                if (connection.ConnectionAborted.IsCancellationRequested)
                    return;
                    
                if (excludedIds == null || !excludedIds.Contains(connection.ConnectionId))
                    eligibleConnections.Add(connection);
            });
            
            // 提取成数组以便后续处理
            var connectionsArray = eligibleConnections.ToArray();
            
            if (connectionsArray.Length == 0)
                return;
                
            // 根据连接数量选择合适的处理策略
            if (connectionsArray.Length <= 10)
            {
                // 连接数量小，直接处理
                await Task.WhenAll(connectionsArray.Select(conn => 
                    SendLocal(conn, new ClientNotification(payload.Target, payload.Arguments!.ToStrings()))));
            }
            else 
            {
                // 大量连接，使用批处理模式
                const int batchSize = 100;
                
                // 创建处理管道，限制并发度
                var processingBlock = new ActionBlock<HubConnectionContext[]>(
                    async batch => 
                    {
                        try 
                        {
                            await _throttleSemaphore.WaitAsync(_disposalTokenSource.Token);
                            try 
                            {
                                await Task.WhenAll(batch.Select(conn => 
                                    SendLocal(conn, new ClientNotification(payload.Target, payload.Arguments!.ToStrings()))));
                            }
                            finally 
                            {
                                _throttleSemaphore.Release();
                            }
                        }
                        catch (OperationCanceledException) 
                        {
                            // 处理已被取消
                        }
                    },
                    new ExecutionDataflowBlockOptions 
                    { 
                        MaxDegreeOfParallelism = 8,
                        CancellationToken = _disposalTokenSource.Token
                    });
                
                // 将连接分批次发送到处理块
                for (int i = 0; i < connectionsArray.Length; i += batchSize)
                {
                    var batchConnections = connectionsArray
                        .Skip(i)
                        .Take(Math.Min(batchSize, connectionsArray.Length - i))
                        .ToArray();
                        
                    await processingBlock.SendAsync(batchConnections, _disposalTokenSource.Token);
                }
                
                // 标记完成并等待所有处理完成
                processingBlock.Complete();
                await processingBlock.Completion;
            }
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            _logger.LogError(ex, "Error processing all message in hub {hubName} (serverId: {serverId})", 
                _hubName, _serverId);
        }
    }

    private Task ProcessServerMessage(ClientMessage clientMessage)
    {
        // 将异步方法改回同步方法
        // 尝试从缓存中获取连接，提高查找效率
        if (!_connectionCache.TryGetValue(clientMessage.ConnectionId, out var connection))
        {
            connection = _connections[clientMessage.ConnectionId];
            if (connection != null)
            {
                // 如果连接有效，则添加到缓存中
                _connectionCache.TryAdd(clientMessage.ConnectionId, connection);
            }
        }
        
        _logger.LogDebug("Processing server message for connection {connectionId} on hub {hubName} (serverId: {serverId}) with connection available: {connectionAvailable}",
            clientMessage.ConnectionId, _hubName, _serverId, connection != null);
            
        if (connection == null || connection.ConnectionAborted.IsCancellationRequested)
            return Task.CompletedTask;
            
        try
        {
            return SendLocal(connection, clientMessage.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error sending message to connection {connectionId}", clientMessage.ConnectionId);
            // 从缓存中移除可能已失效的连接
            _connectionCache.TryRemove(clientMessage.ConnectionId, out _);
            return Task.CompletedTask;
        }
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
            await _operationLock.WaitAsync();
            try
            {
                _connections.Add(connection);
                // 添加到快速查找缓存
                _connectionCache.TryAdd(connection.ConnectionId, connection);
                connectionAdded = true;
            }
            finally
            {
                _operationLock.Release();
            }

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
            {
                await _operationLock.WaitAsync();
                try
                {
                    _connections.Remove(connection);
                    // 从缓存中移除
                    _connectionCache.TryRemove(connection.ConnectionId, out _);
                }
                finally
                {
                    _operationLock.Release();
                }
            }
                
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
            await _operationLock.WaitAsync();
            try
            {
                _connections.Remove(connection);
                // 从缓存中移除
                _connectionCache.TryRemove(connection.ConnectionId, out _);
            }
            finally
            {
                _operationLock.Release();
            }
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
        
        if (_disposalTokenSource.IsCancellationRequested)
            return Task.CompletedTask;

        var message = new InvocationMessage(methodName, args);

        // 优先从快速缓存中查找连接
        if (_connectionCache.TryGetValue(connectionId, out var cachedConnection) && 
            !cachedConnection.ConnectionAborted.IsCancellationRequested)
        {
            return SendLocal(cachedConnection, new ClientNotification(methodName, args!.ToStrings()));
        }

        // 从官方集合中查找
        var connection = _connections[connectionId];
        if (connection != null)
        {
            // 添加到缓存以便快速查找
            _connectionCache.TryAdd(connectionId, connection);
            return SendLocal(connection, new ClientNotification(methodName, args!.ToStrings()));
        }

        return SendExternal(connectionId, message);
    }

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (connectionIds == null || connectionIds.Count == 0)
            return Task.CompletedTask;
            
        if (string.IsNullOrWhiteSpace(methodName)) 
            throw new ArgumentNullException(nameof(methodName));
            
        // 合并取消令牌
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposalTokenSource.Token);
        var linkedToken = linkedCts.Token;
        
        // 对于少量连接，直接并行发送
        if (connectionIds.Count <= 10)
        {
            var tasks = connectionIds.Select(c => SendConnectionAsync(c, methodName, args, linkedToken));
            return Task.WhenAll(tasks);
        }
        
        // 对于大量连接，使用TPL Dataflow限制并发度
        return ProcessBatchedConnections(connectionIds, methodName, args, linkedToken);
    }
    
    // 添加批量处理连接的通用方法
    private async Task ProcessBatchedConnections(IReadOnlyList<string> connectionIds, string methodName, object?[] args, 
        CancellationToken cancellationToken)
    {
        try
        {
            // 创建有限并发处理块
            var processingBlock = new TransformBlock<string, Task>(
                connectionId => SendConnectionAsync(connectionId, methodName, args, cancellationToken),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount * 2, 16), // 根据CPU核心数调整并发度
                    CancellationToken = cancellationToken,
                    BoundedCapacity = 100 // 限制缓冲区大小
                });
                
            // 创建完成块，等待所有任务完成
            var completionBlock = new ActionBlock<Task>(
                task => task.ConfigureAwait(false), // 仅等待任务完成
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                    CancellationToken = cancellationToken
                });
                
            // 连接两个块
            processingBlock.LinkTo(completionBlock, new DataflowLinkOptions { PropagateCompletion = true });
            
            // 将所有连接ID发送到处理块
            foreach (var connectionId in connectionIds)
            {
                if (!await processingBlock.SendAsync(connectionId, cancellationToken).ConfigureAwait(false))
                    break; // 如果无法发送，可能是因为块已关闭或取消
            }
            
            // 标记处理块完成并等待所有任务完成
            processingBlock.Complete();
            await completionBlock.Completion.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // 操作被取消，正常流程
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing batched connections in hub {hubName}", _hubName);
            throw;
        }
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
        if (groupNames == null || groupNames.Count == 0)
            return Task.CompletedTask;
            
        if (string.IsNullOrWhiteSpace(methodName)) 
            throw new ArgumentNullException(nameof(methodName));
            
        // 合并取消令牌
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposalTokenSource.Token);
        var linkedToken = linkedCts.Token;
        
        // 对于少量组，直接并行发送
        if (groupNames.Count <= 5)
        {
            var tasks = groupNames.Select(g => SendGroupAsync(g, methodName, args, linkedToken));
            return Task.WhenAll(tasks);
        }
        
        return ProcessBatchedGroups(groupNames, methodName, args, linkedToken);
    }
    
    // 添加批量处理组的通用方法
    private async Task ProcessBatchedGroups(IReadOnlyList<string> groupNames, string methodName, object?[] args, 
        CancellationToken cancellationToken)
    {
        try
        {
            // 创建信号量以限制并发处理的组数量
            using var throttler = new SemaphoreSlim(8, 8);
            var tasks = new List<Task>(groupNames.Count);
            
            foreach (var groupName in groupNames)
            {
                await throttler.WaitAsync(cancellationToken);
                
                // 使用本地函数以捕获当前组名
                async Task ProcessGroup()
                {
                    try
                    {
                        await SendGroupAsync(groupName, methodName, args, cancellationToken);
                    }
                    finally
                    {
                        throttler.Release();
                    }
                }
                
                tasks.Add(ProcessGroup());
            }
            
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            // 操作被取消，正常流程
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing batched groups in hub {hubName}", _hubName);
            throw;
        }
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
        if (userIds == null || userIds.Count == 0)
            return Task.CompletedTask;
            
        if (string.IsNullOrWhiteSpace(methodName)) 
            throw new ArgumentNullException(nameof(methodName));
            
        // 合并取消令牌
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposalTokenSource.Token);
        var linkedToken = linkedCts.Token;
        
        // 对于少量用户，直接并行发送
        if (userIds.Count <= 5)
        {
            var tasks = userIds.Select(u => SendUserAsync(u, methodName, args, linkedToken));
            return Task.WhenAll(tasks);
        }
        
        return ProcessBatchedUsers(userIds, methodName, args, linkedToken);
    }
    
    // 添加批量处理用户的通用方法
    private async Task ProcessBatchedUsers(IReadOnlyList<string> userIds, string methodName, object?[] args, 
        CancellationToken cancellationToken)
    {
        try
        {
            // 创建信号量以限制并发处理的用户数量
            using var throttler = new SemaphoreSlim(8, 8);
            var tasks = new List<Task>(userIds.Count);
            
            foreach (var userId in userIds)
            {
                await throttler.WaitAsync(cancellationToken);
                
                // 使用本地函数以捕获当前用户ID
                async Task ProcessUser()
                {
                    try
                    {
                        await SendUserAsync(userId, methodName, args, cancellationToken);
                    }
                    finally
                    {
                        throttler.Release();
                    }
                }
                
                tasks.Add(ProcessUser());
            }
            
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            // 操作被取消，正常流程
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing batched users in hub {hubName}", _hubName);
            throw;
        }
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
        if (_disposalTokenSource.IsCancellationRequested || connection.ConnectionAborted.IsCancellationRequested)
            return Task.CompletedTask;
            
        try 
        {
            _logger.LogDebug(
                "Sending local message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);
                
            // ReSharper disable once CoVariantArrayConversion
            return connection.WriteAsync(new InvocationMessage(SignalROrleansConstants.ResponseMethodName, notification.Arguments))
                .AsTask();
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            _logger.LogError(ex, "Error sending local message to connection {connectionId}", connection.ConnectionId);
            // 从缓存中移除可能已失效的连接
            _connectionCache.TryRemove(connection.ConnectionId, out _);
            return Task.CompletedTask;
        }
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

        // 取消所有正在处理的任务
        try
        {
            _disposalTokenSource.Cancel();
            _disposalTokenSource.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error cancelling tasks during disposal");
        }

        _timer?.Dispose();
        _operationLock?.Dispose();
        _throttleSemaphore?.Dispose();
        _streamSetupLock?.Dispose();
        
        // 清理连接缓存
        _connectionCache.Clear();

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
            // 使用超时设置，防止卡死
            var unsubscribeTask = Task.WhenAll(toUnsubscribe);
            if (!unsubscribeTask.Wait(TimeSpan.FromSeconds(5)))
            {
                _logger.LogWarning("Unsubscribe operations timed out in Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                    _hubName, _serverId);
            }
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
        
        // 将启动阶段提前，确保在流服务启动前准备好
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.RuntimeInitialize, // 修改为更早的阶段
           onStart: async cts => 
           {
               try
               {
                   // 延迟初始化，确保流服务准备好
                   _serverId = _serverId == Guid.Empty ? Guid.NewGuid() : _serverId;
                   
                   _logger.LogInformation(
                       "Initializing: Orleans HubLifetimeManager {hubName} (serverId: {serverId}) lifecycle...",
                       _hubName, _serverId);
                       
                   // 先简单初始化计时器，不依赖流服务
                   _timer = new Timer(
                       async _ => await HeartbeatCheck().ConfigureAwait(false), null, 
                       TimeSpan.FromSeconds(30), // 延迟启动，给流服务更多初始化时间
                       TimeSpan.FromMinutes(SignalROrleansConstants.ServerHeartbeatPulseInMinutes));
                       
                   _logger.LogInformation(
                       "Basic initialization complete for Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                       _hubName, _serverId);
               }
               catch (Exception ex) when (cts.IsCancellationRequested)
               {
                   _logger.LogWarning(ex, "Basic setup was cancelled during initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
               catch (Exception ex)
               {
                   _logger.LogError(ex, "Error during basic initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
           });
           
        // 添加一个额外的订阅，在应用程序活动阶段完成流设置
        lifecycle.Subscribe(
           observerName: $"{nameof(OrleansHubLifetimeManager<THub>)}.Streams",
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => 
           {
               try
               {
                   await Task.Delay(1000, cts).ConfigureAwait(false); // 给系统一点时间确保所有服务就绪
                   await EnsureStreamSetup().ConfigureAwait(false);
               }
               catch (Exception ex) when (cts.IsCancellationRequested)
               {
                   _logger.LogWarning(ex, "Stream setup was cancelled during initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
               catch (Exception ex)
               {
                   _logger.LogError(ex, "Error during stream initialization of HubLifetimeManager {hubName}", _hubName);
                   throw;
               }
           });
    }
}
