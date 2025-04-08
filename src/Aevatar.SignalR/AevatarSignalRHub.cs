using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Aevatar.Core.Abstractions;
using Aevatar.SignalR.GAgents;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

namespace Aevatar.SignalR;

// ReSharper disable InconsistentNaming
// [Authorize]
public class AevatarSignalRHub : Hub, IAevatarSignalRHub
{
    private readonly IGAgentFactory _gAgentFactory;
    private readonly ILogger<AevatarSignalRHub> _logger;
    private readonly EventDeserializer _eventDeserializer;
    
    // 使用AsyncLocal来自动关联到当前请求线程上下文
    private static readonly AsyncLocal<ProcessingState> _currentProcessingState = new();
    
    // 使用对象池来减少临时对象的创建，增加池大小以支持更高并发
    private static readonly ObjectPool<SemaphoreSlim> _semaphorePool = 
        new DefaultObjectPool<SemaphoreSlim>(new SemaphoreSlimPoolPolicy(), 100);
        
    // 减少超时时间，避免长时间等待，优化用户体验
    private static readonly TimeSpan SemaphoreTimeout = TimeSpan.FromSeconds(5);
    
    // 添加连接ID缓存，避免频繁创建和查询
    private static readonly ConcurrentDictionary<string, byte> _activeConnections = new();
    
    private class SemaphoreSlimPoolPolicy : IPooledObjectPolicy<SemaphoreSlim>
    {
        public SemaphoreSlim Create() => new(1, 1);

        public bool Return(SemaphoreSlim obj)
        {
            if (obj.CurrentCount == 0)
            {
                try { obj.Release(); } catch { /* 忽略可能的异常 */ }
            }
            return true;
        }
    }
    
    private class ProcessingState : IDisposable
    {
        public SemaphoreSlim Semaphore { get; }
        public string ConnectionId { get; }
        public CancellationTokenSource Cts { get; }
        
        public ProcessingState(SemaphoreSlim semaphore, string connectionId)
        {
            Semaphore = semaphore;
            ConnectionId = connectionId;
            Cts = new CancellationTokenSource();
        }
        
        public void Dispose()
        {
            _semaphorePool.Return(Semaphore);
            try { Cts.Dispose(); } catch { /* 忽略可能的异常 */ }
        }
    }

    public AevatarSignalRHub(IGAgentFactory gAgentFactory, ILogger<AevatarSignalRHub> logger)
    {
        _gAgentFactory = gAgentFactory;
        _logger = logger;
        _eventDeserializer = new EventDeserializer();
    }

    // 优化获取处理状态的方法，使用更短的超时
    private async Task<ProcessingState> InitProcessingStateAsync(string connectionId)
    {
        var semaphore = _semaphorePool.Get();
        
        // 尝试获取信号量，有超时保护
        if (!await semaphore.WaitAsync(SemaphoreTimeout))
        {
            _semaphorePool.Return(semaphore);
            _logger.LogWarning("Timeout waiting for semaphore on connection {ConnectionId}", connectionId);
            throw new HubException("操作因负载过高而超时。请稍后重试。");
        }
        
        var state = new ProcessingState(semaphore, connectionId);
        _currentProcessingState.Value = state;
        return state;
    }

    public async Task<GrainId?> PublishEventAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        var connectionId = GetConnectionId();
        if (string.IsNullOrEmpty(connectionId))
        {
            _logger.LogWarning("PublishEventAsync: Connection ID is null or empty");
            throw new HubException("连接ID无效");
        }
        
        using var state = await InitProcessingStateAsync(connectionId);
        
        try
        {
            _logger.LogInformation("PublishEventAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);
            
            using var _ = new ActivityScope(nameof(PublishEventAsync));

            // 提前启动事件反序列化以提高并行性能
            var eventDeserializeTask = Task.Run(() => DeserializeEvent(eventTypeName, eventJson));
            
            // 减少嵌套，优化异常处理流程，同时启动组成员初始化
            var groupMembersTask = InitializeGroupMembers(grainId);
            
            // 等待两个任务并行完成
            await Task.WhenAll(eventDeserializeTask, groupMembersTask);
            
            var (parentGAgent, signalRGAgent) = groupMembersTask.Result;
            var eventInstance = eventDeserializeTask.Result;
            
            if (parentGAgent is null || signalRGAgent is null)
            {
                _logger.LogWarning("PublishEventAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }
            
            if (eventInstance == null)
            {
                _logger.LogWarning("PublishEventAsync: Failed to deserialize event of type {EventType}", eventTypeName);
                throw new HubException($"无法反序列化类型为 {eventTypeName} 的事件");
            }

            // 并行执行连接注册和代理注册
            var addConnectionTask = AddConnectionIdIfNeeded(signalRGAgent, connectionId, true);
            var registerTask = parentGAgent.RegisterAsync(signalRGAgent);
            
            await Task.WhenAll(addConnectionTask, registerTask);
            
            _logger.LogDebug("SignalRGAgent {SignalRGAgentId} registered to parent {ParentGAgentId}",
                signalRGAgent.GetGrainId(), parentGAgent.GetGrainId());
            
            // 使用带取消令牌的任务，防止长时间挂起
            await signalRGAgent.PublishEventAsync(eventInstance, connectionId)
                .WaitAsync(TimeSpan.FromSeconds(30), state.Cts.Token);
                
            return signalRGAgent.GetGrainId();
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("PublishEventAsync: Operation canceled for connection {ConnectionId}", connectionId);
            throw new HubException("操作已取消");
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("PublishEventAsync: Operation timed out for connection {ConnectionId}", connectionId);
            throw new HubException("操作超时");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PublishEventAsync: Error processing event for connection {ConnectionId}, GrainId {GrainId}",
                connectionId, grainId);
            throw new HubException("处理事件失败: " + ex.Message);
        }
        finally
        {
            _currentProcessingState.Value = null;
        }
    }

    public async Task<GrainId?> SubscribeAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        var connectionId = GetConnectionId();
        if (string.IsNullOrEmpty(connectionId))
        {
            _logger.LogWarning("SubscribeAsync: Connection ID is null or empty");
            throw new HubException("连接ID无效");
        }
        
        using var state = await InitProcessingStateAsync(connectionId);
        
        try
        {
            _logger.LogInformation("SubscribeAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);

            using var _ = new ActivityScope(nameof(SubscribeAsync));

            // 提前启动事件反序列化以提高并行性能
            var eventDeserializeTask = Task.Run(() => DeserializeEvent(eventTypeName, eventJson));
            
            // 减少嵌套，优化异常处理流程，同时启动组成员初始化
            var groupMembersTask = InitializeGroupMembers(grainId);
            
            // 等待两个任务并行完成
            await Task.WhenAll(eventDeserializeTask, groupMembersTask);
            
            var (parentGAgent, signalRGAgent) = groupMembersTask.Result;
            var eventInstance = eventDeserializeTask.Result;
            
            if (parentGAgent is null || signalRGAgent is null)
            {
                _logger.LogWarning("SubscribeAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }
            
            if (eventInstance == null)
            {
                _logger.LogWarning("SubscribeAsync: Failed to deserialize event of type {EventType}", eventTypeName);
                throw new HubException($"无法反序列化类型为 {eventTypeName} 的事件");
            }

            // 并行执行连接注册和代理注册
            var addConnectionTask = AddConnectionIdIfNeeded(signalRGAgent, connectionId, false);
            var registerTask = parentGAgent.RegisterAsync(signalRGAgent);
            
            await Task.WhenAll(addConnectionTask, registerTask);
            
            _logger.LogDebug("SignalRGAgent {SignalRGAgentId} registered to parent {ParentGAgentId} for subscription",
                signalRGAgent.GetGrainId(), parentGAgent.GetGrainId());
            
            // 使用带取消令牌的任务，防止长时间挂起
            await signalRGAgent.PublishEventAsync(eventInstance, connectionId)
                .WaitAsync(TimeSpan.FromSeconds(30), state.Cts.Token);
                
            return signalRGAgent.GetGrainId();
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("SubscribeAsync: Operation canceled for connection {ConnectionId}", connectionId);
            throw new HubException("操作已取消");
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("SubscribeAsync: Operation timed out for connection {ConnectionId}", connectionId);
            throw new HubException("操作超时");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SubscribeAsync: Error processing subscription for connection {ConnectionId}, GrainId {GrainId}",
                connectionId, grainId);
            throw new HubException("处理订阅失败: " + ex.Message);
        }
        finally
        {
            _currentProcessingState.Value = null;
        }
    }

    private EventBase? DeserializeEvent(string eventTypeName, string eventJson)
    {
        try
        {
            return _eventDeserializer.DeserializeEvent(eventJson, eventTypeName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize event of type {EventType}", eventTypeName);
            return null;
        }
    }

    // 优化的GAgent初始化方法，减少嵌套，增加并行获取能力
    private async Task<(IGAgent? ParentGAgent, ISignalRGAgent? SignalRGAgent)> InitializeGroupMembers(
        GrainId grainId)
    {
        try
        {
            var targetGAgent = await _gAgentFactory.GetGAgentAsync(grainId);
            var parentGrainId = await targetGAgent.GetParentAsync();
            
            if (parentGrainId.IsDefault)
            {
                _logger.LogWarning("Parent GrainId is default for target GrainId {GrainId}", grainId);
                return (null, null);
            }
            
            // 获取父GAgent
            var parentGAgentTask = _gAgentFactory.GetGAgentAsync(parentGrainId);
            
            // 提前获取父代理完成后立即开始获取SignalRGAgent
            var parentGAgent = await parentGAgentTask.ConfigureAwait(false);
            var signalRGAgentTask = GetOrCreateSignalRGAgentAsync(parentGAgent);
            
            // 允许ConfigureAwait(false)，提高线程复用效率
            var signalRGAgent = await signalRGAgentTask.ConfigureAwait(false);
            
            return (parentGAgent, signalRGAgent);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initializing group members for GrainId {GrainId}", grainId);
            return (null, null);
        }
    }

    private async Task<ISignalRGAgent> GetOrCreateSignalRGAgentAsync(IGAgent parentGAgent)
    {
        try
        {
            var siblings = await parentGAgent.GetChildrenAsync().ConfigureAwait(false);
            var signalRGrainType = GrainTypeCache.Get(typeof(SignalRGAgent));
            
            var existingGAgentId = siblings.FirstOrDefault(id => id.Type == signalRGrainType);

            if (existingGAgentId.IsDefault is false)
            {
                _logger.LogDebug("Using existing SignalRGAgent with ID {GAgentId}", existingGAgentId);
                return await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(existingGAgentId.GetGuidKey())
                    .ConfigureAwait(false);
            }
            
            _logger.LogDebug("Creating new SignalRGAgent for parent {ParentGAgentId}", parentGAgent.GetGrainId());
            return await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting or creating SignalRGAgent for parent {ParentGAgentId}", 
                parentGAgent.GetGrainId());
            throw;
        }
    }

    private string GetConnectionId() 
    {
        var connectionId = Context?.ConnectionId ?? string.Empty;
        if (!string.IsNullOrEmpty(connectionId))
        {
            // 记录活跃连接
            _activeConnections.TryAdd(connectionId, 1);
        }
        return connectionId;
    }

    private static async Task AddConnectionIdIfNeeded(ISignalRGAgent agent, string connectionId, bool fireAndForget)
    {
        if (!string.IsNullOrEmpty(connectionId))
        {
            await agent.AddConnectionIdAsync(connectionId, fireAndForget).ConfigureAwait(false);
        }
    }

    private static async Task RemoveConnectionIdIfNeeded(ISignalRGAgent agent, string connectionId)
    {
        if (!string.IsNullOrEmpty(connectionId))
        {
            await agent.RemoveConnectionIdAsync(connectionId).ConfigureAwait(false);
        }
    }

    public async Task UnsubscribeAsync(GrainId signalRGAgentGrainId)
    {
        var connectionId = GetConnectionId();
        if (string.IsNullOrEmpty(connectionId))
        {
            _logger.LogWarning("UnsubscribeAsync: Connection ID is null or empty");
            return;
        }
        
        try
        {
            _logger.LogInformation("UnsubscribeAsync: Connection {ConnectionId}, SignalRGAgentGrainId {GrainId}",
                connectionId, signalRGAgentGrainId);
                
            var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(signalRGAgentGrainId.GetGuidKey())
                .ConfigureAwait(false);
                
            // 设置超时，防止长时间阻塞
            await signalRGAgent.RemoveConnectionIdAsync(connectionId)
                .WaitAsync(TimeSpan.FromSeconds(5))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("UnsubscribeAsync: Operation timed out for connection {ConnectionId}", connectionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error unsubscribing connection {ConnectionId} from SignalRGAgent {GrainId}",
                connectionId, signalRGAgentGrainId);
        }
    }

    public override async Task OnConnectedAsync()
    {
        var connectionId = GetConnectionId();
        _logger.LogInformation("Client {ConnectionId} connected", connectionId);
        
        try
        {
            var baseTask = base.OnConnectedAsync();
            var groupTask = Groups.AddToGroupAsync(connectionId, Guid.Empty.ToString());
            
            // 并行执行连接初始化任务
            await Task.WhenAll(baseTask, groupTask);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during connection establishment for {ConnectionId}", connectionId);
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var connectionId = GetConnectionId();
        _logger.LogInformation("Client {ConnectionId} disconnected with reason: {Reason}", 
            connectionId, exception?.Message ?? "No reason provided");
            
        try
        {
            // 清理活跃连接列表
            _activeConnections.TryRemove(connectionId, out _);
            
            var baseTask = base.OnDisconnectedAsync(exception);
            var groupTask = Groups.RemoveFromGroupAsync(connectionId, Guid.Empty.ToString());
            
            // 并行执行连接终止任务
            await Task.WhenAll(baseTask, groupTask);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during connection termination for {ConnectionId}", connectionId);
        }
    }
}

internal static class GrainTypeCache
{
    private static readonly ConcurrentDictionary<Type, GrainType> _cache = new();

    public static GrainType Get(Type grainType) =>
        _cache.GetOrAdd(grainType, t => GrainType.Create(t.FullName!));
}

// 为Task添加超时扩展方法
internal static class TaskExtensions
{
    public static async Task WaitAsync(this Task task, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var timeoutTask = Task.Delay(timeout, cancellationToken);
        var completedTask = await Task.WhenAny(task, timeoutTask);
        
        if (completedTask == timeoutTask)
        {
            throw new TimeoutException("任务执行超时");
        }
        
        // 确保原始任务的异常会被正确传播
        await task;
    }
    
    public static async Task<T> WaitAsync<T>(this Task<T> task, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var timeoutTask = Task.Delay(timeout, cancellationToken);
        var completedTask = await Task.WhenAny(task, timeoutTask);
        
        if (completedTask == timeoutTask)
        {
            throw new TimeoutException("任务执行超时");
        }
        
        // 确保原始任务的异常会被正确传播
        return await task;
    }
}