using System.Collections.Concurrent;
using System.Threading;
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
    
    // 使用对象池来减少临时对象的创建
    private static readonly ObjectPool<SemaphoreSlim> _semaphorePool = 
        new DefaultObjectPool<SemaphoreSlim>(new SemaphoreSlimPoolPolicy(), 50);
        
    // 使用较短的超时时间，避免长时间等待
    private static readonly TimeSpan SemaphoreTimeout = TimeSpan.FromSeconds(10);
    
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
        
        public ProcessingState(SemaphoreSlim semaphore, string connectionId)
        {
            Semaphore = semaphore;
            ConnectionId = connectionId;
        }
        
        public void Dispose()
        {
            _semaphorePool.Return(Semaphore);
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
            throw new HubException("Operation timed out due to high load. Please try again.");
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
            throw new HubException("Connection ID is invalid");
        }
        
        using var state = await InitProcessingStateAsync(connectionId);
        
        try
        {
            _logger.LogInformation("PublishEventAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);
            
            using var _ = new ActivityScope(nameof(PublishEventAsync));

            // 减少嵌套，优化异常处理流程
            var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
            if (parentGAgent == null || signalRGAgent == null)
            {
                _logger.LogWarning("PublishEventAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }

            await AddConnectionIdIfNeeded(signalRGAgent, connectionId, true);
            await parentGAgent.RegisterAsync(signalRGAgent);
            
            _logger.LogDebug("SignalRGAgent {SignalRGAgentId} registered to parent {ParentGAgentId}",
                signalRGAgent.GetGrainId(), parentGAgent.GetGrainId());
            
            var eventInstance = DeserializeEvent(eventTypeName, eventJson);
            if (eventInstance == null)
            {
                _logger.LogWarning("PublishEventAsync: Failed to deserialize event of type {EventType}", eventTypeName);
                throw new HubException($"Failed to deserialize event of type {eventTypeName}");
            }
            
            await signalRGAgent.PublishEventAsync(eventInstance, connectionId);
            return signalRGAgent.GetGrainId();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PublishEventAsync: Error processing event for connection {ConnectionId}, GrainId {GrainId}",
                connectionId, grainId);
            throw new HubException("Failed to process event: " + ex.Message);
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
            throw new HubException("Connection ID is invalid");
        }
        
        using var state = await InitProcessingStateAsync(connectionId);
        
        try
        {
            _logger.LogInformation("SubscribeAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);

            using var _ = new ActivityScope(nameof(SubscribeAsync));

            // 减少嵌套，优化异常处理流程
            var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
            if (parentGAgent == null || signalRGAgent == null)
            {
                _logger.LogWarning("SubscribeAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }

            await AddConnectionIdIfNeeded(signalRGAgent, connectionId, false);
            await parentGAgent.RegisterAsync(signalRGAgent);
            
            _logger.LogDebug("SignalRGAgent {SignalRGAgentId} registered to parent {ParentGAgentId} for subscription",
                signalRGAgent.GetGrainId(), parentGAgent.GetGrainId());
            
            var eventInstance = DeserializeEvent(eventTypeName, eventJson);
            if (eventInstance == null)
            {
                _logger.LogWarning("SubscribeAsync: Failed to deserialize event of type {EventType}", eventTypeName);
                throw new HubException($"Failed to deserialize event of type {eventTypeName}");
            }
            
            await signalRGAgent.PublishEventAsync(eventInstance, connectionId);
            return signalRGAgent.GetGrainId();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SubscribeAsync: Error processing subscription for connection {ConnectionId}, GrainId {GrainId}",
                connectionId, grainId);
            throw new HubException("Failed to process subscription: " + ex.Message);
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
            
            // 并行获取父GAgent和SignalRGAgent
            var parentGAgent = await parentGAgentTask;
            var signalRGAgent = await GetOrCreateSignalRGAgentAsync(parentGAgent);
            
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
            var siblings = await parentGAgent.GetChildrenAsync();
            var existingGAgentId = siblings.FirstOrDefault(id =>
                id.Type == GrainTypeCache.Get(typeof(SignalRGAgent)));

            if (existingGAgentId.IsDefault is false)
            {
                _logger.LogDebug("Using existing SignalRGAgent with ID {GAgentId}", existingGAgentId);
                return await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(existingGAgentId.GetGuidKey());
            }
            
            _logger.LogDebug("Creating new SignalRGAgent for parent {ParentGAgentId}", parentGAgent.GetGrainId());
            return await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting or creating SignalRGAgent for parent {ParentGAgentId}", 
                parentGAgent.GetGrainId());
            throw;
        }
    }

    private string GetConnectionId() => Context?.ConnectionId ?? string.Empty;

    private static async Task AddConnectionIdIfNeeded(ISignalRGAgent agent, string connectionId, bool fireAndForget)
    {
        if (!string.IsNullOrEmpty(connectionId))
        {
            await agent.AddConnectionIdAsync(connectionId, fireAndForget);
        }
    }

    private static async Task RemoveConnectionIdIfNeeded(ISignalRGAgent agent, string connectionId)
    {
        if (!string.IsNullOrEmpty(connectionId))
        {
            await agent.RemoveConnectionIdAsync(connectionId);
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
                
            var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(signalRGAgentGrainId.GetGuidKey());
            await signalRGAgent.RemoveConnectionIdAsync(connectionId);
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
            await base.OnConnectedAsync();
            await Groups.AddToGroupAsync(connectionId, Guid.Empty.ToString());
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
            await base.OnDisconnectedAsync(exception);
            await Groups.RemoveFromGroupAsync(connectionId, Guid.Empty.ToString());
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