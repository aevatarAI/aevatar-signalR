using System.Collections.Concurrent;
using Aevatar.Core.Abstractions;
using Aevatar.SignalR.GAgents;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace Aevatar.SignalR;

// ReSharper disable InconsistentNaming
// [Authorize]
public class AevatarSignalRHub : Hub, IAevatarSignalRHub
{
    private readonly IGAgentFactory _gAgentFactory;
    private readonly ILogger<AevatarSignalRHub> _logger;
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> _connectionSemaphores = new();

    public AevatarSignalRHub(IGAgentFactory gAgentFactory, ILogger<AevatarSignalRHub> logger)
    {
        _gAgentFactory = gAgentFactory;
        _logger = logger;
    }

    public async Task<GrainId?> PublishEventAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        var connectionId = GetConnectionId();
        if (string.IsNullOrEmpty(connectionId))
        {
            _logger.LogWarning("PublishEventAsync: Connection ID is null or empty");
            throw new HubException("Connection ID is invalid");
        }
        
        // 获取连接信号量或创建一个新的
        var semaphore = _connectionSemaphores.GetOrAdd(connectionId, _ => new SemaphoreSlim(1, 1));
        
        try
        {
            // 等待获取信号量，避免同一连接同时发送多个事件
            await semaphore.WaitAsync();
            
            _logger.LogInformation("PublishEventAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);
            
            using var _ = new ActivityScope(nameof(PublishEventAsync));

            var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
            if (parentGAgent == null || signalRGAgent == null)
            {
                _logger.LogWarning("PublishEventAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }

            try
            {
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
        }
        finally
        {
            semaphore.Release();
            
            // 考虑在一段时间后清理不再使用的信号量
            _ = Task.Delay(TimeSpan.FromMinutes(5)).ContinueWith(_ => 
            {
                if (_connectionSemaphores.TryRemove(connectionId, out var oldSemaphore))
                {
                    oldSemaphore.Dispose();
                }
            });
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
        
        // 获取连接信号量或创建一个新的
        var semaphore = _connectionSemaphores.GetOrAdd(connectionId, _ => new SemaphoreSlim(1, 1));
        
        try
        {
            // 等待获取信号量，避免同一连接同时订阅多个事件
            await semaphore.WaitAsync();
            
            _logger.LogInformation("SubscribeAsync: Connection {ConnectionId}, GrainId {GrainId}, EventType {EventType}",
                connectionId, grainId, eventTypeName);

            using var _ = new ActivityScope(nameof(SubscribeAsync));

            var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
            if (parentGAgent == null || signalRGAgent == null)
            {
                _logger.LogWarning("SubscribeAsync: Failed to initialize group members for GrainId {GrainId}", grainId);
                return null;
            }

            try
            {
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
        }
        finally
        {
            semaphore.Release();
        }
    }

    private static EventBase? DeserializeEvent(string eventTypeName, string eventJson)
    {
        try
        {
            return new EventDeserializer().DeserializeEvent(eventJson, eventTypeName);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private async Task<(IGAgent? ParentGAgent, ISignalRGAgent? SignalRGAgent)> InitializeGroupMembers(
        GrainId grainId)
    {
        try
        {
            var targetGAgent = await _gAgentFactory.GetGAgentAsync(grainId);
            var parentGrainId = await targetGAgent.GetParentAsync();
            
            if (parentGrainId.IsDefault)
            {
                var signalRParentGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>();
                var gAgent = await _gAgentFactory.GetGAgentAsync(grainId);
                await signalRParentGAgent.RegisterAsync(gAgent);
                return (signalRParentGAgent, signalRParentGAgent);
            }

            var parentGAgent = await _gAgentFactory.GetGAgentAsync(parentGrainId);
            if (parentGrainId.Type == GrainTypeCache.Get(typeof(SignalRGAgent)))
            {
                return (parentGAgent, await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(parentGrainId.GetGuidKey()));
            }

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
        _logger.LogInformation(
            "Client connecting - Connection Details:\n" +
            "ConnectionId: {ConnectionId}\n" +
            "User: {UserName}\n" +
            "IsAuthenticated: {IsAuthenticated}\n" +
            "Items Count: {ItemsCount}\n" +
            "Claims: {Claims}",
            Context.ConnectionId,
            Context.User?.Identity?.Name ?? "Anonymous",
            Context.User?.Identity?.IsAuthenticated ?? false,
            Context.Items.Count,
            Context.User?.Claims != null 
                ? string.Join(", ", Context.User.Claims.Select(c => $"{c.Type}: {c.Value}"))
                : "No claims");

        var connectionId = GetConnectionId();
        
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
        _logger.LogInformation(
            "Client disconnecting - Connection Details:\n" +
            "ConnectionId: {ConnectionId}\n" +
            "User: {UserName}\n" +
            "Reason: {DisconnectReason}",
            Context.ConnectionId,
            Context.User?.Identity?.Name ?? "Anonymous",
            exception?.Message ?? "Normal disconnection");

        try
        {
            await base.OnDisconnectedAsync(exception);
            await Groups.RemoveFromGroupAsync(connectionId, Guid.Empty.ToString());
            
            if (_connectionSemaphores.TryRemove(connectionId, out var semaphore))
            {
                semaphore.Dispose();
            }
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