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
    private static readonly ConcurrentDictionary<string, string> UserIdToConnectionId = new();
    private static readonly ConcurrentDictionary<string, string> ConnectionIdToUserId = new();
    // No need for UserIdToSignalRGAgent mapping as userId is already GrainId

    public AevatarSignalRHub(IGAgentFactory gAgentFactory, ILogger<AevatarSignalRHub> logger)
    {
        _gAgentFactory = gAgentFactory;
        _logger = logger;
    }

    // Add a method for user identification
    public async Task IdentifyUserAsync(string userId)
    {
        var connectionId = GetConnectionId();
        _logger.LogInformation("Identifying user {UserId} with connection {ConnectionId}", userId, connectionId);
        
        // Check if user had a previous connection
        if (UserIdToConnectionId.TryGetValue(userId, out var oldConnectionId) && 
            oldConnectionId != connectionId)
        {
            _logger.LogInformation("User {UserId} reconnected. Old connection: {OldConnectionId}, New connection: {NewConnectionId}", 
                userId, oldConnectionId, connectionId);
            
            // Migrate any subscriptions or state from old connection to new connection
            await MigrateUserConnectionAsync(userId, oldConnectionId, connectionId);
        }
        
        // Update the mappings
        UserIdToConnectionId[userId] = connectionId;
        ConnectionIdToUserId[connectionId] = userId;
    }

    public async Task<GrainId?> PublishEventAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        _logger.LogInformation($"PublishEventAsync: {grainId} \n{eventTypeName} \n{eventJson}");
        using var _ = new ActivityScope(nameof(PublishEventAsync));

        // Get connection ID and check if there's an associated user ID
        var connectionId = GetConnectionId();
        // Ensure user relationship is initialized

        var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
        if (parentGAgent == null || signalRGAgent == null) return null;

        _logger.LogInformation($"ConnectionId: {connectionId}");
        await AddConnectionIdIfNeeded(signalRGAgent, connectionId, true);
        
        var userId = grainId.ToString();
        
        await IdentifyUserAsync(userId);

        await parentGAgent.RegisterAsync(signalRGAgent);
        _logger.LogInformation($"{signalRGAgent.GetGrainId().ToString()} registered.");
        await signalRGAgent.PublishEventAsync(DeserializeEvent(eventTypeName, eventJson), connectionId);
        return signalRGAgent.GetGrainId();
    }

    public async Task<GrainId?> SubscribeAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        _logger.LogInformation($"SubscribeAsync: {grainId} \n{eventTypeName} \n{eventJson}");

        using var _ = new ActivityScope(nameof(SubscribeAsync));

        // Get connection ID and check if there's an associated user ID
        var connectionId = GetConnectionId();

        var (parentGAgent, signalRGAgent) = await InitializeGroupMembers(grainId);
        if (parentGAgent == null || signalRGAgent == null) return null;

        _logger.LogInformation($"ConnectionId: {connectionId}");
        await AddConnectionIdIfNeeded(signalRGAgent, connectionId, false);
        
        var userId = grainId.ToString();
        await IdentifyUserAsync(userId);
        
        await parentGAgent.RegisterAsync(signalRGAgent);
        _logger.LogInformation($"{signalRGAgent.GetGrainId().ToString()} registered.");
        await signalRGAgent.PublishEventAsync(DeserializeEvent(eventTypeName, eventJson), connectionId);
        return signalRGAgent.GetGrainId();
    }

    private static EventBase DeserializeEvent(string eventTypeName, string eventJson) =>
        new EventDeserializer().DeserializeEvent(eventJson, eventTypeName);

    private async Task<(IGAgent? ParentGAgent, ISignalRGAgent? SignalRGAgent)> InitializeGroupMembers(
        GrainId grainId)
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

    private async Task<ISignalRGAgent> GetOrCreateSignalRGAgentAsync(IGAgent parentGAgent)
    {
        var siblings = await parentGAgent.GetChildrenAsync();
        var existingGAgentId = siblings.FirstOrDefault(id =>
            id.Type == GrainTypeCache.Get(typeof(SignalRGAgent)));

        return existingGAgentId.IsDefault is false
            ? await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(existingGAgentId.GetGuidKey())
            : await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>();
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
        if (!connectionId.IsNullOrEmpty())
        {
            var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(signalRGAgentGrainId.GetGuidKey());
            await signalRGAgent.RemoveConnectionIdAsync(connectionId);
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

        await base.OnConnectedAsync();
        await Groups.AddToGroupAsync(Context.ConnectionId, Guid.Empty.ToString());
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var connectionId = GetConnectionId();
        
        // Get the user ID (if any) associated with this connection
        if (ConnectionIdToUserId.TryGetValue(connectionId, out var userId))
        {
            // We don't remove the userId->connectionId mapping as we want to remember that this user had this connection
            // Only clear the connectionId->userId mapping
            ConnectionIdToUserId.TryRemove(connectionId, out _);
            
            _logger.LogInformation("User {UserId} disconnected with connection {ConnectionId}", userId, connectionId);
        }
        
        await base.OnDisconnectedAsync(exception);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, Guid.Empty.ToString());
    }

    // Add a new method to migrate connection state
    private async Task MigrateUserConnectionAsync(string userId, string oldConnectionId, string newConnectionId)
    {
        // Find SignalRGAgents associated with this user
        // Update their connection information
        // Re-establish subscriptions
        
        // Example (implement according to your specific requirements):
        var grainId = GrainId.Parse(userId);
        var (_, signalRGAgent) = await InitializeGroupMembers(grainId);
        
        if (signalRGAgent != null)
        {
            await RemoveConnectionIdIfNeeded(signalRGAgent, oldConnectionId);
            await AddConnectionIdIfNeeded(signalRGAgent, newConnectionId, false);
        }
    }
}

internal static class GrainTypeCache
{
    private static readonly ConcurrentDictionary<Type, GrainType> _cache = new();

    public static GrainType Get(Type grainType) =>
        _cache.GetOrAdd(grainType, t => GrainType.Create(t.FullName!));
}