using System.Collections.Concurrent;
using Aevatar.Core.Abstractions;
using Aevatar.Core.Abstractions.Extensions;
using Aevatar.SignalR.GAgents;
using Aevatar.SignalR.Grains;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace Aevatar.SignalR;

// ReSharper disable InconsistentNaming
// [Authorize]
public class AevatarSignalRHub : Hub, IAevatarSignalRHub
{
    private readonly IGAgentFactory _gAgentFactory;
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<AevatarSignalRHub> _logger;

    public AevatarSignalRHub(IGAgentFactory gAgentFactory, IGrainFactory grainFactory,
        ILogger<AevatarSignalRHub> logger)
    {
        _gAgentFactory = gAgentFactory;
        _grainFactory = grainFactory;
        _logger = logger;
    }

    public async Task<GrainId> InitializeAsync(GrainId grainId)
    {
        _logger.LogInformation($"InitializeAsync: {grainId}");
        using var scope = new ActivityScope(nameof(InitializeAsync));

        var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(grainId.ToString().ToGuid());
        var signalRGAgentGrainId = signalRGAgent.GetGrainId();
        var signalRGAgentInitGrain = _grainFactory.GetGrain<ISignalRGAgentInitGrain>(signalRGAgentGrainId.GetGuidKey());

        try
        {
            await signalRGAgentInitGrain.InitializeSignalRGAgentAsync(grainId, signalRGAgent).ConfigureAwait(false);
            _logger.LogInformation($"SignalRGAgent {signalRGAgentGrainId} initialized successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to initialize SignalRGAgent {signalRGAgentGrainId}");
        }

        return signalRGAgentGrainId;
    }

    public async Task<GrainId?> PublishEventAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        _logger.LogInformation($"PublishEventAsync: {grainId} \n{eventTypeName} \n{eventJson}");
        using var _ = new ActivityScope(nameof(PublishEventAsync));

        var signalRGAgentGrainId = await InitializeAsync(grainId);
        var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(signalRGAgentGrainId.GetGuidKey());

        var connectionId = GetConnectionId();
        _logger.LogInformation($"ConnectionId: {connectionId}");
        await AddConnectionIdIfNeeded(signalRGAgent, connectionId, true);
        await signalRGAgent.PublishEventAsync(DeserializeEvent(eventTypeName, eventJson), connectionId);
        return signalRGAgent.GetGrainId();
    }

    public async Task<GrainId?> SubscribeAsync(GrainId grainId, string eventTypeName, string eventJson)
    {
        _logger.LogInformation($"SubscribeAsync: {grainId} \n{eventTypeName} \n{eventJson}");

        using var _ = new ActivityScope(nameof(SubscribeAsync));

        var signalRGAgentGrainId = await InitializeAsync(grainId);
        var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(signalRGAgentGrainId.GetGuidKey());

        var connectionId = GetConnectionId();
        _logger.LogInformation($"ConnectionId: {connectionId}");
        await AddConnectionIdIfNeeded(signalRGAgent, connectionId, false);
        await signalRGAgent.PublishEventAsync(DeserializeEvent(eventTypeName, eventJson), connectionId);
        return signalRGAgent.GetGrainId();
    }

    private static EventBase DeserializeEvent(string eventTypeName, string eventJson) =>
        new EventDeserializer().DeserializeEvent(eventJson, eventTypeName);

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
        _logger.LogInformation(
            "Client disconnecting - Connection Details:\n" +
            "ConnectionId: {ConnectionId}\n" +
            "User: {UserName}\n" +
            "Reason: {DisconnectReason}",
            Context.ConnectionId,
            Context.User?.Identity?.Name ?? "Anonymous",
            exception?.Message ?? "Normal disconnection");

        await base.OnDisconnectedAsync(exception);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, Guid.Empty.ToString());
    }
}

internal static class GrainTypeCache
{
    private static readonly ConcurrentDictionary<Type, GrainType> _cache = new();

    public static GrainType Get(Type grainType) =>
        _cache.GetOrAdd(grainType, t => GrainType.Create(t.FullName!));
}