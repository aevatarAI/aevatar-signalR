using Aevatar.Core;
using Aevatar.Core.Abstractions;
using Aevatar.SignalR.GAgents;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR.Protocol;
using Orleans.Streams;

namespace Aevatar.SignalR.Clients;

/// <inheritdoc cref="ISignalRClientGAgent"/>
internal sealed class SignalRClientGAgent : GAgentBase<SignalRClientGAgentState, SignalRClientGAgentStateLogEvent,
    EventBase, SignalRClientGAgentConfiguration>, ISignalRClientGAgent
{
    private const int MaxFailAttempts = 3;
    
    private string _hubName = default!;
    private string _connectionId = default!;
    private Guid ServerId => State.ServerId;

    private IStreamProvider _streamProvider = default!;
    private StreamSubscriptionHandle<Guid>? _serverDisconnectedSubscription = default;

    private int _failAttempts;

    public override Task<string> GetDescriptionAsync()
    {
        return Task.FromResult($"SignalRClientGAgent for {_hubName} connection {_connectionId}");
    }

    protected override async Task OnGAgentActivateAsync(CancellationToken cancellationToken)
    {
        _streamProvider = this.GetOrleansSignalRStreamProvider();

        // Resume subscriptions if we have already been "connected".
        // We know we have already been connected if the "ServerId" parameter is set.
        if (ServerId != default)
        {
            // We will listen to this stream to know if the server is disconnected (silo goes down) so that we can enact client disconnected procedure.
            var serverDisconnectedStream = _streamProvider.GetServerDisconnectionStream(State.ServerId);
            var handlers = await serverDisconnectedStream.GetAllSubscriptionHandles();
            var serverDisconnectedSubscription = handlers.FirstOrDefault();
            if (serverDisconnectedSubscription != null)
            {
                await serverDisconnectedSubscription.ResumeAsync((serverId, _) => OnDisconnect("server-disconnected"));
            }
        }
    }

    protected override async Task PerformConfigAsync(SignalRClientGAgentConfiguration configuration)
    {
        _hubName = configuration.HubType ?? _hubName;
        _connectionId = configuration.ConnectionId ?? _connectionId;

        RaiseEvent(new SetSignalRInfoStateLogEvent
        {
            HubType = _hubName,
            ConnectionId = _connectionId
        });
        await ConfirmEvents();
    }

    public async Task OnConnect(Guid serverId)
    {
        RaiseEvent(new SetServerIdStateLogEvent
        {
            ServerId = serverId,
        });
        await ConfirmEvents();
        var serverDisconnectedStream = _streamProvider.GetServerDisconnectionStream(serverId);
        _serverDisconnectedSubscription =
            await serverDisconnectedStream.SubscribeAsync(_ => OnDisconnect("server-disconnected"));
    }

    public async Task OnDisconnect(string? reason = null)
    {
        Logger.LogDebug(
            "Disconnecting connection on {hubName} for connection {connectionId} from server {serverId} via reason '{reason}'.",
            _hubName, _connectionId, State.ServerId, reason);

        if (_serverDisconnectedSubscription is not null)
        {
            await _serverDisconnectedSubscription.UnsubscribeAsync();
            _serverDisconnectedSubscription = null;
        }

        await _streamProvider.GetClientDisconnectionStream(_connectionId).OnNextAsync(_connectionId);

        DeactivateOnIdle();
    }

    // NB: Interface method is marked [ReadOnly] so this method will be re-entrant/interleaved.
    public async Task Send([Immutable] InvocationMessage message)
    {
        if (ServerId != default)
        {
            Logger.LogDebug("Sending message on {hubName}.{message.Target} to connection {connectionId}",
                _hubName, message.Target, _connectionId);

            // Routes the message to the silo (server) where the client is actually connected.
            var stream = _streamProvider.GetServerStream(ServerId);
            await stream.OnNextAsync(new ClientMessage(_hubName, _connectionId, message));

            Interlocked.Exchange(ref _failAttempts, 0);
        }
        else
        {
            Logger.LogInformation(
                "Client not connected for connectionId '{connectionId}' and hub '{hubName}' ({targetMethod})",
                _connectionId, _hubName, message.Target);

            if (Interlocked.Increment(ref _failAttempts) >= MaxFailAttempts)
            {
                Logger.LogWarning(
                    "Force disconnect client for connectionId {connectionId} and hub {hubName} ({targetMethod}) after exceeding attempts limit",
                    _connectionId, _hubName, message.Target);

                await OnDisconnect("attempts-limit-reached");
            }
        }
    }

    public Task SendOneWay(InvocationMessage message) => Send(message);

    [GenerateSerializer]
    public class SetSignalRInfoStateLogEvent : SignalRClientGAgentStateLogEvent
    {
        [Id(0)] public required string HubType { get; set; }
        [Id(1)] public required string ConnectionId { get; set; }
    }
    
    [GenerateSerializer]
    public class SetServerIdStateLogEvent : SignalRClientGAgentStateLogEvent
    {
        [Id(0)] public required Guid ServerId { get; set; }
    }

    protected override void GAgentTransitionState(SignalRClientGAgentState state, StateLogEventBase<SignalRClientGAgentStateLogEvent> @event)
    {
        switch (@event)
        {
            case SetSignalRInfoStateLogEvent setSignalRInfoStateLogEvent:
                State.HubType = setSignalRInfoStateLogEvent.HubType;
                State.ConnectionId = setSignalRInfoStateLogEvent.ConnectionId;
                break;
            case SetServerIdStateLogEvent setServerIdStateLogEvent:
                State.ServerId = setServerIdStateLogEvent.ServerId;
                break;
        }
    }
}