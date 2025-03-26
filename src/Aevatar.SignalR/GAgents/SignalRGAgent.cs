using System.Threading.Channels;
using Aevatar.Core;
using Aevatar.Core.Abstractions;
using Aevatar.SignalR.Core;
using Microsoft.Extensions.Logging;

namespace Aevatar.SignalR.GAgents;

[GenerateSerializer]
public class SignalRGAgentState : StateBase
{
    [Id(1)] public Dictionary<string, bool> ConnectionIds { get; set; } = new();
    [Id(2)] public Dictionary<Guid, string> ConnectionIdMap { get; set; } = new();
    [Id(3)] public Dictionary<string, string> UserConnectionMap { get; set; } = new(); // Maps userId -> connectionId
    [Id(4)] public Dictionary<string, List<ResponseToPublisherEventBase>> PendingMessages { get; set; } = new(); // Maps userId -> pending messages
}

[GenerateSerializer]
public class SignalRStateLogEvent : StateLogEventBase<SignalRStateLogEvent>;

[GenerateSerializer]
public class SignalRGAgentConfiguration : ConfigurationBase
{
    [Id(0)] public string ConnectionId { get; set; } = string.Empty;
}

[GAgent]
public class SignalRGAgent :
    GAgentBase<SignalRGAgentState, SignalRStateLogEvent, EventBase, SignalRGAgentConfiguration>,
    ISignalRGAgent
{
    private readonly HubContext<AevatarSignalRHub> _hubContext;

    private Channel<ResponseToPublisherEventBase> _signalRMessageChannel;

    public SignalRGAgent(IGrainFactory grainFactory)
    {
        _hubContext = new HubContext<AevatarSignalRHub>(grainFactory);
    }

    public override Task<string> GetDescriptionAsync()
    {
        return Task.FromResult("SignalR Publisher.");
    }

    protected override async Task OnGAgentActivateAsync(CancellationToken cancellationToken)
    {
        _signalRMessageChannel = Channel.CreateUnbounded<ResponseToPublisherEventBase>();
        StartProcessingQueue();
    }

    private void StartProcessingQueue()
    {
        var reader = _signalRMessageChannel.Reader;
        Task.Run(async () =>
        {
            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out var msg))
                {
                    await SendWithRetryAsync(msg);
                }
            }
        });
    }

    private async Task SendWithRetryAsync(object message)
    {
        const int maxRetries = 3;
        var messageDelivered = false;
        
        // Try to find the user ID associated with the message for potential storage
        string? userId = null;
        if (message is AevatarSignalRResponse<ResponseToPublisherEventBase> response && 
            response.Response?.ConnectionId != null)
        {
            // Try to find the user ID associated with this connection ID
            foreach (var (user, conn) in State.UserConnectionMap)
            {
                if (conn == response.Response.ConnectionId)
                {
                    userId = user;
                    break;
                }
            }
        }
        
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var connectionIdList = State.ConnectionIds;
                foreach (var (connectionId, fireAndForget) in connectionIdList)
                {
                    Logger.LogInformation("Sending message to connectionId: {ConnectionId}, Message {Message}", connectionId,
                        message);
                    
                    try
                    {
                        await _hubContext.Client(connectionId)
                            .Send(SignalROrleansConstants.ResponseMethodName, message);
                        messageDelivered = true;
                        
                        if (fireAndForget)
                        {
                            Logger.LogDebug("Cleaning up connectionId: {ConnectionId}", connectionId);
                            RaiseEvent(new RemoveConnectionIdStateLogEvent
                            {
                                ConnectionId = connectionId
                            });
                            await ConfirmEvents();
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Failed to send message to connectionId: {ConnectionId}", connectionId);
                        // Connection might be broken, but we continue to try other connections
                    }
                }

                if (messageDelivered)
                {
                    return;
                }
                
                // If we get here, we couldn't deliver the message to any connection
                await Task.Delay(1000 * (i + 1));
            }
            catch (Exception ex)
            {
                if (i >= maxRetries - 1)
                    Logger.LogError(ex, $"Message failed after {maxRetries} retries.");
                else
                    await Task.Delay(1000 * (i + 1));
            }
        }
        
        // If we couldn't deliver the message and we have a user ID, store it for later delivery
        if (!messageDelivered && !string.IsNullOrEmpty(userId) && 
            message is AevatarSignalRResponse<ResponseToPublisherEventBase> failedResponse)
        {
            Logger.LogInformation("Storing message for later delivery to user {UserId}", userId);
            RaiseEvent(new StorePendingMessageStateLogEvent
            {
                UserId = userId,
                Message = failedResponse.Response
            });
            await ConfirmEvents();
        }
    }

    private async Task EnqueueMessageAsync(ResponseToPublisherEventBase message)
    {
        await _signalRMessageChannel.Writer.WriteAsync(message);
    }

    public async Task PublishEventAsync<T>(T @event, string connectionId) where T : EventBase
    {
        await PublishAsync(@event);
        
        Logger.LogDebug("Mapping correlationId to connectionId: {@CorrelationId} {ConnectionId}", @event.CorrelationId!.Value, connectionId);
        
        RaiseEvent(new MapCorrelationIdToConnectionIdStateLogEvent
        {
            CorrelationId = @event.CorrelationId!.Value,
            ConnectionId = connectionId
        });
        await ConfirmEvents();
    }

    public async Task AddConnectionIdAsync(string connectionId, bool fireAndForget)
    {
        RaiseEvent(new AddConnectionIdStateLogEvent
        {
            ConnectionId = connectionId,
            FireAndForget = fireAndForget
        });
        await ConfirmEvents();
    }

    public async Task RemoveConnectionIdAsync(string connectionId)
    {
        RaiseEvent(new RemoveConnectionIdStateLogEvent
        {
            ConnectionId = connectionId
        });
        await ConfirmEvents();
    }

    public async Task RegisterUserAsync(string userId, string connectionId)
    {
        if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(connectionId))
            return;

        Logger.LogInformation("Registering user {UserId} with connection {ConnectionId}", userId, connectionId);
        
        RaiseEvent(new RegisterUserStateLogEvent
        {
            UserId = userId,
            ConnectionId = connectionId
        });
        await ConfirmEvents();
    }

    public async Task ReconnectUserAsync(string userId, string newConnectionId)
    {
        if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(newConnectionId))
            return;

        Logger.LogInformation("Reconnecting user {UserId} with new connection {ConnectionId}", userId, newConnectionId);
        
        // Update the user's connection ID
        RaiseEvent(new RegisterUserStateLogEvent
        {
            UserId = userId,
            ConnectionId = newConnectionId
        });
        await ConfirmEvents();
        
        // Deliver any pending messages
        await DeliverPendingMessagesAsync(userId, newConnectionId);
    }

    public async Task DeliverPendingMessagesAsync(string userId, string connectionId)
    {
        if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(connectionId))
            return;

        if (!State.PendingMessages.TryGetValue(userId, out var pendingMessages) || !pendingMessages.Any())
        {
            Logger.LogInformation("No pending messages for user {UserId}", userId);
            return;
        }

        Logger.LogInformation("Delivering {Count} pending messages to user {UserId}", pendingMessages.Count, userId);
        
        foreach (var message in pendingMessages)
        {
            message.ConnectionId = connectionId;
            await EnqueueMessageAsync(new AevatarSignalRResponse<ResponseToPublisherEventBase>
            {
                IsSuccess = true,
                Response = message
            });
        }
        
        // Clear the pending messages for this user
        RaiseEvent(new ClearPendingMessagesStateLogEvent
        {
            UserId = userId
        });
        await ConfirmEvents();
    }

    [AllEventHandler]
    public async Task ResponseToSignalRAsync(EventWrapperBase eventWrapperBase)
    {
        Logger.LogInformation($"ResponseToSignalRAsync: {eventWrapperBase}");
        var eventWrapper = (EventWrapper<EventBase>)eventWrapperBase;
        if (!eventWrapper.Event.GetType().IsSubclassOf(typeof(ResponseToPublisherEventBase)))
        {
            Logger.LogDebug("Event is not a ResponseToPublisherEventBase");
            return;
        }

        var @event = (ResponseToPublisherEventBase)eventWrapper.Event;

        if (State.ConnectionIdMap.TryGetValue(@event.CorrelationId!.Value, out var connectionId))
        {
            @event.ConnectionId = connectionId;
        }
        else
        {
            Logger.LogInformation("Cannot find corresponding connectionId for correlationId: {@CorrelationId}", @event.CorrelationId);
        }

        await EnqueueMessageAsync(new AevatarSignalRResponse<ResponseToPublisherEventBase>
        {
            IsSuccess = true,
            Response = @event
        });
    }

    [EventHandler]
    public async Task HandleExceptionEventAsync(EventHandlerExceptionEvent @event)
    {
        Logger.LogInformation($"HandleExceptionEventAsync: {@event}");

        if (State.ConnectionIdMap.TryGetValue(@event.CorrelationId!.Value, out var connectionId))
        {
            var response = new AevatarSignalRResponse<ResponseToPublisherEventBase>
            {
                IsSuccess = false,
                ErrorType = ErrorType.EventHandler,
                ErrorMessage = $"GrainId: {@event.GrainId}, ExceptionMessage: {@event.ExceptionMessage}",
                ConnectionId = connectionId
            };
            await EnqueueMessageAsync(response);
        }
    }

    [EventHandler]
    public async Task GAgentBaseExceptionEventAsync(GAgentBaseExceptionEvent @event)
    {
        Logger.LogInformation($"GAgentBaseExceptionEventAsync: {@event}");

        if (State.ConnectionIdMap.TryGetValue(@event.CorrelationId!.Value, out var connectionId))
        {
            var response = new AevatarSignalRResponse<ResponseToPublisherEventBase>
            {
                IsSuccess = false,
                ErrorType = ErrorType.Framework,
                ErrorMessage = $"GrainId: {@event.GrainId}, ExceptionMessage: {@event.ExceptionMessage}",
                ConnectionId = connectionId
            };
            await EnqueueMessageAsync(response);
        }
    }

    protected override void GAgentTransitionState(SignalRGAgentState state,
        StateLogEventBase<SignalRStateLogEvent> @event)
    {
        switch (@event)
        {
            case AddConnectionIdStateLogEvent addConnectionIdStateLogEvent:
                State.ConnectionIds[addConnectionIdStateLogEvent.ConnectionId] =
                    addConnectionIdStateLogEvent.FireAndForget;
                break;
            case RemoveConnectionIdStateLogEvent removeConnectionIdStateLogEvent:
                State.ConnectionIds.Remove(removeConnectionIdStateLogEvent.ConnectionId);
                break;
            case MapCorrelationIdToConnectionIdStateLogEvent mapCorrelationIdToConnectionIdStateLogEvent:
                State.ConnectionIdMap[mapCorrelationIdToConnectionIdStateLogEvent.CorrelationId] =
                    mapCorrelationIdToConnectionIdStateLogEvent.ConnectionId;
                break;
            case RegisterUserStateLogEvent registerUserStateLogEvent:
                State.UserConnectionMap[registerUserStateLogEvent.UserId] = registerUserStateLogEvent.ConnectionId;
                break;
            case ClearPendingMessagesStateLogEvent clearPendingMessagesStateLogEvent:
                State.PendingMessages.Remove(clearPendingMessagesStateLogEvent.UserId);
                break;
            case StorePendingMessageStateLogEvent storePendingMessageStateLogEvent:
                if (!State.PendingMessages.TryGetValue(storePendingMessageStateLogEvent.UserId, out var messages))
                {
                    messages = new List<ResponseToPublisherEventBase>();
                    State.PendingMessages[storePendingMessageStateLogEvent.UserId] = messages;
                }
                messages.Add(storePendingMessageStateLogEvent.Message);
                break;
        }
    }

    [GenerateSerializer]
    public class AddConnectionIdStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public string ConnectionId { get; set; } = string.Empty;
        [Id(1)] public bool FireAndForget { get; set; } = true;
    }

    [GenerateSerializer]
    public class RemoveConnectionIdStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public string ConnectionId { get; set; } = string.Empty;
    }

    [GenerateSerializer]
    public class MapCorrelationIdToConnectionIdStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public Guid CorrelationId { get; set; }
        [Id(1)] public string ConnectionId { get; set; }
    }

    [GenerateSerializer]
    public class RegisterUserStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public string UserId { get; set; } = string.Empty;
        [Id(1)] public string ConnectionId { get; set; } = string.Empty;
    }

    [GenerateSerializer]
    public class ClearPendingMessagesStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public string UserId { get; set; } = string.Empty;
    }

    [GenerateSerializer]
    public class StorePendingMessageStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public string UserId { get; set; } = string.Empty;
        [Id(1)] public ResponseToPublisherEventBase Message { get; set; }
    }
}