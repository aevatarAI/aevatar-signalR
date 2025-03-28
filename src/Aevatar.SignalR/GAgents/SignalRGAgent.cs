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
    [Id(3)] public List<ResponseToPublisherEventBase> PendingMessages { get; set; } = new();
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

    private async Task SendWithRetryAsync(ResponseToPublisherEventBase message)
    {
        const int maxRetries = 3;
        var messageDelivered = false;
        
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var connectionIdList = State.ConnectionIds;
                
                // First try to use the ConnectionId from the message
                if (!string.IsNullOrEmpty(message.ConnectionId) && connectionIdList.ContainsKey(message.ConnectionId))
                {
                    Logger.LogInformation("Trying to send message to specified connectionId: {ConnectionId}, Message {Message}", 
                        message.ConnectionId, message);
                    
                    try 
                    {
                        await _hubContext.Client(message.ConnectionId)
                            .Send(SignalROrleansConstants.ResponseMethodName, message);
                        messageDelivered = true;
                        
                        if (connectionIdList[message.ConnectionId]) // If it's fireAndForget
                        {
                            Logger.LogDebug("Cleaning up connectionId: {ConnectionId}", message.ConnectionId);
                            RaiseEvent(new RemoveConnectionIdStateLogEvent
                            {
                                ConnectionId = message.ConnectionId
                            });
                            await ConfirmEvents();
                        }
                        
                        return; // Message delivered, return immediately
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Failed to send message to specified connectionId: {ConnectionId}", message.ConnectionId);
                        // Continue to try other connections
                    }
                }
                
                // If specified ConnectionId doesn't exist or sending failed, try connections with fireAndForget=false
                foreach (var (connectionId, fireAndForget) in connectionIdList)
                {
                    // Skip already tried connectionId
                    if (connectionId == message.ConnectionId)
                        continue;
                        
                    // Only use connections with fireAndForget=false as backup
                    if (!fireAndForget)
                    {
                        Logger.LogInformation("Trying to send message using persistent connection, connectionId: {ConnectionId}, Message {Message}", 
                            connectionId, message);
                        
                        try
                        {
                            await _hubContext.Client(connectionId)
                                .Send(SignalROrleansConstants.ResponseMethodName, message);
                            messageDelivered = true;
                            return; // Message delivered, return immediately
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "Failed to send message using persistent connection: {ConnectionId}", connectionId);
                            // Continue to try other connections
                        }
                    }
                }
                
                // If all above failed, try sending to all remaining connections
                foreach (var (connectionId, fireAndForget) in connectionIdList)
                {
                    // Skip already tried connectionIds
                    if (connectionId == message.ConnectionId || !fireAndForget)
                        continue;
                    
                    Logger.LogInformation("Last attempt to send message to connectionId: {ConnectionId}, Message {Message}", 
                        connectionId, message);
                    
                    try
                    {
                        await _hubContext.Client(connectionId)
                            .Send(SignalROrleansConstants.ResponseMethodName, message);
                        messageDelivered = true;
                        
                        Logger.LogDebug("Cleaning up connectionId: {ConnectionId}", connectionId);
                        if (fireAndForget)
                        {
                            RaiseEvent(new RemoveConnectionIdStateLogEvent
                            {
                                ConnectionId = connectionId
                            });
                            await ConfirmEvents();
                        }

                        return; // Message delivered, return immediately
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Failed to send message: {ConnectionId}", connectionId);
                        // Continue to try other connections
                    }
                }

                if (messageDelivered)
                {
                    return;
                }
                
                // If we reach here, we couldn't deliver the message to any connection
                await Task.Delay(100 * (i + 1));
            }
            catch (Exception ex)
            {
                if (i >= maxRetries - 1)
                    Logger.LogError(ex, $"Message failed after {maxRetries} retries.");
                else
                    await Task.Delay(100 * (i + 1));
            }
        }
        
        if (!messageDelivered && message is AevatarSignalRResponse<ResponseToPublisherEventBase> failedResponse)
        {
            Logger.LogInformation("Storing message for later delivery");
            RaiseEvent(new StorePendingMessageStateLogEvent
            {
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
        if (!fireAndForget)
        {
            // After adding connection ID, try to deliver all pending messages
            await DeliverPendingMessagesAsync(connectionId);
        }
       
    }

    public async Task RemoveConnectionIdAsync(string connectionId)
    {
        RaiseEvent(new RemoveConnectionIdStateLogEvent
        {
            ConnectionId = connectionId
        });
        await ConfirmEvents();
    }

    public async Task DeliverPendingMessagesAsync(string connectionId)
    {
        if (string.IsNullOrEmpty(connectionId))
            return;

        if (!State.PendingMessages.Any())
        {
            Logger.LogInformation("No pending messages");
            return;
        }

        Logger.LogInformation("Delivering {Count} pending messages", State.PendingMessages.Count);
        
        foreach (var message in State.PendingMessages)
        {
            message.ConnectionId = connectionId;
            await EnqueueMessageAsync(new AevatarSignalRResponse<ResponseToPublisherEventBase>
            {
                IsSuccess = true,
                Response = message
            });
        }
        
        RaiseEvent(new ClearPendingMessagesStateLogEvent());
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
            case ClearPendingMessagesStateLogEvent:
                State.PendingMessages.Clear();
                break;
            case StorePendingMessageStateLogEvent storePendingMessageStateLogEvent:
                State.PendingMessages.Add(storePendingMessageStateLogEvent.Message);
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
    public class ClearPendingMessagesStateLogEvent : SignalRStateLogEvent
    {
        // No additional properties needed
    }

    [GenerateSerializer]
    public class StorePendingMessageStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public ResponseToPublisherEventBase Message { get; set; }
    }
}