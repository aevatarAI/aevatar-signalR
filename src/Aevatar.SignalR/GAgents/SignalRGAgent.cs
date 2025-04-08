using System.Threading.Channels;
using Aevatar.Core;
using Aevatar.Core.Abstractions;
using Aevatar.SignalR.Core;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

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
    private IDisposable? _processQueueTimer;
    private readonly TimeSpan _processQueueInterval = TimeSpan.FromMilliseconds(200);
    private const int MaxMessagesPerBatch = 50;
    private bool _isProcessingQueue;
    private readonly Channel<ResponseToPublisherEventBase> _messageChannel;

    public SignalRGAgent(IGrainFactory grainFactory)
    {
        _hubContext = new HubContext<AevatarSignalRHub>(grainFactory);
        _messageChannel = Channel.CreateUnbounded<ResponseToPublisherEventBase>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        
        _ = ProcessMessagesAsync();
    }

    public override Task<string> GetDescriptionAsync()
    {
        return Task.FromResult("SignalR Publisher.");
    }

    protected override Task OnGAgentActivateAsync(CancellationToken cancellationToken)
    {
        foreach (var message in State.PendingMessages.ToList())
        {
            _messageChannel.Writer.TryWrite(message);
        }
        
        RaiseEvent(new ClearPendingMessagesStateLogEvent());
        
        _processQueueTimer = RegisterTimer(
            ProcessQueueTimerCallback,
            null,
            TimeSpan.Zero,
            _processQueueInterval);
            
        return Task.CompletedTask;
    }

    private async Task ProcessQueueTimerCallback(object state)
    {
        if (State.PendingMessages.Count > 0 && !_isProcessingQueue)
        {
            _isProcessingQueue = true;
            try
            {
                var messagesToProcess = State.PendingMessages.ToList();
                foreach (var message in messagesToProcess)
                {
                    _messageChannel.Writer.TryWrite(message);
                }
                
                RaiseEvent(new ClearPendingMessagesStateLogEvent());
                await ConfirmEvents();
            }
            finally
            {
                _isProcessingQueue = false;
            }
        }
    }

    private async Task ProcessMessagesAsync()
    {
        try
        {
            await foreach (var message in _messageChannel.Reader.ReadAllAsync())
            {
                try
                {
                    await SendWithRetryAsync(message);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error processing message: {Message}", message);
                    RaiseEvent(new EnqueueMessageStateLogEvent { Message = message });
                    await ConfirmEvents();
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Fatal error in message processing loop");
            _ = Task.Delay(1000).ContinueWith(_ => ProcessMessagesAsync());
        }
    }

    private async Task SendWithRetryAsync(object message)
    {
        const int maxRetries = 3;
        Exception? lastException = null;
        
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var connectionIdList = new Dictionary<string, bool>(State.ConnectionIds);
                var sentCount = 0;
                
                foreach (var (connectionId, fireAndForget) in connectionIdList)
                {
                    Logger.LogDebug("Sending message to connectionId: {ConnectionId}, Message type: {MessageType}", 
                        connectionId, message.GetType().Name);
                    
                    await _hubContext.Client(connectionId)
                        .Send(SignalROrleansConstants.ResponseMethodName, message);
                    sentCount++;
                    
                    if (fireAndForget)
                    {
                        Logger.LogDebug("Cleaning up connectionId: {ConnectionId}", connectionId);
                        RaiseEvent(new RemoveConnectionIdStateLogEvent
                        {
                            ConnectionId = connectionId
                        });
                    }
                }
                
                if (sentCount > 0)
                {
                    await ConfirmEvents();
                }
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                if (i < maxRetries - 1)
                {
                    await Task.Delay(200 * (int)Math.Pow(2, i));
                }
            }
        }
        
        if (lastException != null)
        {
            Logger.LogError(lastException, "Message delivery failed after {MaxRetries} retries", maxRetries);
            throw lastException;
        }
    }

    private Task EnqueueMessageAsync(ResponseToPublisherEventBase message)
    {
        if (!_messageChannel.Writer.TryWrite(message))
        {
            RaiseEvent(new EnqueueMessageStateLogEvent
            {
                Message = message
            });
            return ConfirmEvents();
        }
        
        return Task.CompletedTask;
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
            case MapCorrelationIdToConnectionIdStateLogEvent mapEvent:
                State.ConnectionIdMap[mapEvent.CorrelationId] = mapEvent.ConnectionId;
                break;
            case EnqueueMessageStateLogEvent enqueueEvent:
                State.PendingMessages.Add(enqueueEvent.Message);
                break;
            case ClearPendingMessagesStateLogEvent:
                State.PendingMessages.Clear();
                break;
        }
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _processQueueTimer?.Dispose();
        return Task.CompletedTask;
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
    public class EnqueueMessageStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public ResponseToPublisherEventBase Message { get; set; }
    }

    [GenerateSerializer]
    public class ClearPendingMessagesStateLogEvent : StateLogEventBase<SignalRStateLogEvent>
    {
    }
}