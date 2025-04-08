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
    private const int MaxRetries = 3;
    private readonly Channel<ResponseToPublisherEventBase> _messageChannel;

    public SignalRGAgent(IGrainFactory grainFactory)
    {
        _hubContext = new HubContext<AevatarSignalRHub>(grainFactory);
        _messageChannel = Channel.CreateUnbounded<ResponseToPublisherEventBase>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        
        // 为了避免启动后台任务（Orleans不推荐），采用Timer触发方式，确保Grain单线程模型
        // _ = ProcessMessagesAsync();
    }

    public override Task<string> GetDescriptionAsync()
    {
        return Task.FromResult("SignalR Publisher.");
    }

    protected override Task OnGAgentActivateAsync(CancellationToken cancellationToken)
    {
        if (State.PendingMessages.Count > 0)
        {
            Logger.LogInformation("Loading {Count} pending messages from state", State.PendingMessages.Count);
            
            foreach (var message in State.PendingMessages.ToList())
            {
                _messageChannel.Writer.TryWrite(message);
            }
            
            RaiseEvent(new ClearPendingMessagesStateLogEvent());
        }
        
        // 使用旧版RegisterTimer方法，避免GrainTimerConfig引用问题
        _processQueueTimer = RegisterTimer(
            ProcessQueueTimerCallback,
            null,
            TimeSpan.Zero,
            _processQueueInterval);
            
        return Task.CompletedTask;
    }

    private async Task ProcessQueueTimerCallback(object? state)
    {
        // 移除锁，Orleans已经保证了Grain的单线程执行
        // if (!await _processingLock.WaitAsync(0))
        //    return;
            
        try
        {
            // 先处理Channel中的消息
            await ProcessChannelMessagesAsync();
            
            // 然后处理持久化状态中的消息
            if (State.PendingMessages.Count > 0)
            {
                var messagesToProcess = State.PendingMessages
                    .Take(MaxMessagesPerBatch)
                    .ToList();
                    
                Logger.LogDebug("Processing {Count} pending messages from state queue", messagesToProcess.Count);
                
                foreach (var message in messagesToProcess)
                {
                    _messageChannel.Writer.TryWrite(message);
                }
                
                if (messagesToProcess.Count > 0)
                {
                    RaiseEvent(new RemovePendingMessagesStateLogEvent
                    {
                        Count = messagesToProcess.Count
                    });
                    await ConfirmEvents();
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error in ProcessQueueTimerCallback");
        }
        // 移除finally块中的锁释放
    }

    private async Task ProcessChannelMessagesAsync()
    {
        // 确保每次处理的消息数量有限，避免长时间阻塞Grain
        int processedCount = 0;
        
        while (processedCount < MaxMessagesPerBatch && _messageChannel.Reader.TryRead(out var message))
        {
            try
            {
                await SendWithRetryAsync(message);
                processedCount++;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error processing message: {Message}", message);
                RaiseEvent(new EnqueueMessageStateLogEvent { Message = message });
                await ConfirmEvents();
            }
        }
    }

    private async Task SendWithRetryAsync(object message)
    {
        Exception? lastException = null;
        
        for (var i = 0; i < MaxRetries; i++)
        {
            try
            {
                var connectionIdList = new Dictionary<string, bool>(State.ConnectionIds);
                var sentCount = 0;
                var failedConnections = new List<string>();
                
                foreach (var (connectionId, fireAndForget) in connectionIdList)
                {
                    try 
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
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Failed to send message to connectionId: {ConnectionId}", connectionId);
                        failedConnections.Add(connectionId);
                    }
                }
                
                if (failedConnections.Count > 0)
                {
                    foreach (var connectionId in failedConnections)
                    {
                        RaiseEvent(new RemoveConnectionIdStateLogEvent
                        {
                            ConnectionId = connectionId
                        });
                    }
                }
                
                if (sentCount > 0 || failedConnections.Count > 0)
                {
                    await ConfirmEvents();
                }
                
                if (sentCount > 0)
                {
                    return;
                }
                
                if (connectionIdList.Count > 0 && sentCount == 0)
                {
                    throw new Exception("Failed to send message to any connection");
                }
                
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                if (i < MaxRetries - 1)
                {
                    await Task.Delay(200 * (int)Math.Pow(2, i));
                }
            }
        }
        
        if (lastException != null)
        {
            Logger.LogError(lastException, "Message delivery failed after {MaxRetries} retries", MaxRetries);
            throw lastException;
        }
    }

    private Task EnqueueMessageAsync(ResponseToPublisherEventBase message)
    {
        // 确保消息入队列，如果无法立即发送则将其保存到状态中
        if (!_messageChannel.Writer.TryWrite(message))
        {
            RaiseEvent(new EnqueueMessageStateLogEvent { Message = message });
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
            case RemovePendingMessagesStateLogEvent removePendingMessagesStateLogEvent:
                State.PendingMessages.RemoveRange(0, removePendingMessagesStateLogEvent.Count);
                break;
        }
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _processQueueTimer?.Dispose();
        // 确保关闭消息通道
        _messageChannel.Writer.Complete();
        
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    [GenerateSerializer]
    public class AddConnectionIdStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public required string ConnectionId { get; set; } = string.Empty;
        [Id(1)] public bool FireAndForget { get; set; } = true;
    }

    [GenerateSerializer]
    public class RemoveConnectionIdStateLogEvent : SignalRStateLogEvent
    {
        [Id(0)] public required string ConnectionId { get; set; } = string.Empty;
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
        [Id(0)] public required ResponseToPublisherEventBase Message { get; set; }
    }

    [GenerateSerializer]
    public class ClearPendingMessagesStateLogEvent : StateLogEventBase<SignalRStateLogEvent>
    {
    }

    [GenerateSerializer]
    public class RemovePendingMessagesStateLogEvent : StateLogEventBase<SignalRStateLogEvent>
    {
        [Id(0)] public int Count { get; set; }
    }
}