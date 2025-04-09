using Aevatar.SignalR.GAgents;

namespace Aevatar.SignalR.Grains;

public interface ISignalRGAgentInitGrain : IGrainWithGuidKey
{
    Task InitializeSignalRGAgentAsync(GrainId grainId, ISignalRGAgent signalRGAgent);
}