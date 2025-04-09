using Aevatar.Core.Abstractions;
using Aevatar.SignalR.GAgents;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aevatar.SignalR.Grains;

public class SignalRGAgentInitGrain : Grain, ISignalRGAgentInitGrain
{
    private readonly ILogger<SignalRGAgentInitGrain> _logger;
    private readonly IGAgentFactory _gAgentFactory;

    public SignalRGAgentInitGrain(ILogger<SignalRGAgentInitGrain> logger)
    {
        _logger = logger;
        _gAgentFactory = ServiceProvider.GetRequiredService<IGAgentFactory>();
    }

    public async Task InitializeSignalRGAgentAsync(GrainId grainId, ISignalRGAgent signalRGAgent)
    {
        _logger.LogDebug("[{grainId}] InitializeSignalRGAgentAsync, SignalRGAgent GrainId: {signalRGAgentGrainId}",
            grainId.ToString(),
            signalRGAgent.GetGrainId().ToString());
        var targetGAgent = await _gAgentFactory.GetGAgentAsync(grainId);
        var parentGrainId = await targetGAgent.GetParentAsync();
        if (parentGrainId.IsDefault) // No parent.
        {
            _logger.LogDebug("[{grainId}] No parent found", grainId.ToString());
            // Then the SignalRGAgent will be the parent of the target GAgent.
            var gAgent = await _gAgentFactory.GetGAgentAsync(grainId);
            await signalRGAgent.RegisterAsync(gAgent);
            return;
        }

        // Otherwise, the target GAgent is the sibling of the SignalRGAgent.
        var parentGAgent = await _gAgentFactory.GetGAgentAsync(parentGrainId);
        if (parentGrainId.Type == GrainTypeCache.Get(typeof(SignalRGAgent)))
        {
            _logger.LogDebug("[{grainId}] Parent is already a SignalRGAgent: {parentGrainId}", grainId.ToString(),
                parentGrainId.ToString());
            // In case the parent is already set before.
            return;
        }

        var siblings = await parentGAgent.GetChildrenAsync();
        var existingGAgentId = siblings.FirstOrDefault(id =>
            id.Type == GrainTypeCache.Get(typeof(SignalRGAgent)));
        if (existingGAgentId.IsDefault)
        {
            _logger.LogDebug("[{grainId}] No existing SignalRGAgent found, creating a new one.", grainId.ToString());
            await parentGAgent.RegisterAsync(signalRGAgent);
        }
    }
}