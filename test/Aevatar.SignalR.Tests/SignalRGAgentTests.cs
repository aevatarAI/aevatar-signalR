using Aevatar.Core.Abstractions;
using Aevatar.SignalR.GAgents;
using Aevatar.SignalR.Tests.GAgents;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Shouldly;
using NSubstitute;

namespace Aevatar.SignalR.Tests;

// ReSharper disable InconsistentNaming
public sealed class SignalRGAgentTests : AevatarSignalRTestBase
{
    private readonly IGAgentFactory _gAgentFactory;
    private readonly AevatarSignalRHub _signalRHub;

    public SignalRGAgentTests()
    {
        _gAgentFactory = GetRequiredService<IGAgentFactory>();
        var logger = GetRequiredService<ILogger<AevatarSignalRHub>>();
        _signalRHub = new AevatarSignalRHub(_gAgentFactory, logger);
    }

    [Fact]
    public async Task SignalRGAgentTest()
    {
        var signalRGAgent = await _gAgentFactory.GetGAgentAsync<ISignalRGAgent>(
            new SignalRGAgentConfiguration
            {
                ConnectionId = "test-connection-id"
            });
        (await signalRGAgent.GetDescriptionAsync()).ShouldNotBeNull();
    }

    [Fact]
    public async Task SignalRHubTest()
    {
        var groupGAgent = await _gAgentFactory.GetGAgentAsync<IStateGAgent<GroupGAgentState>>();
        var naiveGAgent = await _gAgentFactory.GetGAgentAsync<IStateGAgent<NaiveTestGAgentState>>();
        await groupGAgent.RegisterAsync(naiveGAgent);

        var naiveTestEvent = new NaiveTestEvent
        {
            Greeting = "Hello, SignalR!"
        };

        // 创建测试Hub上下文
        var connection = SignalRTestHelper.CreateHubConnectionContext();
        var mockContext = new TestHubCallerContext(connection);
            
        // 设置Hub的Context
        typeof(Hub).GetProperty("Context")!.SetValue(_signalRHub, mockContext);

        await _signalRHub.PublishEventAsync(naiveGAgent.GetGrainId(), typeof(NaiveTestEvent).FullName!,
            JsonConvert.SerializeObject(naiveTestEvent));

        var children = await groupGAgent.GetChildrenAsync();
        children.Count.ShouldBe(2);
        children.Last().Type.ShouldBe(GrainType.Create("Aevatar.SignalR.GAgents.SignalRGAgent"));
    }
}