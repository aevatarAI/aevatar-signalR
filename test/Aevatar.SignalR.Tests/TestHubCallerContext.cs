using System.Security.Claims;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Aevatar.SignalR.Tests;

public class TestHubCallerContext : HubCallerContext
{
    private readonly HubConnectionContext _connectionContext;

    public TestHubCallerContext(HubConnectionContext connectionContext)
    {
        _connectionContext = connectionContext;
    }

    public override string ConnectionId => _connectionContext.ConnectionId;
    
    public override string? UserIdentifier => _connectionContext.UserIdentifier;
    
    public override ClaimsPrincipal? User => _connectionContext.User;
    
    public override IDictionary<object, object?> Items => _connectionContext.Items;
    
    public override IFeatureCollection Features => _connectionContext.Features;
    
    public override CancellationToken ConnectionAborted => _connectionContext.ConnectionAborted;
    
    public override void Abort() => _connectionContext.Abort();
} 