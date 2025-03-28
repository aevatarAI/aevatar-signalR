using Aevatar.Core.Abstractions.Extensions;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using SignalRSample.GAgents;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .AddJsonFile("appsettings.secrets.json", optional: true)
    .Build();

var signalRConfig = configuration.GetSection("SignalR");
var hubUrl = signalRConfig["HubUrl"];

var connection = new HubConnectionBuilder()
    .WithUrl(hubUrl!)
    .WithAutomaticReconnect() 
    .Build();

// Handle connection state changes
connection.Closed += error =>
{
    Console.WriteLine($"[Connection Status] Connection closed: {error?.Message ?? "No error message"}");
    return Task.CompletedTask;
};

connection.Reconnecting += error =>
{
    Console.WriteLine($"[Connection Status] Attempting to reconnect: {error?.Message ?? "No error message"}");
    return Task.CompletedTask;
};

connection.Reconnected += connectionId =>
{
    Console.WriteLine($"[Connection Status] Reconnected, new connection ID: {connectionId}");
    return Task.CompletedTask;
};

connection.On<string>("ReceiveResponse", (message) =>
{
    Console.WriteLine($"[Event] {message}");
});

await connection.StartAsync();
Console.WriteLine("[Connection Status] Successfully connected to server");

await PublishEventAsync("PublishEventAsync");
await PublishEventAsync("SubscribeAsync");

// Add task to simulate connection drop
_ = Task.Run(async () =>
{
    // Wait for a while before simulating disconnect
    await Task.Delay(5000);
    Console.WriteLine("[Simulation] About to disconnect...");
    
    try 
    {
        await connection.StopAsync();
        Console.WriteLine("[Simulation] Connection manually disconnected");
        
        // Wait for a while before reconnecting
        await Task.Delay(3000);
        Console.WriteLine("[Simulation] Attempting to reconnect...");
        await connection.StartAsync();
        Console.WriteLine("[Simulation] Successfully reconnected");
        
        // After connection is restored, send test message again
        await PublishEventAsync("PublishEventAsync");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Simulation] Error during simulation: {ex.Message}");
    }
});

// Main loop
while (true)
{
    await Task.Delay(1000);
}

async Task PublishEventAsync(string methodName)
{
    try
    {
        var eventJson = JsonConvert.SerializeObject(new
        {
            Greeting = "Test message"
        });

        await SendEventWithRetry(connection, methodName,
            "SignalRSample.GAgents.signalR",
            "test".ToGuid().ToString("N"),
            typeof(NaiveTestEvent).FullName!,
            eventJson);

        Console.WriteLine("✅ Success");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Abnormal: {ex.Message}");
    }
}

async Task SendEventWithRetry(HubConnection conn, string methodName, string grainType, string grainKey, string eventTypeName, string eventJson)
{
    var grainId = GrainId.Create(grainType, grainKey);
    const int maxRetries = 3;
    var retryCount = 0;

    while (retryCount < maxRetries)
    {
        try
        {
            if (conn.State != HubConnectionState.Connected)
            {
                Console.WriteLine("Connection broke, retrying...");
                await conn.StartAsync();
            }

            var signalRGAgentGrainId = await connection.InvokeAsync<GrainId>(methodName, grainId, eventTypeName, eventJson);
            Console.WriteLine($"SignalRGAgentGrainId: {signalRGAgentGrainId.ToString()}");
            return;
        }
        catch (Exception ex)
        {
            retryCount++;
            Console.WriteLine($"❌ Failed（Retry {retryCount}/{maxRetries}）: {ex.Message}");
            if (retryCount >= maxRetries)
            {
                throw;
            }
            await Task.Delay(1000 * retryCount);
        }
    }
}