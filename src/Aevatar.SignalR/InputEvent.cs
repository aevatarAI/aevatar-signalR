using Aevatar.Core.Abstractions;

namespace Aevatar.SignalR;

[GenerateSerializer]
public class InputEvent : EventBase
{
    [Id(0)] public required string Message { get; set; }
}