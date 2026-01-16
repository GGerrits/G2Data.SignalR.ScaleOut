namespace G2Data.SignalR.ScaleOut.Core;

public interface ISignalRBackplane
{
    Task PublishAsync(SignalRMessage message, CancellationToken cancellationToken);

    Task SubscribeAsync(Func<SignalRMessage, Task> onMessageReceived, CancellationToken cancellationToken);
}
