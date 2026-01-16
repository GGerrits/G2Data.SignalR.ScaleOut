namespace G2Data.SignalR.ScaleOut.Core;

public sealed class SignalRMessage
{
    public required Guid Id { get; set; }

    public SignalRMessageScope Scope { get; set; } = default!;

    public string Method { get; set; } = default!;

    public string Payload { get; set; } = default!;

    public string[]? Targets { get; set; }

    public string[]? ExcludedConnectionIds { get; set; }

    public string SenderId { get; set; } = default!;

    public DateTime SentAt { get; set; } = DateTime.UtcNow;
}
