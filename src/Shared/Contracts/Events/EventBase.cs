public abstract record EventBase
{
    public Guid MessageId { get; init; }
    public Guid CorrelationId { get; init; }
    public Guid OrderId { get; init; }
    public DateTime CreatedAtUtc { get; init; }
}