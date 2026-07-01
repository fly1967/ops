namespace PaymentService.Events;

public sealed class OrderCreated
{
    public Guid MessageId { get; set; }
    public Guid CorrelationId { get; set; }
    public Guid OrderId { get; set; }

    public decimal TotalAmount { get; set; }

    public DateTime CreatedAtUtc { get; set; }
}s