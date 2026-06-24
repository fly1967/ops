namespace OrderService.Models;

public class OrderEvent
{
    public Guid Id { get; set; }

    public Guid OrderId { get; set; }

    public string EventType { get; set; } = string.Empty;

    public Guid CorrelationId { get; set; }

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}