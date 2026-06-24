namespace OrderService.Models;

public class Order
{
    public Guid Id { get; set; }

    public string CustomerName { get; set; } = string.Empty;

    public string CustomerEmail { get; set; } = string.Empty;

    public string PackageName { get; set; } = string.Empty;

    public string Status { get; set; } = "Pending";

    public decimal TotalAmount { get; set; }

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}