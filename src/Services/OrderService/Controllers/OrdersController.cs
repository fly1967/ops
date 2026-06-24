using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Middleware;
using OrderService.Models;
using OrderService.Requests;
using System.Text.Json;

namespace OrderService.Controllers;

[ApiController]
[Route("api/orders")]
public class OrdersController : ControllerBase
{
    private readonly OrdersDbContext _db;

    public OrdersController(OrdersDbContext db)
    {
        _db = db;
    }

    [HttpPost]
    public async Task<ActionResult<Order>> CreateOrder(CreateOrderRequest request)
    {


        var correlationId = GetCorrelationId();

        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerName = request.CustomerName,
            CustomerEmail = request.CustomerEmail,
            PackageName = request.PackageName,
            TotalAmount = request.TotalAmount,
            Status = "Pending",
            CreatedAtUtc = DateTime.UtcNow
        };

        _db.Orders.Add(order);
        await _db.SaveChangesAsync();

        var orderEvent = new OrderEvent
        {
            Id = Guid.NewGuid(),
            OrderId = order.Id,
            EventType = "OrderCreated",
            CorrelationId = correlationId,
            CreatedAtUtc = DateTime.UtcNow
        };

        _db.OrderEvents.Add(orderEvent);
        await _db.SaveChangesAsync();

        var outboxMessage = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "OrderCreated",
            Payload = JsonSerializer.Serialize(new
            {
                OrderId = order.Id,
                CorrelationId = correlationId,
                CustomerName = order.CustomerName,
                CustomerEmail = order.CustomerEmail,
                PackageName = order.PackageName,
                TotalAmount = order.TotalAmount
            }),
            Published = false,
            CreatedAtUtc = DateTime.UtcNow
        };

        _db.OutboxMessages.Add(outboxMessage);

        await _db.SaveChangesAsync();

        return CreatedAtAction(
            nameof(GetOrder),
            new { id = order.Id },
            order);
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Order>> GetOrder(Guid id)
    {
        var order = await _db.Orders
            .FirstOrDefaultAsync(o => o.Id == id);

        if (order == null)
        {
            return NotFound();
        }

        return Ok(order);
    }

    [HttpPost("{id:guid}/cancel")]
    public async Task<IActionResult> CancelOrder(Guid id)
    {
        var order = await _db.Orders
            .FirstOrDefaultAsync(o => o.Id == id);

        if (order == null)
        {
            return NotFound();
        }

        order.Status = "Cancelled";

        await _db.SaveChangesAsync();

        return NoContent();
    }

    private Guid GetCorrelationId()
    {
        if (HttpContext.Items.TryGetValue(
                CorrelationIdMiddleware.HeaderName,
                out var value)
            && value is Guid correlationId)
        {
            return correlationId;
        }

        return Guid.NewGuid();
    }
}