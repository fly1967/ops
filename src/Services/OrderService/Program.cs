using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Middleware;
using Serilog;
using OrderService.BackgroundServices;
using OrderService.Messaging;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, config) =>
{
    config
        .ReadFrom.Configuration(context.Configuration)
        .Enrich.FromLogContext()
        .WriteTo.Console();
});

builder.Services.AddControllers();

builder.Services.AddHostedService<OutboxPublisher>();

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("OrdersDb")));

builder.Services.AddHealthChecks()
    .AddSqlServer(builder.Configuration.GetConnectionString("OrdersDb")!);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<RabbitMqPublisher>();
var app = builder.Build();

app.UseSerilogRequestLogging();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// app.UseHttpsRedirection();

app.UseMiddleware<CorrelationIdMiddleware>();

app.UseAuthorization();

app.MapControllers();

app.MapHealthChecks("/health");

Console.WriteLine(
    builder.Configuration.GetConnectionString("OrdersDb"));

app.Run();