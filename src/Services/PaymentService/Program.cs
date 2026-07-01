using Microsoft.EntityFrameworkCore;
using PaymentService.Data;
using PaymentService.Messaging;
using Serilog;

Console.WriteLine("Starting PaymentService...");

var builder = WebApplication.CreateBuilder(args);

Console.WriteLine("Builder created.");

builder.Host.UseSerilog((context, services, configuration) =>
{
    configuration.ReadFrom.Configuration(context.Configuration);
});

Console.WriteLine("Serilog configured.");

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

Console.WriteLine("Swagger configured.");

builder.Services.AddDbContext<PaymentDbContext>(options =>
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("PaymentDb")));

Console.WriteLine("Database configured.");

builder.Services.AddHostedService<RabbitMqConsumer>();

Console.WriteLine("RabbitMqConsumer registered.");

var app = builder.Build();

Console.WriteLine("Application built.");

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

Console.WriteLine("Swagger middleware added.");

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

Console.WriteLine("About to call app.Run()");

app.Run();