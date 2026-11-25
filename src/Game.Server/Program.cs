using Game.Core;
using Game.Server;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);
// 1. Configuración de RabbitMQ (sección "RabbitMQ" de appsettings.json;
//    se puede sobrescribir con variables de entorno, p. ej. RabbitMQ__Host=rabbitmq)
var rabbitOptions = builder.Configuration
    .GetSection("RabbitMQ")
    .Get<RabbitMqConnectionOptions>()
    ?? throw new InvalidOperationException("Falta la sección RabbitMQ en appsettings.json.");

// 2. Publicador de eventos con esa configuración
var eventPublisher = new RabbitGameEventPublisher(rabbitOptions);

// 3. Conexión y canal. Si RabbitMQ no está disponible el servidor arranca igualmente:
//    el publicador reintenta conectar en cada publicación y el cliente recibe la advertencia.
try
{
    await eventPublisher.InitializeAsync();
}
catch (GameEventDeliveryException exception)
{
    Console.Error.WriteLine($"[Aviso] {exception.Message} Se reintentará al publicar eventos.");
}

// 4. Registro como singleton (como clase concreta y como IGameEventPublisher para GameCoordinator)
builder.Services.AddSingleton(eventPublisher);
builder.Services.AddSingleton<IGameEventPublisher>(eventPublisher);

var serverAddress = Environment.GetEnvironmentVariable("GAME_SERVER_URL") ?? "http://localhost:5000";

builder.WebHost.UseUrls(serverAddress);

builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureEndpointDefaults(endpoint => endpoint.Protocols = HttpProtocols.Http2));

// Servicios gRPC + lógica del juego (una única instancia: el estado vive en el coordinador)
builder.Services.AddGrpc();
builder.Services.AddSingleton<IDiceRoller, RandomDiceRoller>();
builder.Services.AddSingleton<GameCoordinator>();


await using var app = builder.Build();
app.MapGrpcService<GameGrpcService>();

app.MapGet("/", () => "Servidor de juego gRPC.");
await app.RunAsync();
