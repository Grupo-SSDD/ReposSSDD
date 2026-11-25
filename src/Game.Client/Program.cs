using Game.Client;
using Game.Core;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

var serverAddress = Environment.GetEnvironmentVariable("GAME_SERVER_ADDRESS") ?? "http://localhost:5000";

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddEnvironmentVariables()
    .Build();
// 1. Configuración de RabbitMQ (appsettings.json, sobrescribible con RabbitMQ__Host, etc.)
var rabbitOptions = configuration
    .GetSection("RabbitMQ")
    .Get<RabbitMqConnectionOptions>()
    ?? throw new InvalidOperationException("Falta la sección RabbitMQ en appsettings.json.");

// 2. Suscriptor de eventos con esa configuración
await using var eventSubscriber = new RabbitGameEventSubscriber(rabbitOptions);


// Aquí se crea el cliente gRPC y se pasa la dirección del servidor:
await using var grpcClient = new GrpcGameClient(serverAddress);


var ansiConsole = AnsiConsole.Console;

var consoleGame = new ConsoleGame(grpcClient, eventSubscriber, ansiConsole);
if (Console.IsInputRedirected ||
    Console.IsOutputRedirected ||
    !ansiConsole.Profile.Capabilities.Interactive)
{
    await consoleGame.RunAsync(Console.In, Console.Out);
}
else
{
    await consoleGame.RunInteractiveAsync();
}
