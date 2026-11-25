namespace Game.Core;

// SOLO SI NO EXISTE YA en tu proyecto. Si ya tienes esta clase, añádele
// únicamente las tres últimas propiedades (ExchangeName, TurnRoutingKey, ScoresRoutingKey).
public sealed class RabbitMqConnectionOptions
{
    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";

    public string ExchangeName { get; init; } = "distributed-dice";
    public string TurnRoutingKey { get; init; } = "game.turn";
    public string ScoresRoutingKey { get; init; } = "game.scores";
}
