namespace Game.Core;

// SOLO SI NO EXISTE YA. GameCoordinator y GameGrpcService ya la usan, así que lo más
// probable es que esté en otro fichero de Game.Core (busca "class GameRuleException").
// Si la añades y ya existía, el compilador dará "already contains a definition": bórrala.
public enum GameError
{
    InvalidName,
    InvalidPlayerCount,
    GameNotFound,
    GameAlreadyActive,
    GameFull,
    GameNotStarted,
    GameFinished,
    NotPlayersTurn
}

public sealed class GameRuleException(GameError error, string message) : Exception(message)
{
    public GameError Error { get; } = error;
}
