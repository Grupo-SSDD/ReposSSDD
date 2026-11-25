using Game.Contracts;
using Game.Core;
using Grpc.Core;
using CoreGameStatus = Game.Core.GameStatus;
using ProtoGameStatus = Game.Contracts.GameStatus;

namespace Game.Server;

public sealed class GameGrpcService : GameService.GameServiceBase
{
    private readonly GameCoordinator _coordinator;

    public GameGrpcService(GameCoordinator coordinator)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
    }

    public override async Task<GameReply> CreateGame(
        CreateGameRequest request,
        ServerCallContext context)
    {
        try
        {
            var result = await _coordinator.CreateGameAsync(
                request.Name,
                request.PlayerCount,
                context.CancellationToken);

            return new GameReply
            {
                State = ToContract(result.State),
                PlayerId = result.PlayerId.ToString("D")
            };
        }
        catch (GameRuleException exception)
        {
            throw ToRpcException(exception);
        }
    }

    public override async Task<GameReply> JoinGame(
        JoinGameRequest request,
        ServerCallContext context)
    {
        try
        {
            var result = await _coordinator.JoinGameAsync(request.Name, context.CancellationToken);

            return new GameReply
            {
                State = ToContract(result.State),
                PlayerId = result.PlayerId.ToString("D"),

                EventDeliveryWarning = result.EventDeliveryWarning ?? string.Empty
            };
        }
        catch (GameRuleException exception)
        {
            throw ToRpcException(exception);
        }
    }

    public override Task<GameReply> GetState(GetStateRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.GameId, out var gameId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "El identificador de partida no es válido."));
        }

        try
        {
            return Task.FromResult(new GameReply
            {
                State = ToContract(_coordinator.GetState(gameId))
            });
        }
        catch (GameRuleException exception)
        {
            throw ToRpcException(exception);
        }
    }

    public override async Task<GameReply> Roll(RollRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.GameId, out var gameId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "El identificador de partida no es válido."));
        }

        if (!Guid.TryParse(request.PlayerId, out var playerId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "El identificador de jugador no es válido."));
        }

        try
        {
            var result = await _coordinator.RollAsync(gameId, playerId, context.CancellationToken);

            return new GameReply
            {
                State = ToContract(result.State),
                PlayerId = result.PlayerId.ToString("D"),
                FirstDie = result.FirstDie,
                SecondDie = result.SecondDie,
                EventDeliveryWarning = result.EventDeliveryWarning ?? string.Empty
            };
        }
        catch (GameRuleException exception)
        {
            throw ToRpcException(exception);
        }
    }

    private static Game.Contracts.GameState ToContract(GameSnapshot state)
    {
        var result = new Game.Contracts.GameState
        {
            GameId = state.Id.ToString("D"),
            Name = state.Name,
            Capacity = state.Capacity,
            Status = state.Status switch
            {
                CoreGameStatus.WaitingForPlayers => ProtoGameStatus.WaitingForPlayers,
                CoreGameStatus.InProgress => ProtoGameStatus.InProgress,
                CoreGameStatus.Finished => ProtoGameStatus.Finished,
                _ => throw new InvalidOperationException($"Estado de partida no soportado: {state.Status}.")
            },
            CurrentPlayerId = state.CurrentPlayerId?.ToString("D") ?? string.Empty,
            WinnerId = state.WinnerId?.ToString("D") ?? string.Empty
        };

        result.Players.AddRange(state.Players.Select(player => new Game.Contracts.Player
        {
            PlayerId = player.Id.ToString("D"),
            Number = player.Number,
            Score = player.Score
        }));

        return result;
    }

    private static RpcException ToRpcException(GameRuleException exception)
    {
        var status = exception.Error switch
        {
            GameError.InvalidName or GameError.InvalidPlayerCount => StatusCode.InvalidArgument,
            GameError.GameNotFound => StatusCode.NotFound,
            GameError.GameAlreadyActive => StatusCode.AlreadyExists,
            GameError.GameFull or
                GameError.GameNotStarted or
                GameError.GameFinished or
                GameError.NotPlayersTurn => StatusCode.FailedPrecondition,
            _ => throw new InvalidOperationException($"Error de juego no soportado: {exception.Error}.")
        };

        return new RpcException(new Status(status, exception.Message));
    }
}
