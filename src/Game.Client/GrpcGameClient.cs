using Game.Contracts;
using Game.Core;
using Grpc.Core;
using Grpc.Net.Client;
using CoreGameStatus = Game.Core.GameStatus;
using ProtoGameStatus = Game.Contracts.GameStatus;

namespace Game.Client;

public sealed class GrpcGameClient : IPlayerGameClient, IAsyncDisposable
{
    private readonly GrpcChannel _channel;
    private readonly GameService.GameServiceClient _client;

    public GrpcGameClient(string serverAddress, HttpMessageHandler? httpHandler = null)
    {
        _channel = GrpcChannel.ForAddress(serverAddress, new GrpcChannelOptions
        {
            HttpHandler = httpHandler
        });
        _client = new GameService.GameServiceClient(_channel);
    }

    public Task<CreateGameResult> CreateGameAsync(
        string name,
        int playerCount,
        CancellationToken cancellationToken) =>
        CallAsync(async () =>
        {
            var reply = await _client.CreateGameAsync(
                new CreateGameRequest
                {
                    Name = name,
                    PlayerCount = playerCount
                },
                cancellationToken: cancellationToken);

            var state = ToSnapshot(reply.State);
            var playerId = ParseRequiredGuid(reply.PlayerId, "jugador");
            return new CreateGameResult(state.Id, playerId, state);
        }, cancellationToken);

    public Task<JoinGameResult> JoinGameAsync(string name, CancellationToken cancellationToken) =>
        CallAsync(async () =>
        {
            var reply = await _client.JoinGameAsync(
                new JoinGameRequest { Name = name },
                cancellationToken: cancellationToken);

            var playerId = ParseRequiredGuid(reply.PlayerId, "jugador");
            var state = ToSnapshot(reply.State);
            var warning = EmptyToNull(reply.EventDeliveryWarning);
            return new JoinGameResult(playerId, state, warning);
        }, cancellationToken);

    public Task<GameSnapshot> GetStateAsync(Guid gameId, CancellationToken cancellationToken) =>
        CallAsync(async () =>
        {
            var reply = await _client.GetStateAsync(
                new GetStateRequest { GameId = gameId.ToString("D") },
                cancellationToken: cancellationToken);
            return ToSnapshot(reply.State);
        }, cancellationToken);

    public Task<RollResult> RollAsync(
        Guid gameId,
        Guid playerId,
        CancellationToken cancellationToken) =>
        CallAsync(async () =>
        {
            var reply = await _client.RollAsync(
                new RollRequest
                {
                    GameId = gameId.ToString("D"),
                    PlayerId = playerId.ToString("D")
                },
                cancellationToken: cancellationToken);

            var rolledPlayerId = ParseRequiredGuid(reply.PlayerId, "jugador");
            var firstDie = reply.FirstDie;
            var secondDie = reply.SecondDie;
            var state = ToSnapshot(reply.State);
            var warning = EmptyToNull(reply.EventDeliveryWarning);
            return new RollResult(rolledPlayerId, firstDie, secondDie, state, warning);
        }, cancellationToken);

    private static async Task<T> CallAsync<T>(
        Func<Task<T>> call,
        CancellationToken cancellationToken)
    {
        try
        {
            return await call();
        }
        catch (RpcException exception) when (
            exception.StatusCode != StatusCode.Cancelled || !cancellationToken.IsCancellationRequested)
        {
            var message = string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? $"El servidor rechazó la operación ({exception.StatusCode})."
                : exception.Status.Detail;
            throw new GameClientException(exception.StatusCode, message, exception);
        }
    }

    private static GameSnapshot ToSnapshot(Game.Contracts.GameState? state)
    {
        if (state is null)
        {
            throw new InvalidDataException("El servidor devolvió una respuesta sin estado de partida.");
        }

        var players = state.Players
            .Select(player => new PlayerSnapshot(
                ParseRequiredGuid(player.PlayerId, "jugador"),
                player.Number,
                player.Score))
            .ToArray();

        return new GameSnapshot(
            ParseRequiredGuid(state.GameId, "partida"),
            state.Name,
            state.Capacity,
            state.Status switch
            {
                ProtoGameStatus.WaitingForPlayers => CoreGameStatus.WaitingForPlayers,
                ProtoGameStatus.InProgress => CoreGameStatus.InProgress,
                ProtoGameStatus.Finished => CoreGameStatus.Finished,
                _ => throw new InvalidDataException($"El servidor devolvió un estado de partida desconocido: {state.Status}.")
            },
            players,
            ParseOptionalGuid(state.CurrentPlayerId, "jugador del turno"),
            ParseOptionalGuid(state.WinnerId, "ganador"));
    }

    private static Guid ParseRequiredGuid(string value, string valueName) =>
        Guid.TryParse(value, out var result)
            ? result
            : throw new InvalidDataException($"El servidor devolvió un identificador de {valueName} inválido.");

    private static Guid? ParseOptionalGuid(string value, string valueName) =>
        string.IsNullOrEmpty(value) ? null : ParseRequiredGuid(value, valueName);

    private static string? EmptyToNull(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value;

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }
}
