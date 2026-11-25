using System.IO;
using System.Net.Sockets;
using System.Threading.Channels;
using Game.Core;
using Google.Protobuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Game.Client;

public sealed class RabbitGameEventSubscriber(RabbitMqConnectionOptions options)
    : IPlayerEventSubscriber, IAsyncDisposable
{
    public RabbitMqConnectionOptions ConnectionOptions { get; } = options;

    // Valores leídos de appsettings.json (sección RabbitMQ)
    public string ExchangeName => ConnectionOptions.ExchangeName;
    public string TurnRoutingKey => ConnectionOptions.TurnRoutingKey;
    public string ScoresRoutingKey => ConnectionOptions.ScoresRoutingKey;

    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;
    private Channel<GameEvent>? _events;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true } && _channel is { IsOpen: true } && _events is not null)
            {
                return;
            }

            await DisposeResourcesAsync();
            var events = Channel.CreateUnbounded<GameEvent>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = ConnectionOptions.Host,
                    Port = ConnectionOptions.Port,
                    UserName = ConnectionOptions.UserName,
                    Password = ConnectionOptions.Password
                };
                var connection = await factory.CreateConnectionAsync(cancellationToken);
                IChannel? channel = null;
                try
                {
                    channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
                    await channel.ExchangeDeclareAsync(
                        exchange: ExchangeName,
                        type: ExchangeType.Topic,
                        durable: true,
                        autoDelete: false,
                        cancellationToken: cancellationToken);
                    
                    // Cola temporal: nombre generado por el bróker, exclusiva de este cliente y
                    // borrada al desconectar. Recibe tanto turnos como puntuaciones.
                    var queue = await channel.QueueDeclareAsync(
                        queue: string.Empty,
                        durable: false,
                        exclusive: true,
                        autoDelete: true,
                        cancellationToken: cancellationToken);
                    await channel.QueueBindAsync(
                        queue: queue.QueueName,
                        exchange: ExchangeName,
                        routingKey: TurnRoutingKey,
                        cancellationToken: cancellationToken);
                    await channel.QueueBindAsync(
                        queue: queue.QueueName,
                        exchange: ExchangeName,
                        routingKey: ScoresRoutingKey,
                        cancellationToken: cancellationToken);
                
                    Task HandleEventReceivedAsync(object sender, BasicDeliverEventArgs delivery)
                    {
                        try
                        {
                            events.Writer.TryWrite(
                                RabbitGameEventDeserializer.Deserialize(delivery.RoutingKey, delivery.Body, ConnectionOptions));
                        }
                        catch (Exception exception) when (
                            exception is InvalidDataException or InvalidProtocolBufferException or FormatException)
                        {
                            events.Writer.TryComplete(
                                new GameEventSubscriptionException("Se recibió un evento RabbitMQ inválido.", exception));
                        }

                        return Task.CompletedTask;
                    }

                    Task HandleConsumerShutdownAsync(object sender, ShutdownEventArgs shutdown)
                    {
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            events.Writer.TryComplete(
                                new GameEventSubscriptionException(
                                    "El canal de eventos RabbitMQ se cerró.",
                                    new IOException("Canal RabbitMQ cerrado.")));
                        }

                        return Task.CompletedTask;
                    }

                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += HandleEventReceivedAsync;
                    consumer.ShutdownAsync += HandleConsumerShutdownAsync;

                    await channel.BasicConsumeAsync(
                        queue: queue.QueueName,
                        autoAck: true,
                        consumer: consumer,
                        cancellationToken: cancellationToken);

                    _connection = connection;
                    _channel = channel;
                    _events = events;
                }
                catch
                {
                    if (channel is not null)
                    {
                        await channel.DisposeAsync();
                    }

                    await connection.DisposeAsync();

                    throw;
                }
            }
            catch (Exception exception) when (IsBrokerFailure(exception))
            {
                events.Writer.TryComplete();
                throw new GameEventSubscriptionException(
                    "No se pudo iniciar la suscripción a RabbitMQ.",
                    exception);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public IAsyncEnumerable<GameEvent> ReadEventsAsync(CancellationToken cancellationToken)
    {
        var events = _events ??
            throw new InvalidOperationException("La suscripción RabbitMQ debe iniciarse antes de leer eventos.");
        return events.Reader.ReadAllAsync(cancellationToken);
    }

    private static bool IsBrokerFailure(Exception exception) =>
        exception is RabbitMQClientException or IOException or SocketException or TimeoutException;

    private async ValueTask DisposeResourcesAsync()
    {
        _events?.Writer.TryComplete();
        _events = null;
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycleGate.WaitAsync();
        try
        {
            await DisposeResourcesAsync();
        }
        finally
        {
            _lifecycleGate.Release();
            _lifecycleGate.Dispose();
        }
    }
}
