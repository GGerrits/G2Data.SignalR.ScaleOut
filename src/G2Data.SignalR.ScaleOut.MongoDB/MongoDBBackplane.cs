using G2Data.SignalR.ScaleOut.Core;
using MongoDB.Driver;

namespace G2Data.SignalR.ScaleOut.MongoDB;

internal sealed class MongoDBBackplane : ISignalRBackplane
{
    private readonly IMongoCollection<SignalRMessage> _collection;
    private readonly IServiceProvider _serviceProvider;

    public MongoDBBackplane(IServiceProvider serviceProvider, Func<IServiceProvider, IMongoDatabase> getDbDelegate, string collectionName)
    {
        _serviceProvider = serviceProvider;
        var db = getDbDelegate(_serviceProvider);
        MongoDBSetup.Init(db, collectionName);
        _collection = db.GetCollection<SignalRMessage>(collectionName);
    }

    public async Task PublishAsync(SignalRMessage message, CancellationToken cancellationToken)
    {
        await _collection.InsertOneAsync(message, null, cancellationToken);
    }

    public async Task SubscribeAsync(Func<SignalRMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<SignalRMessage>>()
            .Match(x => x.OperationType == ChangeStreamOperationType.Insert);
        using var stream = await _collection.WatchAsync(pipeline, cancellationToken: cancellationToken);
        while (await stream.MoveNextAsync(cancellationToken))
        {
            foreach (var change in stream.Current)
            {
                await onMessageReceived(change.FullDocument);
            }
        }
    }
}
