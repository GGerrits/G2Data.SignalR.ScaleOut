using G2Data.SignalR.ScaleOut.Core;
using MongoDB.Bson;
using MongoDB.Driver;

namespace G2Data.SignalR.ScaleOut.MongoDB;

internal static class MongoDBSetup
{
    public static void Init(IMongoDatabase database, string collectionName)
    {
        if (!CollectionExists(database, collectionName))
        {
            database.CreateCollection(collectionName);
        }

        var collection = database.GetCollection<BsonDocument>(collectionName);

        var fieldName = nameof(SignalRMessage.SentAt);
        var indexName = $"IX_{collection.CollectionNamespace.CollectionName}_{fieldName}_TTL";

        if (!IndexExists(collection, indexName))
        {
            var ttlIndex = new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending(fieldName),
                new CreateIndexOptions
                {
                    Name = indexName,
                    ExpireAfter = TimeSpan.FromMilliseconds(300)
                }
            );
            collection.Indexes.CreateOne(ttlIndex);
        }
    }

    private static bool CollectionExists(IMongoDatabase database, string collectionName)
    {
        var filter = new BsonDocument("name", collectionName);
        var collections = database.ListCollections(new ListCollectionsOptions { Filter = filter });
        return collections.Any();
    }

    private static bool IndexExists(IMongoCollection<BsonDocument> collection, string indexName)
    {
        var existingIndexes = collection.Indexes.List();

        foreach (var index in existingIndexes.ToEnumerable())
        {
            var exisitingIndexName = index["name"].AsString;
            if (exisitingIndexName.Equals(indexName, StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }
        }
        return false;
    }
}
