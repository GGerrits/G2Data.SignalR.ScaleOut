using MongoDB.Driver;

namespace G2Data.SignalR.ScaleOut.MongoDB
{
    public sealed class MongoDBOptions
    {
        public Func<IServiceProvider, IMongoDatabase>? GetDbDelegate { get; set; }

        public string? CollectionName { get; set; }
    }
}
