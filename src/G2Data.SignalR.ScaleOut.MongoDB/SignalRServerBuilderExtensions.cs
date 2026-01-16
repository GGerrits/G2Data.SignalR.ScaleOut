using G2Data.SignalR.ScaleOut.Core;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace G2Data.SignalR.ScaleOut.MongoDB;

public static class SignalRServerBuilderExtensions
{
    private const string _defaultCollectionName = "SignalR.ScaleOut.Messages";

    public static ISignalRServerBuilder AddMongoDB(this ISignalRServerBuilder builder, Action<MongoDBOptions> configure)
    {
        var options = new MongoDBOptions();
        configure(options);
        builder.Services.AddSingleton<ISignalRBackplane>(sp => InitBackplane(sp, options.GetDbDelegate, options.CollectionName ?? _defaultCollectionName));
        builder.Services.AddSingleton(typeof(HubLifetimeManager<>), typeof(SignalRHubLifeTimeManager<>));
        return builder;
    }

    private static MongoDBBackplane InitBackplane(IServiceProvider serviceProvider
        , Func<IServiceProvider, IMongoDatabase>? getDbDelegate, string collectionName)
    {
        if (getDbDelegate == null)
        {
            throw new ArgumentNullException(nameof(getDbDelegate), "GetDbDelegate must be provided in MongoDBOptions");
        }
        return new MongoDBBackplane(serviceProvider, getDbDelegate, collectionName);
    }
}