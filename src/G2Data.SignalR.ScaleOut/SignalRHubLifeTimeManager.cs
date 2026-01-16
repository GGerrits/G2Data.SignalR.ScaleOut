using G2Data.SignalR.ScaleOut.Core;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;
using System.Text.Json;

namespace G2Data.SignalR.ScaleOut;

public sealed class SignalRHubLifeTimeManager<THub> : HubLifetimeManager<THub> where THub : Hub
{
    public SignalRHubLifeTimeManager(ISignalRBackplane backplane, IHostApplicationLifetime lifetime)
    {
        _backplane = backplane;
        _ = Task.Run(ListenAsync);
        lifetime.ApplicationStopping.Register(() => _cts.Cancel());
    }

    private readonly ISignalRBackplane _backplane;

    private readonly string _serverId = Guid.NewGuid().ToString();

    private readonly CancellationTokenSource _cts = new();

    private readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _groups = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _users = new();

    private readonly JsonSerializerOptions _serializerOptions = new(JsonSerializerDefaults.Web);

    public override Task OnConnectedAsync(HubConnectionContext connection)
    {
        _connections[connection.ConnectionId] = connection;
        if (connection.UserIdentifier is not null)
        {
            var connections = _users.GetOrAdd(connection.UserIdentifier, _ => new ConcurrentDictionary<string, byte>());
            connections[connection.ConnectionId] = 0;
        }
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        _connections.TryRemove(connection.ConnectionId, out _);
        foreach (var group in _groups.Values)
        {
            group.TryRemove(connection.ConnectionId, out _);
        }

        if (connection.UserIdentifier is not null &&
            _users.TryGetValue(connection.UserIdentifier, out var connections))
        {
            connections.TryRemove(connection.ConnectionId, out _);
            if (connections.IsEmpty)
            {
                _users.TryRemove(connection.UserIdentifier, out _);
            }
        }
        return Task.CompletedTask;
    }

    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken)
    {
        var group = _groups.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, byte>());
        group[connectionId] = 0;
        return Task.CompletedTask;
    }

    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken)
    {
        if (_groups.TryGetValue(groupName, out var group))
        {
            group.TryRemove(connectionId, out _);
            if (group.IsEmpty)
            {
                _groups.TryRemove(groupName, out _);
            }
        }
        return Task.CompletedTask;
    }

    private async Task SendToConnections(IEnumerable<string> connectionIds, string method, string payload, IReadOnlySet<string>? exclude = null)
    {
        var tasks = new List<Task>();
        var args = JsonSerializer.Deserialize<object[]>(payload, _serializerOptions)!;
        foreach (var id in connectionIds)
        {
            if (exclude?.Contains(id) == true)
            {
                continue;
            }
            if (!_connections.TryGetValue(id, out var conn))
            {
                continue;
            }
            var invocationMessage = new InvocationMessage(method, args);
            var serialized = new SerializedHubMessage(invocationMessage);
            var task = conn.WriteAsync(serialized).AsTask();
            tasks.Add(task);
        }
        await Task.WhenAll(tasks);
    }

    public override Task SendAllAsync(string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.All, methodName, args, null, null, cancellationToken);

    public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.AllExcept, methodName, args, null, excludedConnectionIds, cancellationToken);

    public override Task SendConnectionAsync(string connectionId, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Connections, methodName, args, [connectionId], null, cancellationToken);

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Connections, methodName, args, connectionIds, null, cancellationToken);

    public override Task SendGroupAsync(string groupName, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Groups, methodName, args, [groupName], null, cancellationToken);

    public override Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.GroupExcept, methodName, args, [groupName], excludedConnectionIds, cancellationToken);

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Groups, methodName, args, groupNames, null, cancellationToken);

    public override Task SendUserAsync(string userId, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Users, methodName, args, [userId], null, cancellationToken);

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object[] args, CancellationToken cancellationToken)
        => Publish(SignalRMessageScope.Users, methodName, args, userIds, null, cancellationToken);

    private Task Publish(SignalRMessageScope scope, string method, object[] args, IEnumerable<string>? targets, IReadOnlyList<string>? excluded, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(args, _serializerOptions);
        return _backplane.PublishAsync(new SignalRMessage
        {
            Id = Guid.NewGuid(),
            Scope = scope,
            Method = method,
            Payload = payload,
            Targets = targets?.ToArray(),
            ExcludedConnectionIds = excluded?.ToArray(),
            SenderId = _serverId
        }, ct);
    }

    private async Task ListenAsync()
    {
        await _backplane.SubscribeAsync(async msg =>
        {
            switch (msg.Scope)
            {
                case SignalRMessageScope.All:
                    await SendToConnections(_connections.Keys, msg.Method, msg.Payload);
                    break;
                case SignalRMessageScope.AllExcept:
                    await SendToConnections(_connections.Keys, msg.Method, msg.Payload, msg.ExcludedConnectionIds!.ToHashSet());
                    break;
                case SignalRMessageScope.Connections:
                    await SendToConnections(msg.Targets!, msg.Method, msg.Payload);
                    break;
                case SignalRMessageScope.Groups:
                    {
                        foreach (var group in msg.Targets!)
                        {
                            if (_groups.TryGetValue(group, out var connections))
                            {
                                await SendToConnections(connections.Keys, msg.Method, msg.Payload);
                            }
                        }
                    }
                    break;
                case SignalRMessageScope.GroupExcept:
                    {
                        var g = msg.Targets![0];
                        if (_groups.TryGetValue(g, out var connections))
                        {
                            await SendToConnections(connections.Keys, msg.Method, msg.Payload, msg.ExcludedConnectionIds!.ToHashSet());
                        }
                    }
                    break;
                case SignalRMessageScope.Users:
                    foreach (var user in msg.Targets!)
                    {
                        if (_users.TryGetValue(user, out var conns))
                        {
                            await SendToConnections(conns.Keys, msg.Method, msg.Payload);
                        }
                    }
                    break;
            }
        }, _cts.Token);
    }
}