using ServiceStack.Redis;

namespace VirtuosPlayFab.Service.RedisService
{
    public interface IServiceStackRedisServiceProvider
    {
        IRedisClientsManager RedisClientManager { get; }
    }
}
