using ServiceStack.Redis;
using StackExchange.Redis;

namespace VirtuosPlayFab.Service.RedisService
{
    public interface IStackExchangeRedisServiceProvider
    {
        IDatabase StackExchangeDB { get; }
    }
}
