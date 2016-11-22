using VirtuosPlayFab.Config;
using StackExchange.Redis;
namespace VirtuosPlayFab.Service.RedisService
{
    public class StackExchangeRedisServiceProvider : IStackExchangeRedisServiceProvider
    {
        private ConnectionMultiplexer multiplexer;
        public StackExchangeRedisServiceProvider(RedisSettings setting)
        {
            multiplexer = ConnectionMultiplexer.Connect($"{setting.Host}:{setting.Port}");
        }
        public IDatabase StackExchangeDB
        {
            get
            {
                return multiplexer.GetDatabase();
            }
        }
    }
}
