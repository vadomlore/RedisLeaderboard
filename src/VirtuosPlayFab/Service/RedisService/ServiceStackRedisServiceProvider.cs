using ServiceStack.Redis;
using VirtuosPlayFab.Config;

namespace VirtuosPlayFab.Service.RedisService
{
    public class ServiceStackRedisServiceProvider: IServiceStackRedisServiceProvider
    {
        private IRedisClientsManager redisClientManager;
        public ServiceStackRedisServiceProvider(RedisSettings setting)
        {
            redisClientManager = new RedisManagerPool($"{setting.Host}:{setting.Port}");
        }
        public IRedisClientsManager RedisClientManager
        {
            get
            {
                return redisClientManager;
            }
        }
    }
}
