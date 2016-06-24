using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLeaderboard
{
    //public class AppHost : AppSelfHostBase
    //{
    //    public AppHost() : base("AWS Redis ElastiCache Example", typeof(MyServices).Assembly) { }

    //    public override void Configure(Container container)
    //    {
    //    //Your DB initialization
    //    ...

    //    // AWS ElastiCache servers are NOT accessible from outside AWS
    //    // Use MemoryCacheClient locally
    //    if (AppSettings.GetString("Environment") == "Production")
    //        {
    //            container.Register<IRedisClientsManager>(c =>
    //                new PooledRedisClientManager(
    //                    // Primary node from AWS (master)
    //                    AwsElastiCacheConfig.MasterNodes,
    //                    // Read replica nodes from AWS (slaves)
    //                    AwsElastiCacheConfig.SlaveNodes));

    //            container.Register<ICacheClient>(c =>
    //                container.Resolve<IRedisClientsManager>().GetCacheClient());
    //        }
    //        else
    //        {
    //            container.Register<ICacheClient>(new MemoryCacheClient());
    //        }
    //    }
    //}
}
