using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLeaderboard
{
    public class RedisOptions
    {
        public string connectionString;      
        public RedisOptions()
        {
            connectionString = $"{ConfigSettings.RedisHost}:{ConfigSettings.RedisPort}";
        }
        public RedisOptions(string connectionString)
        {
            this.connectionString = connectionString;
        }
        public void Merge(RedisOptions redisOptions)
        {
            if (redisOptions.connectionString != this.connectionString)
            {
                this.connectionString = redisOptions.connectionString;
            }
        }
    }
}
