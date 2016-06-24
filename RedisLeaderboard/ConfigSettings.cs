using System.Configuration;
namespace RedisLeaderboard
{
    public static class ConfigSettings
    {
        public static int LeaderBoardPageSize => int.Parse(ConfigurationManager.AppSettings["LeaderboardPageSize"]);

        public static int RedisPort => int.Parse(ConfigurationManager.AppSettings["DefaultRedisPort"]);

        public static string RedisHost => ConfigurationManager.AppSettings["DefaultRedisHost"].ToString();
    }
}