namespace VirtuosPlayFab.Config
{

    public class Appsettings
    {
        public RedisSettings RedisSettings { get; set; }

        public LeaderboardSettings LeaderboardSettings { get; set; }
    }
    /// <summary>
    /// the redis settings
    /// </summary>
    public class RedisSettings
    {
        /// <summary>
        /// redis port
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// redis host
        /// </summary>
        public string Host { get; set; }
    }

    /// <summary>
    /// the leaderboard settings
    /// </summary>
    public class LeaderboardSettings
    {
        public int PageSize { get; set; }
    }
}