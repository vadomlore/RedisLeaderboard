using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VirtuosPlayFab.Service.LeaderboardService
{
    public interface ILeaderboardService
    {
        /// <summary>
        /// get the member rank position for the player.
        /// </summary>
        /// <param name="member"></param>
        /// <param name="leaderboardName"></param>
        /// <returns></returns>
        Task<long> RankFor(string member, string leaderboardName);

        /// <summary>
        /// get the members infomration by rank range;
        /// </summary>
        /// <param name="startRanking"></param>
        /// <param name="endRanking"></param>
        /// <param name="leaderboardName"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        Task<List<RankInfo>> MembersFromRankRange(int startRanking, int endRanking, string leaderboardName, LeaderboardRequestOptions options);
    }
}
