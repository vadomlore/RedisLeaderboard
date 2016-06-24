using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLeaderboard
{
    public class RankCondition
    {
        //Member name.
        public string Member { get; set; }

        //Current score for the member in the leaderboard
        public double CurrentScore { get; set; }

        //Member score
        public double Score { get; set; }

        //MemberData
        public string MemberData { get; set; }

        //leaderboard options
        public bool reverse { get; set; }

    }
}
