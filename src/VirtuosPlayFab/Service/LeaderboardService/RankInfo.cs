using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VirtuosPlayFab.Service.LeaderboardService
{
    public class RankInfo
    {
        public string Member { get; set; }
        public double? Score { get; set; }
        public long? Position { get; set; }
        public string MemberData { get; set; }
    }
}
