using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VirtuosPlayFab.Models
{
    public class LeaderboardRankForRequest
    {
        public string PlayerId { get; set; }

        public string LeaderboardName { get; set; }

    }

    public class LeaderboardRankForResponse
    {
        public long Position { get; set; }
    }
}
