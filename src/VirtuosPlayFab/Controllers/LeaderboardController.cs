using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using VirtuosPlayFab.Models;
using VirtuosPlayFab.Service.LeaderboardService;

namespace VirtuosPlayFab.Controllers
{
    [Route("api/[controller]")]
    public class LeaderboardController:Controller
    {
        private readonly ILeaderboardService leaderboardService;

        public LeaderboardController(ILeaderboardService leaderboardService)
        {
            this.leaderboardService = leaderboardService;
        }

        [HttpPost("RankFor")]
        public async Task<IActionResult> LeaderboardRankFor([FromBody]LeaderboardRankForRequest request)
        {
            LeaderboardRankForResponse response = new LeaderboardRankForResponse();
            try
            {
                if (request == null || request.LeaderboardName == null || request.PlayerId == null)
                {
                    return BadRequest("Invalid Parameter");
                }
                response.Position = await leaderboardService.RankFor(request.PlayerId, request.LeaderboardName);
            }
            catch (Exception)
            {
                return BadRequest("Internal leaderboardService Rank For error");
            }
            return new OkObjectResult(response);
        }
    }
}
