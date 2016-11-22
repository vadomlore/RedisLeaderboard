using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using VirtuosPlayFab.Config;
using VirtuosPlayFab.Service.RedisService;
namespace VirtuosPlayFab.Service.LeaderboardService
{
    public class StackExchangeLeaderboardService:ILeaderboardService
    {
        public static LeaderboardOptions defaultLeaderBoardOptions = new LeaderboardOptions();

        private IStackExchangeRedisServiceProvider provider;

        private readonly Appsettings appSettings;

        private string _leaderboardName;

        public LeaderboardOptions LeaderBoardOptions { get; set; }

        public string LeaderboardName
        {
            get
            {
                if (string.IsNullOrEmpty(_leaderboardName))
                {
                    throw new InvalidOperationException("Must call initialize leaderboard first to get leaderboard options");
                }
                return _leaderboardName;
            }
            set
            {
                _leaderboardName = value;
            }
        }

        public StackExchangeLeaderboardService(IStackExchangeRedisServiceProvider provider)
        {
            this.provider = provider;
        }

        /// <summary>
        /// Key for retrieving optional member data.
        /// </summary>
        /// <param name="leaderboardName"></param>
        /// <returns>@return a key in the form of +leaderboard_name:member_data+</returns>
        string MemberDataKey(string leaderboardName)
        {
            return this.LeaderBoardOptions.GlobalMemberData == false ? $"{leaderboardName}:{this.LeaderBoardOptions.MemberDataNamespace}" : this.LeaderBoardOptions.MemberDataNamespace;
        }

        /// <summary>
        /// alidate and return the page size. Returns the +DEFAULT_PAGE_SIZE+ if the page size is less than 1.
        /// </summary>
        /// <param name="pageSize">param page_size [int] Page size.</param>
        /// <returns>@return the page size. Returns the +DEFAULT_PAGE_SIZE+ if the page size is less than 1.</returns>
        int ValidatePageSize(int pageSize)
        {
            if (pageSize < 1)
            {
                return this.LeaderBoardOptions.PageSize;
            }
            else
            {
                return pageSize;
            }
        }

        void Initialize(string leaderboardName, LeaderboardOptions leaderboardOptions = null)
        {
            this._leaderboardName = leaderboardName;
            this.LeaderBoardOptions = defaultLeaderBoardOptions;
            if (leaderboardOptions != null)
            {
                this.LeaderBoardOptions.Merge(leaderboardOptions);
            }
        }

        /// <summary>
        /// Rank a member in the leaderboard.
        /// </summary>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score.</param>
        /// <param name="memberData">Optional member data </param>
        public Task RankMemeber(string member, double score, string memberData = null)
        {
            return RankMemberIn(this.LeaderboardName, member, score, memberData);
        }

        /// <summary>
        /// Rank a member in the named leaderboard.
        /// </summary>
        /// <param name="leaderBoardName">Name of the leaderboard</param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional member data</param>
        public Task<bool> RankMemberIn(string leaderBoardName, string member, double score, string memberData = null)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            db.SortedSetAddAsync(leaderBoardName, member, score);
            if (memberData != null)
            {
                db.HashSetAsync(MemberDataKey(leaderBoardName), member, memberData);
            }
            return trans.ExecuteAsync();
        }

        /// <summary>
        /// Rank a member across multiple leaderboards
        /// </summary>
        /// <param name="leaderboards">Leaderboard names</param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional member data</param>
        public Task RankMemberAcross(List<string> leaderboards, string member, double score, string memberData = null)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            leaderboards.ForEach(leaderboardName =>
            {
                db.SortedSetAddAsync(leaderboardName, member, score);
                if (memberData != null)
                {
                    db.HashSetAsync(MemberDataKey(leaderboardName), member, memberData);
                }
            });
            trans.ExecuteAsync();
            return TaskDone.Done;
        }


        /// <summary>
        /// Delegate which must return +true+ or +false+ that controls whether or not the member is ranked in the leaderboard.
        /// </summary>
        /// <param name="rankCondition"></param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional MemberData</param>
        public Task RankMemberIf(Predicate<RankCondition> rankCondition, string member, double score, string memberData = null)
        {
            return RankMemberIfIn(this.LeaderboardName, rankCondition, member, score, memberData);
        }

        /// <summary>
        /// Rank a member in the named leaderboard based on execution of the +rank_conditional+.
        /// </summary>
        /// <param name="leaderboardName"> Name of the leaderboard.</param>
        /// <param name="rankCondition"> Lambda which must return +true+ or +false+ that controls whether or not the member is ranked in the leaderboard.</param>
        /// <param name="member">Member name.</param>
        /// <param name="score">Member score.</param>
        /// <param name="memberData">Optional MemberData.</param>
        public async Task<bool> RankMemberIfIn(string leaderboardName, Predicate<RankCondition> rankCondition, string member, double score, string memberData = null)
        {
            var db = provider.StackExchangeDB;
            var currentScore = await db.SortedSetScoreAsync(leaderboardName, member) ?? 0.0;
            if (rankCondition(new RankCondition()
            {
                Member = member,
                CurrentScore = currentScore,
                Score = score,
                MemberData = memberData,
                reverse = this.LeaderBoardOptions.Reverse
            }))
            {
                return await RankMemberIn(leaderboardName, member, score, memberData);
            }
            return true;
        }


        /// <summary>
        /// Retrieve the optional member data for a given member in the leaderboard.
        /// </summary>
        /// <param name="member">member name</param>
        /// <returns>optional MemberData</returns>
        public async Task<string> MemberDataFor(string member)
        {
            var value = await MemberDataForIn(this.LeaderboardName, member);
            return value;
        }

        /// <summary>
        /// Retrieve the optional member data for a given member in the named leaderboard.
        /// </summary>
        /// <param name="leaderboardName">Name of the leaderboard</param>
        /// <param name="member">Memeber name</param>
        /// <returns>optional MemberData</returns>
        public Task<RedisValue> MemberDataForIn(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;
            return db.HashGetAsync(MemberDataKey(leaderboardName), member);
        }

        /// <summary>
        /// update optional MemberData for current leaderboard 
        /// </summary>
        /// <param name="member">Member name</param>
        /// <param name="memberData">optional member data</param>
        public Task<bool> UpdateMemberData(string member, string memberData)
        {
            return UpdateMemberDataIn(this.LeaderboardName, member, memberData);
        }

        /// <summary>
        /// update optional MemberData for a given named leaderboard 
        /// </summary>
        /// <param name="leaderboardName">the leaderboard name</param>
        /// <param name="member">member name</param>
        /// <param name="memberData">optional member data</param>
        public Task<bool> UpdateMemberDataIn(string leaderboardName, string member, string memberData)
        {
            var db = provider.StackExchangeDB;
            return db.HashSetAsync(MemberDataKey(leaderboardName), member, memberData);
        }


        /// <summary>
        /// remove optional MemberData for current leaderboard 
        /// </summary>
        /// <param name="member">Member name</param>
        public async Task<bool> RemoveMemberData(string member)
        {
            return await RemoveMemberDataIn(this.LeaderboardName, member);
        }

        /// <summary>
        /// remove optional MemberData for a given named leaderboard 
        /// </summary>
        /// <param name="leaderboardName">the leaderboard name</param>
        /// <param name="member">member name</param>
        public Task<bool> RemoveMemberDataIn(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;
            return db.HashDeleteAsync(MemberDataKey(leaderboardName), member);
        }

        /// <summary>
        /// rank a list of members in current leaderboard
        /// </summary>
        /// <param name="memberScoreCollection">a list of members and scores</param>
        public Task RankMemebers(List<MemberScore> memberScoreCollection)
        {
            return RankMembersIn(this.LeaderboardName, memberScoreCollection);
        }

        /// <summary>
        /// Rank a list of members in the named leaderboard.
        /// </summary>
        /// <param name="leaderBoardName"></param>
        /// <param name="memberScoreCollection">a list of members and scores</param>
        public Task RankMembersIn(string leaderBoardName, List<MemberScore> memberScoreCollection)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            memberScoreCollection.ForEach(memberScore =>
            {
                db.SortedSetAddAsync(leaderBoardName, memberScore.Member, memberScore.Score);
            });
            return trans.ExecuteAsync();
        }


        /// <summary>
        /// remove member from current leaderboard
        /// </summary>
        /// <param name="member">Member name</param>
        public Task RemoveMember(string member)
        {
            return RemoveMemberFrom(this.LeaderboardName, member);
        }

        /// <summary>
        /// remove member from named leaderboard
        /// </summary>
        /// <param name="leaderboardName"></param>
        /// <param name="member"></param>
        public Task RemoveMemberFrom(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            trans.SortedSetRemoveAsync(leaderboardName, member);
            trans.HashDeleteAsync(MemberDataKey(leaderboardName), member);
            return trans.ExecuteAsync();
        }

        /// <summary>
        /// Retrieve the total numbers of the leaderboard
        /// </summary>
        async Task<long> TotalMembers()
        {
            return await TotalMembersIn(this.LeaderboardName);
        }

        /// <summary>
        /// Retrieve the total numbers of the named leaderboard
        /// </summary>
        Task<long> TotalMembersIn(string leaderboardName)
        {
            var db = provider.StackExchangeDB;
            return db.SortedSetLengthAsync(leaderboardName);
        }

        /// <summary>
        /// Retrieve the total pages of the leaderboard
        /// </summary>
        public async Task<int> TotalPages(int? pageSize)
        {
            return await TotalPagesIn(this.LeaderboardName, pageSize);
        }

        /// <summary>
        /// Retrieve the total pages of the named leaderboard
        /// </summary>
        public async Task<int> TotalPagesIn(string leaderboardName, int? pageSize)
        {
            var _pageSize = this.LeaderBoardOptions.PageSize;
            if (!pageSize.HasValue)
            {
                _pageSize = pageSize.Value;
            }
            decimal page = (await (TotalMembersIn(leaderboardName)) / _pageSize);
            return (int)Math.Ceiling(page);
        }

        public Task<long> TotalMembersInScoreRange(double minScore, double MaxScore)
        {
            return TotalMembersInScoreRangeIn(this.LeaderboardName, minScore, MaxScore);
        }


        public Task<long> TotalMembersInScoreRangeIn(string leaderboardName, double minScore, double maxScore)
        {
            var db = provider.StackExchangeDB;
            return db.SortedSetLengthAsync(leaderboardName, minScore, maxScore);
        }

        public Task<bool> ChangeScoreFor(string member, double delta, string memberData = null)
        {
            return ChangeScoreForMemberIn(this.LeaderboardName, member, delta, memberData);
        }

        public Task<bool> ChangeScoreForMemberIn(string leaderboardName, string member, double delta, string memberData = null)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            trans.SortedSetIncrementAsync(leaderboardName, member, delta);
            if (memberData == null)
            {
                trans.HashSetAsync(MemberDataKey(leaderboardName), member, memberData);
            }
            return trans.ExecuteAsync();
        }

        /// <summary>
        /// get specified the player rank
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public Task<long> RankFor(string member)
        {
            return RankForIn(this.LeaderboardName, member);
        }

        /// <summary>
        /// get specified the player in rank
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public async Task<long> RankForIn(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;
            if (this.LeaderBoardOptions.Reverse)
            {
                var value = await db.SortedSetRankAsync(leaderboardName, member, Order.Descending);
                value = value ?? 0;
                return value.Value + 1;
            }
            else
            {
                var value = await db.SortedSetRankAsync(leaderboardName, member, Order.Ascending);
                value = value ?? 0;
                return value.Value + 1;
            }
        }

        public Task<double> ScoreFor(string member)
        {
            return ScoreForIn(this.LeaderboardName, member);
        }

        public async Task<double> ScoreForIn(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;
            return await db.SortedSetScoreAsync(leaderboardName, member) ?? 0.0;
        }

        public Task<bool> MemberExists(string member)
        {
            return MemberExistsIn(this.LeaderboardName, member);
        }

        public async Task<bool> MemberExistsIn(string leaderboardName, string member)
        {
            var exists = false;
            var db = provider.StackExchangeDB;
            exists = (await db.SortedSetRankAsync(leaderboardName, member) == null) ? false : true;
            return exists;
        }

        public Task<RankInfo> ScoreAndRank(string member)
        {
            return ScoreAndRankFor(this.LeaderboardName, member);
        }

        public async Task<RankInfo> ScoreAndRankFor(string leaderboardName, string member)
        {
            var db = provider.StackExchangeDB;

            var score = 0.0;
            var index = 0L;
            var trans = db.CreateTransaction();
            score = await trans.SortedSetScoreAsync(leaderboardName, member) ?? 0.0;
            if (this.LeaderBoardOptions.Reverse)
            {
                index = await db.SortedSetRankAsync(leaderboardName, member, Order.Ascending) ?? 0L;
            }
            else
            {
                index = await db.SortedSetRankAsync(leaderboardName, member, Order.Descending) ?? 0L;
            }
            var task = trans.ExecuteAsync();
            return new RankInfo()
            {
                Member = member,
                Position = index + 1,
                Score = score
            };
        }

        public Task RemoveMembersInScoreRange(double minScore, double maxScore)
        {
            return RemoveMembersInScoreRangeIn(this.LeaderboardName, minScore, maxScore);
        }

        public Task RemoveMembersInScoreRangeIn(string leaderboardName, double minScore, double maxScore)
        {
            var db = provider.StackExchangeDB;
            return db.SortedSetRemoveRangeByScoreAsync(leaderboardName, minScore, maxScore);
        }

        public Task<long> RemoveMembersOutsideRank(int rank)
        {
            return RemoveMembersOutsideRankIn(this.LeaderboardName, rank);
        }

        public Task<long> RemoveMembersOutsideRankIn(string leaderboardName, int rank)
        {
            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    return db.SortedSetRemoveRangeByRankAsync(leaderboardName, rank, -1);
                }
                else
                {
                    return db.SortedSetRemoveRangeByRankAsync(leaderboardName, 0, -(rank) - 1);
                }
            }
        }

        public Task<double> PercentileFor(string member)
        {
            return PercentileForIn(this.LeaderboardName, member);
        }

        public async Task<double> PercentileForIn(string leaderboardName, string member)
        {
            if (!await MemberExistsIn(leaderboardName, member))
            {
                return 0;
            }
            var db = provider.StackExchangeDB;
            {
                long count = 0L;
                var index = 0L;
                var trans = db.CreateTransaction();
                {
                    count = await trans.SortedSetLengthAsync(leaderboardName);
                    index = await db.SortedSetRankAsync(leaderboardName, member, Order.Descending) ?? 0L;
                    var task = trans.ExecuteAsync();
                }
                double percentile = (count - index - 1) * 1.0 / count * 100;
                if (this.LeaderBoardOptions.Reverse)
                {
                    percentile = 100 - percentile;
                }
                return percentile;
            }
        }


        /// <summary>
        /// calca the score for a given percentile value in the leaderboard
        /// </summary>
        /// <param name="percentile"></param>
        /// <returns></returns>
        public Task<double> ScoreForPercentile(float percentile)
        {
            return ScoreForPercentileIn(this.LeaderboardName, percentile);
        }

        public async Task<double> ScoreForPercentileIn(string leaderboardName, float percentile)
        {
            if (percentile < 0 || percentile > 100)
            {
                return 0;
            }
            long totalMembers = await TotalMembersIn(leaderboardName);
            if (totalMembers < 1) return 0;
            if (this.LeaderBoardOptions.Reverse)
            {
                percentile = 100 - percentile;
            }
            var index = (totalMembers - 1) * (percentile / 100.0);

            SortedSetEntry[] scores;
            var db = provider.StackExchangeDB;
            scores = await db.SortedSetRangeByRankWithScoresAsync(leaderboardName, (int)Math.Floor(index), (int)Math.Ceiling(index));
            if (Double.Equals(index, Math.Ceiling(index)))
            {
                return scores[0].Score;
            }
            else
            {
                var interpolateFraction = index - Math.Floor(index);
                return scores[0].Score + interpolateFraction * (scores[1].Score - scores[0].Score);
            }
        }

        /// <summary>
        /// Determine the page where a member falls in the leaderboard.
        /// </summary>
        /// <param name="member">Member name.</param>
        /// <param name="pageSize">Page size to be used in determining page location.</param>
        /// <returns>return the page where a member falls in the leaderboard or 0 if error</returns>
        public async Task<int> PageFor(string member, int pageSize = -1)
        {
            if (pageSize < 0)
            {
                pageSize = appSettings.LeaderboardSettings.PageSize;
            }
            return await PageForIn(this.LeaderboardName, member, pageSize);
        }

        /// <summary>
        /// Determine the page where a member falls in the leaderboard.
        /// </summary>
        /// <param name="leaderboardName">leaderboard name</param>
        /// <param name="member">Member name.</param>
        /// <param name="pageSize">pageSize</param>
        /// <returns></returns>
        public async Task<int> PageForIn(string leaderboardName, string member, int pageSize = -1)
        {
            long rankForMember;

            if (pageSize < 0)
            {
                pageSize = appSettings.LeaderboardSettings.PageSize;
            }

            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse == true)
                {
                    rankForMember = await db.SortedSetRankAsync(leaderboardName, member, Order.Ascending) ?? 0;
                }
                else
                {
                    rankForMember = await db.SortedSetRankAsync(leaderboardName, member, Order.Descending) ?? 0;
                }
            }
            if (rankForMember < 0)
            {
                rankForMember = 0;
            }
            rankForMember += 1;
            decimal data = rankForMember / pageSize;
            return (int)Math.Ceiling(data);
        }

        public Task ExpireLeaderboard(DateTime date)
        {
            return ExpireLeaderboardFor(this.LeaderboardName, date);
        }

        public Task ExpireLeaderboardFor(string leaderboardName, DateTime date)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            trans.KeyExpireAsync(leaderboardName, date);
            trans.KeyExpireAsync(MemberDataKey(leaderboardName), date);
            return trans.ExecuteAsync();
        }

        public async Task<List<RankInfo>> LeadersIn(string leaderboardName, int currentPage, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            if (currentPage < 1)
            {
                currentPage = 1;
            }
            int pageSize = ValidatePageSize(leaderboardRequestOptions.PageSize);
            var indexForRedis = currentPage - 1;
            var startingOffset = indexForRedis * pageSize;
            if (startingOffset < 0)
            {
                startingOffset = 0;
            }

            RedisValue[] rawLeaderData;
            var endingOffset = (startingOffset + pageSize) - 1;
            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, startingOffset, endingOffset, Order.Ascending);
                }
                else
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, startingOffset, endingOffset, Order.Descending);
                }
            }
            if (rawLeaderData != null)
            {
                var data = (from v in rawLeaderData
                    where !v.IsNullOrEmpty
                    select (string)v).ToList();
                return await RankedInListIn(leaderboardName, data, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }

        public async Task<List<RankInfo>> AllLeadersFrom(string leaderboardName, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            RedisValue[] rawLeaderData = null;
            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, 0, -1, Order.Ascending);
                }
                else
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, 0, -1, Order.Descending);
                }
            }
            if (rawLeaderData != null)
            {
                var data = (from v in rawLeaderData
                            where !v.IsNullOrEmpty
                            select (string)v).ToList();
                return await RankedInListIn(leaderboardName, data, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }


        public Task<List<RankInfo>> MembersFromScoreRange(double minScore, double maxScore, LeaderboardRequestOptions options)
        {
            return MembersFromScoreRangeIn(this.LeaderboardName, minScore, maxScore, options);
        }

        public async Task<List<RankInfo>> MembersFromScoreRangeIn(string leaderboardName, double minScore, double maxScore, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            RedisValue[] rawLeaderData;
            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = await db.SortedSetRangeByScoreAsync(leaderboardName, minScore, maxScore, order:Order.Ascending);
                }
                else
                {
                    rawLeaderData = await db.SortedSetRangeByScoreAsync(leaderboardName, minScore, maxScore, order: Order.Descending);
                }
            }
            if (rawLeaderData != null)
            {
                var data = (from v in rawLeaderData
                            where !v.IsNullOrEmpty
                            select (string)v).ToList();
                return await RankedInListIn(leaderboardName, data, options);
            }
            else
            {
                return new List<RankInfo>();
            }
        }

        public Task<List<RankInfo>> MembersFromRankRange(int startRanking, int endRanking, LeaderboardRequestOptions options)
        {
            return MembersFromRankRangeIn(this.LeaderboardName, startRanking, endRanking, options);
        }

        public async Task<List<RankInfo>> MembersFromRankRangeIn(string leaderboardName, int startRanking, int endRanking, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            startRanking -= 1;
            if (startRanking < 0)
                startRanking = 0;

            endRanking -= 1;
            if (endRanking > await TotalMembersIn(leaderboardName))
                endRanking = (int)(await TotalMembersIn(leaderboardName) - 1);

            RedisValue[] rawLeaderData;
            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, startRanking, endRanking, Order.Ascending);
                }
                else
                {
                    rawLeaderData = await db.SortedSetRangeByRankAsync(leaderboardName, startRanking, endRanking, Order.Descending);
                }
            }
            if (rawLeaderData != null)
            {
                var data = (from v in rawLeaderData
                            where !v.IsNullOrEmpty
                            select (string)v).ToList();
                return await RankedInListIn(leaderboardName, data, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }


        public Task<List<RankInfo>> Top(int number, LeaderboardRequestOptions options)
        {
            return TopIn(this.LeaderboardName, number, options);
        }

        public Task<List<RankInfo>> TopIn(string leaderboardName, int number, LeaderboardRequestOptions options)
        {
            return MembersFromRankRangeIn(leaderboardName, 1, number, options);
        }

        public Task<RankInfo> MemberAt(int position, LeaderboardRequestOptions options)
        {
            return MemberAtIn(this.LeaderboardName, position, options);
        }

        public async Task<RankInfo> MemberAtIn(string leaderbaordName, int position, LeaderboardRequestOptions options)
        {
            if (position <= await TotalMembersIn(leaderbaordName))
            {
                var leaderboardRequestOptions = new LeaderboardRequestOptions();
                leaderboardRequestOptions.Merge(options);
                var pageSize = ValidatePageSize(leaderboardRequestOptions.PageSize);
                var currentPage = (int)Math.Ceiling((decimal)(position / pageSize));
                var offset = (position - 1) % pageSize;
                var leaders = await LeadersIn(leaderbaordName, currentPage, leaderboardRequestOptions);
                if (leaders != null && leaders.Count > 0)
                {
                    return leaders[offset];
                }
            }
            return null;
        }


        public Task<List<RankInfo>> AroundMe(string member, LeaderboardRequestOptions options)
        {
            return AroundMeIn(this.LeaderboardName, member, options);
        }


        public async Task<List<RankInfo>> AroundMeIn(string leaderboardName, string member, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            long reverseRankForMe;

            var db = provider.StackExchangeDB;
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    reverseRankForMe = await db.SortedSetRankAsync(leaderboardName, member, Order.Ascending)?? 0;
                }
                else
                {
                    reverseRankForMe = await db.SortedSetRankAsync(leaderboardName, member, Order.Descending) ?? 0;
                }

                if (reverseRankForMe < 0)
                {
                    return new List<RankInfo>();
                }
                var pageSize = ValidatePageSize(leaderboardRequestOptions.PageSize);
                var startingOffset = reverseRankForMe - (pageSize / 2);
                if (startingOffset < 0)
                {
                    startingOffset = 0;
                }
                var endOffset = startingOffset + pageSize - 1;

                RedisValue[] rawLeaderData = null;
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = db.SortedSetRangeByRank(leaderboardName, (int)startingOffset, (int)endOffset, Order.Ascending);
                }
                else
                {
                    rawLeaderData = db.SortedSetRangeByRank(leaderboardName, (int)startingOffset, (int)endOffset, Order.Descending);
                }
                if (rawLeaderData != null)
                {
                    var data = (from v in rawLeaderData
                                where !v.IsNullOrEmpty
                                select (string)v).ToList();
                    return await RankedInListIn(leaderboardName, data, leaderboardRequestOptions);
                }
                else
                {
                    return new List<RankInfo>();
                }
            }
        }
        
        //implement the aggregate operation. options = {:aggregate => :sum})
        //implement the aggregate operation. options = {:aggregate => :sum})

        public Task LeaderboardsSetOperation(string destination, List<string> keys, SetOperation setOp, Aggregate aggregation)
        {
            var db = provider.StackExchangeDB;
            List<RedisKey> redisKeys = new List<RedisKey>();
            keys.ForEach(key =>
            {
                var redisKey = new RedisKey();
                redisKey.Append(key);
                redisKeys.Add(redisKey);
            });
            return db.SortedSetCombineAndStoreAsync(setOp, destination, redisKeys.ToArray(), null, aggregation);
        }


        public async Task<List<RankInfo>> RankedInListIn(string leaderboardName, List<string> members, LeaderboardRequestOptions options)
        {

            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            if (leaderboardRequestOptions.MembersOnly)
            {
                return members.Select(e => new RankInfo() { Member = e }).ToList();
            }
            var ranksForMembers = new List<RankInfo>();
            List<long> ranks = new List<long>();
            List<double> scores = new List<double>();
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            List<Task<long?>> tasks = new List<Task<long?>>();
            List<Task<double?>> scoreTasks = new List<Task<double?>>();
            //List<Task<RedisValue>> memberTasks = new List<Task<RedisValue>>();
            Dictionary<string, Task<RedisValue>> memberTasksMap = new Dictionary<string, Task<RedisValue>>();

            foreach (var member in members)
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    tasks.Add(db.SortedSetRankAsync(leaderboardName, member, Order.Ascending));
                }
                else
                {
                    await db.SortedSetRankAsync(leaderboardName, member, Order.Descending);
                }
                scoreTasks.Add(trans.SortedSetScoreAsync(leaderboardName, member));
            }
            var executeTask = trans.ExecuteAsync();
            await Task.WhenAll(tasks);
            foreach (var t in tasks)
            {
                ranks.Add(t.Result ?? 0);
            }
            foreach (var t in scoreTasks)
            {
                scores.Add(t.Result ?? 0);
            }

            for (int index = 0; index < members.Count; index++)
            {       
                if (leaderboardRequestOptions.WithDataMember)
                {
                    memberTasksMap.Add(members[index], MemberDataForIn(leaderboardName, members[index]));
                }
            }
            await Task.WhenAll(memberTasksMap.Values.ToArray());
            for (int index = 0; index < members.Count; index++)
            {
                var data = new RankInfo();
                try
                {
                    data.Position = ranks[index] + 1;
                    data.Score = scores[index];
                }
                catch (Exception)
                {
                    data.Position = null;
                }
                data.Member = members[index];
                if (data.Position == null)
                    continue;
                if (leaderboardRequestOptions.WithDataMember)
                {
                    //data.MemberData = MemberDataForIn(leaderboardName, members[index]);
                    data.MemberData = memberTasksMap[data.Member].Result;
                }
                ranksForMembers.Add(data);
            }

            switch (leaderboardRequestOptions.SortBy.ToLower())
            {
                case "rank":
                    ranksForMembers.Sort(delegate (RankInfo x, RankInfo y)
                    {
                        if (y.Position == null) return -1;
                        if (x.Position == null) return 1;
                        else
                        {
                            return (int)(x.Position - y.Position);
                        }
                    });
                    break;
                case "score":
                    ranksForMembers.Sort(delegate (RankInfo x, RankInfo y)
                    {
                        if (y.Score == null) return -1;
                        if (x.Score == null) return 1;
                        else
                        {
                            return (int)(x.Score - y.Score);
                        }
                    });
                    break;
                default:
                    break;
            }
            return ranksForMembers;
        }

        /// <summary>
        /// delete the named leaderboard
        /// </summary>
        /// <param name="leaderBoardName"></param>
        public Task DeleteLeaderboard(string leaderBoardName)
        {
            var db = provider.StackExchangeDB;
            var trans = db.CreateTransaction();
            trans.KeyDeleteAsync(leaderBoardName);
            trans.KeyDeleteAsync(MemberDataKey(leaderBoardName));
            return trans.ExecuteAsync();                
        }

        /// <summary>
        /// disconnect the redis connection
        /// </summary>
        public void DisConnect()
        {

        }

        #region Controller Call Api
        
        public Task<long> RankFor(string member, string leaderboardName)
        {
            Initialize(leaderboardName);
            return RankFor(member);
        }

        public Task<List<RankInfo>> MembersFromRankRange(int startRanking, int endRanking, string leaderboardName, LeaderboardRequestOptions options)
        {
            Initialize(leaderboardName);
            return MembersFromRankRange(startRanking, endRanking, options);
        }
        #endregion
    }
}
