using ServiceStack.Redis;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using VirtuosPlayFab.Config;
using VirtuosPlayFab.Service.RedisService;
namespace VirtuosPlayFab.Service.LeaderboardService
{
    public class ServiceStackLeaderboardService
    {
        private IServiceStackRedisServiceProvider provider;

        private readonly Appsettings appSettings;

        public string LeaderboardName { get; set; }

        //public static LeaderboardOptions defaultLeaderBoardOptions = new LeaderboardOptions();
        //public static RedisOptions defaultRedisOptions = new RedisOptions();

        public LeaderboardOptions LeaderBoardOptions { get; set; }

        public ServiceStackLeaderboardService(string leaderboardName, LeaderboardOptions leaderboardOptions = null)
        {
            this.LeaderboardName = leaderboardName;
            this.LeaderBoardOptions = new LeaderboardOptions();
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
        public void RankMemeber(string member, double score, string memberData = null)
        {
            RankMemberIn(this.LeaderboardName, member, score, memberData);
        }

        /// <summary>
        /// Rank a member in the named leaderboard.
        /// </summary>
        /// <param name="leaderBoardName">Name of the leaderboard</param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional member data</param>
        public void RankMemberIn(string leaderBoardName, string member, double score, string memberData = null)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.AddItemToSortedSet(leaderBoardName, member, score));
                    if (memberData != null)
                    {

                        trans.QueueCommand(r => r.SetEntryInHash(MemberDataKey(leaderBoardName), member, memberData));
                    }
                    trans.Commit();
                }
            }
        }

        /// <summary>
        /// Rank a member across multiple leaderboards
        /// </summary>
        /// <param name="leaderboards">Leaderboard names</param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional member data</param>
        public void RankMemberAcross(List<string> leaderboards, string member, double score, string memberData = null)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    leaderboards.ForEach(leaderboardName =>
                    {
                        trans.QueueCommand(r => r.AddItemToSortedSet(leaderboardName, member, score));
                        if (memberData != null)
                        {
                            trans.QueueCommand(r => r.SetEntryInHash(MemberDataKey(leaderboardName), member, memberData));
                        }
                    });
                    trans.Commit();
                }
            }
        }


        /// <summary>
        /// Delegate which must return +true+ or +false+ that controls whether or not the member is ranked in the leaderboard.
        /// </summary>
        /// <param name="rankCondition"></param>
        /// <param name="member">Member name</param>
        /// <param name="score">Member score</param>
        /// <param name="memberData">Optional MemberData</param>
        public void RankMemberIf(Predicate<RankCondition> rankCondition, string member, double score, string memberData = null)
        {
            RankMemberIfIn(this.LeaderboardName, rankCondition, member, score, memberData);
        }

        /// <summary>
        /// Rank a member in the named leaderboard based on execution of the +rank_conditional+.
        /// </summary>
        /// <param name="leaderboardName"> Name of the leaderboard.</param>
        /// <param name="rankCondition"> Lambda which must return +true+ or +false+ that controls whether or not the member is ranked in the leaderboard.</param>
        /// <param name="member">Member name.</param>
        /// <param name="score">Member score.</param>
        /// <param name="memberData">Optional MemberData.</param>
        public void RankMemberIfIn(string leaderboardName, Predicate<RankCondition> rankCondition, string member, double score, string memberData = null)
        {
            var currentScore = 0.0;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                currentScore = (double)redis.GetItemScoreInSortedSet(leaderboardName, member);
                if (rankCondition(new RankCondition()
                {
                    Member = member,
                    CurrentScore = currentScore,
                    Score = score,
                    MemberData = memberData,
                    reverse = this.LeaderBoardOptions.Reverse
                }))
                {
                    RankMemberIn(leaderboardName, member, score, memberData);
                }
            }
        }


        /// <summary>
        /// Retrieve the optional member data for a given member in the leaderboard.
        /// </summary>
        /// <param name="member">member name</param>
        /// <returns>optional MemberData</returns>
        public string MemberDataFor(string member)
        {
            return MemberDataForIn(this.LeaderboardName, member);
        }

        /// <summary>
        /// Retrieve the optional member data for a given member in the named leaderboard.
        /// </summary>
        /// <param name="leaderboardName">Name of the leaderboard</param>
        /// <param name="member">Memeber name</param>
        /// <returns>optional MemberData</returns>
        public string MemberDataForIn(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                return redis.GetValueFromHash(MemberDataKey(leaderboardName), member);
            }
        }

        /// <summary>
        /// update optional MemberData for current leaderboard 
        /// </summary>
        /// <param name="member">Member name</param>
        /// <param name="memberData">optional member data</param>
        public void UpdateMemberData(string member, string memberData)
        {
            UpdateMemberDataIn(this.LeaderboardName, member, memberData);
        }

        /// <summary>
        /// update optional MemberData for a given named leaderboard 
        /// </summary>
        /// <param name="leaderboardName">the leaderboard name</param>
        /// <param name="member">member name</param>
        /// <param name="memberData">optional member data</param>
        public void UpdateMemberDataIn(string leaderboardName, string member, string memberData)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                redis.SetEntryInHash(MemberDataKey(leaderboardName), member, memberData);
            }
        }


        /// <summary>
        /// remove optional MemberData for current leaderboard 
        /// </summary>
        /// <param name="member">Member name</param>
        public void RemoveMemberData(string member)
        {
            RemoveMemberDataIn(this.LeaderboardName, member);
        }

        /// <summary>
        /// remove optional MemberData for a given named leaderboard 
        /// </summary>
        /// <param name="leaderboardName">the leaderboard name</param>
        /// <param name="member">member name</param>
        public void RemoveMemberDataIn(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                redis.RemoveEntryFromHash(MemberDataKey(leaderboardName), member);
            }
        }

        /// <summary>
        /// rank a list of members in current leaderboard
        /// </summary>
        /// <param name="memberScoreCollection">a list of members and scores</param>
        public void RankMemebers(List<MemberScore> memberScoreCollection)
        {
            RankMembersIn(this.LeaderboardName, memberScoreCollection);
        }

        /// <summary>
        /// Rank a list of members in the named leaderboard.
        /// </summary>
        /// <param name="leaderBoardName"></param>
        /// <param name="memberScoreCollection">a list of members and scores</param>
        public void RankMembersIn(string leaderBoardName, List<MemberScore> memberScoreCollection)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    memberScoreCollection.ForEach(memberScore =>
                    {
                        trans.QueueCommand(r => r.AddItemToSortedSet(leaderBoardName, memberScore.Member, memberScore.Score));
                    });
                    trans.Commit();
                }
            }
        }


        /// <summary>
        /// remove member from current leaderboard
        /// </summary>
        /// <param name="member">Member name</param>
        public void RemoveMember(string member)
        {
            RemoveMemberFrom(this.LeaderboardName, member);
        }

        /// <summary>
        /// remove member from named leaderboard
        /// </summary>
        /// <param name="leaderboardName"></param>
        /// <param name="member"></param>
        public void RemoveMemberFrom(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.RemoveItemFromSortedSet(leaderboardName, member));
                    trans.QueueCommand(r => r.RemoveEntryFromHash(MemberDataKey(leaderboardName), member));
                    trans.Commit();
                }
            }
        }

        /// <summary>
        /// Retrieve the total numbers of the leaderboard
        /// </summary>
        long TotalMembers()
        {
            return TotalMembersIn(this.LeaderboardName);
        }

        /// <summary>
        /// Retrieve the total numbers of the named leaderboard
        /// </summary>
        long TotalMembersIn(string leaderboardName)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                return redis.GetSortedSetCount(leaderboardName);
            }
        }

        /// <summary>
        /// Retrieve the total pages of the leaderboard
        /// </summary>
        public int TotalPages(int? pageSize)
        {
            return TotalPagesIn(this.LeaderboardName, pageSize);
        }

        /// <summary>
        /// Retrieve the total pages of the named leaderboard
        /// </summary>
        public int TotalPagesIn(string leaderboardName, int? pageSize)
        {
            var _pageSize = this.LeaderBoardOptions.PageSize;
            if (!pageSize.HasValue)
            {
                _pageSize = pageSize.Value;
            }
            decimal page = (TotalMembersIn(leaderboardName) / _pageSize);
            return (int)Math.Ceiling(page);
        }

        public long TotalMembersInScoreRange(double minScore, double MaxScore)
        {
            return TotalMembersInScoreRangeIn(this.LeaderboardName, minScore, MaxScore);
        }


        public long TotalMembersInScoreRangeIn(string leaderboardName, double minScore, double maxScore)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                return redis.GetSortedSetCount(leaderboardName, minScore, maxScore);
            }
        }


        public void ChangeScoreFor(string member, double delta, string memberData = null)
        {
            ChangeScoreForMemberIn(this.LeaderboardName, member, delta, memberData);
        }

        public void ChangeScoreForMemberIn(string leaderboardName, string member, double delta, string memberData = null)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.IncrementItemInSortedSet(leaderboardName, member, delta));
                    if (memberData != null)
                    {
                        trans.QueueCommand(r => r.SetEntryInHash(MemberDataKey(leaderboardName), member, memberData));
                    }
                    trans.Commit();
                }
            }
        }


        public long RankFor(string member)
        {
            return RankForIn(this.LeaderboardName, member);
        }

        public long RankForIn(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    return redis.GetItemIndexInSortedSet(leaderboardName, member) + 1;
                }
                else
                {
                    return redis.GetItemIndexInSortedSetDesc(leaderboardName, member) + 1;
                }
            }
        }

        public double ScoreFor(string member)
        {
            return ScoreForIn(this.LeaderboardName, member);
        }

        public double ScoreForIn(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                return redis.GetItemScoreInSortedSet(leaderboardName, member);
            }
        }

        public bool MemberExists(string member)
        {
            return MemberExistsIn(this.LeaderboardName, member);
        }

        public bool MemberExistsIn(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                return redis.SortedSetContainsItem(leaderboardName, member);
            }
        }

        public RankInfo ScoreAndRank(string member)
        {
            return ScoreAndRankFor(this.LeaderboardName, member);
        }

        public RankInfo ScoreAndRankFor(string leaderboardName, string member)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                var score = 0.0;
                var index = 0L;
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.GetItemScoreInSortedSet(leaderboardName, member), s => score = s);
                    if (this.LeaderBoardOptions.Reverse)
                    {
                        trans.QueueCommand(r => r.GetItemIndexInSortedSet(leaderboardName, member), i => index = i);
                    }
                    else
                    {
                        trans.QueueCommand(r => r.GetItemIndexInSortedSetDesc(leaderboardName, member), i => index = i);

                    }
                    trans.Commit();
                }
                return new RankInfo()
                {
                    Member = member,
                    Position = index + 1,
                    Score = score
                };
            }
        }

        public void RemoveMembersInScoreRange(double minScore, double maxScore)
        {
            RemoveMembersInScoreRangeIn(this.LeaderboardName, minScore, maxScore);
        }

        public void RemoveMembersInScoreRangeIn(string leaderboardName, double minScore, double maxScore)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                redis.RemoveRangeFromSortedSetByScore(leaderboardName, minScore, maxScore);
            }
        }

        public long RemoveMembersOutsideRank(int rank)
        {
            return RemoveMembersOutsideRankIn(this.LeaderboardName, rank);
        }

        public long RemoveMembersOutsideRankIn(string leaderboardName, int rank)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    return redis.RemoveRangeFromSortedSet(leaderboardName, rank, -1);
                }
                else
                {
                    return redis.RemoveRangeFromSortedSet(leaderboardName, 0, -(rank) - 1);
                }
            }
        }

        public double PercentileFor(string member)
        {
            return PercentileForIn(this.LeaderboardName, member);
        }

        public double PercentileForIn(string leaderboardName, string member)
        {
            if (!MemberExistsIn(leaderboardName, member))
            {
                return 0;
            }
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                long count = 0L;
                var index = 0L;
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.GetSortedSetCount(leaderboardName), i => count = i);
                    trans.QueueCommand(r => r.GetItemIndexInSortedSetDesc(leaderboardName, member), i => index = i);
                    trans.Commit();
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
        public double ScoreForPercentile(float percentile)
        {
            return ScoreForPercentileIn(this.LeaderboardName, percentile);
        }

        public double ScoreForPercentileIn(string leaderboardName, float percentile)
        {
            if (percentile < 0 || percentile > 100)
            {
                return 0;
            }
            long totalMembers = TotalMembersIn(leaderboardName);
            if (totalMembers < 1) return 0;
            if (this.LeaderBoardOptions.Reverse)
            {
                percentile = 100 - percentile;
            }
            var index = (totalMembers - 1) * (percentile / 100.0);

            List<double> scores;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                scores = redis.GetRangeWithScoresFromSortedSet(leaderboardName, (int)Math.Floor(index), (int)Math.Ceiling(index)).Values.ToList();
            }
            if (Double.Equals(index, Math.Ceiling(index)))
            {
                return scores[0];
            }
            else
            {
                var interpolateFraction = index - Math.Floor(index);
                return scores[0] + interpolateFraction * (scores[1] - scores[0]);
            }
        }

        /// <summary>
        /// Determine the page where a member falls in the leaderboard.
        /// </summary>
        /// <param name="member">Member name.</param>
        /// <param name="pageSize">Page size to be used in determining page location.</param>
        /// <returns>return the page where a member falls in the leaderboard or 0 if error</returns>
        public int PageFor(string member, int pageSize = -1)
        {
            if(pageSize < 0)
            {
                pageSize = appSettings.LeaderboardSettings.PageSize;
            }

            return PageForIn(this.LeaderboardName, member, pageSize);
        }

        /// <summary>
        /// Determine the page where a member falls in the leaderboard.
        /// </summary>
        /// <param name="leaderboardName">leaderboard name</param>
        /// <param name="member">Member name.</param>
        /// <param name="pageSize">pageSize</param>
        /// <returns></returns>
        public int PageForIn(string leaderboardName, string member, int pageSize = -1)
        {
            long rankForMember;

            if (pageSize < 0)
            {
                pageSize = appSettings.LeaderboardSettings.PageSize;
            }

            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                rankForMember = this.LeaderBoardOptions.Reverse == true ?
                    redis.GetItemIndexInSortedSet(leaderboardName, member) :
                    redis.GetItemIndexInSortedSetDesc(leaderboardName, member);
            }
            if (rankForMember < 0)
            {
                rankForMember = 0;
            }
            rankForMember += 1;
            decimal data = rankForMember / pageSize;
            return (int)Math.Ceiling(data);
        }


        public void ExpireLeaderboard(double seconds)
        {
            ExpireLeaderboardFor(this.LeaderboardName, seconds);
        }

        public void ExpireLeaderboardFor(string leaderboardName, double seconds)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.ExpireEntryIn(leaderboardName, TimeSpan.FromSeconds(seconds)));
                    trans.QueueCommand(r => r.ExpireEntryIn(MemberDataKey(leaderboardName), TimeSpan.FromSeconds(seconds)));
                    trans.Commit();
                }
            }
        }

        public void ExpireLeaderboard(DateTime date)
        {
            ExpireLeaderboardFor(this.LeaderboardName, date);
        }

        public void ExpireLeaderboardFor(string leaderboardName, DateTime date)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.ExpireEntryAt(leaderboardName, date));
                    trans.QueueCommand(r => r.ExpireEntryAt(MemberDataKey(leaderboardName), date));
                    trans.Commit();
                }
            }
        }

        public List<RankInfo> LeadersIn(string leaderboardName, int currentPage, LeaderboardRequestOptions options)
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

            List<string> rawLeaderData;
            var endingOffset = (startingOffset + pageSize) - 1;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = redis.GetRangeFromSortedSet(leaderboardName, startingOffset, endingOffset);
                }
                else
                {
                    rawLeaderData = redis.GetRangeFromSortedSetDesc(leaderboardName, startingOffset, endingOffset);
                }
            }
            if (rawLeaderData != null)
            {
                return RankedInListIn(leaderboardName, rawLeaderData, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }

        public List<RankInfo> AllLeadersFrom(string leaderboardName, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            List<string> rawLeaderData = null;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = redis.GetRangeFromSortedSet(leaderboardName, 0, -1);
                }
                else
                {
                    rawLeaderData = redis.GetRangeFromSortedSetDesc(leaderboardName, 0, -1);
                }
            }
            if (rawLeaderData != null)
            {
                return RankedInListIn(leaderboardName, rawLeaderData, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }


        public List<RankInfo> MembersFromScoreRange(double minScore, double maxScore, LeaderboardRequestOptions options)
        {
            return MembersFromScoreRangeIn(this.LeaderboardName, minScore, maxScore, options);
        }

        public List<RankInfo> MembersFromScoreRangeIn(string leaderboardName, double minScore, double maxScore, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            List<string> rawLeaderData;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = redis.GetRangeFromSortedSetByLowestScore(leaderboardName, minScore, maxScore);

                }
                else
                {
                    rawLeaderData = redis.GetRangeFromSortedSetByHighestScore(leaderboardName, maxScore, minScore);
                }
            }
            if (rawLeaderData != null)
            {
                return RankedInListIn(leaderboardName, rawLeaderData, options);
            }
            else
            {
                return new List<RankInfo>();
            }
        }

        public List<RankInfo> MembersFromRankRange(double minScore, int startRanking, int endRanking, LeaderboardRequestOptions options)
        {
            return MembersFromRankRangeIn(this.LeaderboardName, startRanking, endRanking, options);
        }

        public List<RankInfo> MembersFromRankRangeIn(string leaderboardName, int startRanking, int endRanking, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            startRanking -= 1;
            if (startRanking < 0)
                startRanking = 0;

            endRanking -= 1;
            if (endRanking > TotalMembersIn(leaderboardName))
                endRanking = (int)(TotalMembersIn(leaderboardName) - 1);

            List<string> rawLeaderData;
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = redis.GetRangeFromSortedSet(leaderboardName, startRanking, endRanking);

                }
                else
                {
                    rawLeaderData = redis.GetRangeFromSortedSetDesc(leaderboardName, startRanking, endRanking);
                }
            }
            if (rawLeaderData != null)
            {
                return RankedInListIn(leaderboardName, rawLeaderData, leaderboardRequestOptions);
            }
            else
            {
                return new List<RankInfo>();
            }
        }


        public List<RankInfo> Top(int number, LeaderboardRequestOptions options)
        {
            return TopIn(this.LeaderboardName, number, options);
        }

        public List<RankInfo> TopIn(string leaderboardName, int number, LeaderboardRequestOptions options)
        {
            return MembersFromRankRangeIn(leaderboardName, 1, number, options);
        }

        public RankInfo MemberAt(int position, LeaderboardRequestOptions options)
        {
            return MemberAtIn(this.LeaderboardName, position, options);
        }

        public RankInfo MemberAtIn(string leaderbaordName, int position, LeaderboardRequestOptions options)
        {
            if (position <= TotalMembersIn(leaderbaordName))
            {
                var leaderboardRequestOptions = new LeaderboardRequestOptions();
                leaderboardRequestOptions.Merge(options);
                var pageSize = ValidatePageSize(leaderboardRequestOptions.PageSize);
                var currentPage = (int)Math.Ceiling((decimal)(position / pageSize));
                var offset = (position - 1) % pageSize;
                var leaders = LeadersIn(leaderbaordName, currentPage, leaderboardRequestOptions);
                if (leaders != null && leaders.Count > 0)
                {
                    return leaders[offset];
                }
            }
            return null;
        }


        public List<RankInfo> AroundMe(string member, LeaderboardRequestOptions options)
        {
            return AroundMeIn(this.LeaderboardName, member, options);
        }


        public List<RankInfo> AroundMeIn(string leaderboardName, string member, LeaderboardRequestOptions options)
        {
            var leaderboardRequestOptions = new LeaderboardRequestOptions();
            leaderboardRequestOptions.Merge(options);
            long reverseRankForMe;

            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                if (this.LeaderBoardOptions.Reverse)
                {
                    reverseRankForMe = redis.GetItemIndexInSortedSetDesc(leaderboardName, member);
                }
                else
                {
                    reverseRankForMe = redis.GetItemIndexInSortedSet(leaderboardName, member);
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

                List<string> rawLeaderData = null;
                if (this.LeaderBoardOptions.Reverse)
                {
                    rawLeaderData = redis.GetRangeFromSortedSet(leaderboardName, (int)startingOffset, (int)endOffset);

                }
                else
                {
                    rawLeaderData = redis.GetRangeFromSortedSetDesc(leaderboardName, (int)startingOffset, (int)endOffset);
                }
                if (rawLeaderData != null)
                {
                    return RankedInListIn(leaderboardName, rawLeaderData, leaderboardRequestOptions);
                }
                else
                {
                    return new List<RankInfo>();
                }
            }
        }

        //Fix me: implement the aggregate operation. options = {:aggregate => :sum})
        public void MergeLeaderboards(string destination, List<string> keys)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                redis.StoreUnionFromSortedSets(destination, keys.ToArray(), new string[] { });
            }
        }

        //Fix me: implement the aggregate operation. options = {:aggregate => :sum})
        public void IntersetLeaderboard(string destination, List<string> keys)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                redis.StoreIntersectFromSortedSets(destination, keys.ToArray(), new string[] { });
            }
        }

        List<RankInfo> RankedInListIn(string leaderboardName, List<string> members, LeaderboardRequestOptions options)
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
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    members.ForEach(member =>
                    {
                        if (this.LeaderBoardOptions.Reverse)
                        {
                            trans.QueueCommand(r => r.GetItemIndexInSortedSet(leaderboardName, member), i => ranks.Add(i));
                        }
                        else
                        {
                            trans.QueueCommand(r => r.GetItemIndexInSortedSetDesc(leaderboardName, member), i => ranks.Add(i));
                        }
                        trans.QueueCommand(r => r.GetItemScoreInSortedSet(leaderboardName, member), s => scores.Add(s));
                    });
                    trans.Commit();
                }
            }

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
                    data.MemberData = MemberDataForIn(leaderboardName, members[index]);
                }
                ranksForMembers.Add(data);
            }
            switch (leaderboardRequestOptions.SortBy.ToLower())
            {
                case "rank":
                    ranksForMembers.Sort(delegate (RankInfo x, RankInfo y) {
                        if (y.Position == null) return -1;
                        if (x.Position == null) return 1;
                        else
                        {
                            return (int)(x.Position - y.Position);
                        }
                    });
                    break;
                case "score":
                    ranksForMembers.Sort(delegate (RankInfo x, RankInfo y) {
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
        public void DeleteLeaderboard(string leaderBoardName)
        {
            using (IRedisClient redis = provider.RedisClientManager.GetClient())
            {
                using (var trans = redis.CreateTransaction())
                {
                    trans.QueueCommand(r => r.Remove(leaderBoardName));
                    trans.QueueCommand(r => r.Remove(MemberDataKey(leaderBoardName)));
                    trans.Commit();
                }
            }
        }



        /// <summary>
        /// disconnect the redis connection
        /// </summary>
        public void DisConnect()
        {
            this.provider.RedisClientManager.GetClient().Shutdown();
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
    }
}
