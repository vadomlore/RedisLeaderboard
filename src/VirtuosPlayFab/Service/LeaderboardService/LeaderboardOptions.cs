namespace VirtuosPlayFab.Service.LeaderboardService
{ 
    public class LeaderboardOptions
    {
        public int PageSize { get; set; } = 10;
        public bool Reverse { get; set; } = false;
        public string MemberKey { get; set; }
        public string MemberDataNamespace { get; set; } = "member_data";
        public bool GlobalMemberData { get; set; } = false;

        public LeaderboardOptions()
        {

        }
        public LeaderboardOptions(int? pageSize, bool? reverse, bool? globalMemberData, string memberDataNamespace = null)
        {
            if (pageSize.HasValue && pageSize.Value >= 1)
            {
                this.PageSize = pageSize.Value;
            }
            if (reverse.HasValue)
            {
                this.Reverse = reverse.Value;
            }
            if (memberDataNamespace != null)
            {
                this.MemberDataNamespace = memberDataNamespace;
            }
            if (globalMemberData.HasValue)
            {
                this.GlobalMemberData = globalMemberData.Value;
            }
        }
        public void Merge(LeaderboardOptions leaderboardOptions)
        {
            if (leaderboardOptions.PageSize != this.PageSize)
            {
                this.PageSize = leaderboardOptions.PageSize;
            }
            if (leaderboardOptions.Reverse != this.Reverse)
            {
                this.Reverse = leaderboardOptions.Reverse;
            }
            if (!string.IsNullOrEmpty(leaderboardOptions.MemberDataNamespace) && leaderboardOptions.MemberDataNamespace != this.MemberDataNamespace)
            {
                this.MemberDataNamespace = leaderboardOptions.MemberDataNamespace;
            }
            if (leaderboardOptions.GlobalMemberData != this.GlobalMemberData)
            {
                this.GlobalMemberData = leaderboardOptions.GlobalMemberData;
            }
            if (leaderboardOptions.MemberKey != null)
            {
                this.MemberKey = leaderboardOptions.MemberKey;
            }
        }
    }
}
