using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLeaderboard
{
    /// <summary>
    /// Default options when requesting data from a leaderboard.
    ///with_member_data+ false: Return member data along with the member names.
    ///page_size+ nil: The default page size will be used.
    ///members_only+ false: Only return the member name, not their score and rank.
    ///sort_by+ :none: The default sort for a call to `ranked_in_list`.
    /// </summary>
    public class LeaderboardRequestOptions
    {
        public bool WithDataMember { get; set; } = false;
        public int PageSize { get; set; } = DefaultSettings.DEFAULT_PAGE_SIZE;
        public bool MembersOnly { get; set; } = false;
        public string SortBy { get; set; } = "rank";
        public bool IncludeMissing { get; set; } = true;


        public void Merge(LeaderboardRequestOptions options)
        {
            if(options != null)
            {
                this.WithDataMember = options.WithDataMember;
                this.PageSize = options.PageSize;
                this.MembersOnly = options.MembersOnly;
                this.SortBy = options.SortBy;
                this.IncludeMissing = options.IncludeMissing;
            }
        }
    }
}
