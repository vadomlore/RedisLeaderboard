using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VirtuosPlayFab
{
    /// <summary>
    /// A special void 'Done' Task that is already in the RunToCompletion state.
    /// Equivalent to Task.FromResult(1).
    /// </summary>
    public static class TaskDone
    {
        private static readonly Task<int> doneConstant = Task.FromResult(1);

        /// <summary>
        /// A special 'Done' Task that is already in the RunToCompletion state
        /// </summary>
        public static Task Done
        {
            get
            {
                return doneConstant;
            }
        }
    }
}
