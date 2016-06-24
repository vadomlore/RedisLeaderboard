using System;
using System.Collections.Generic;
using ServiceStack.Redis;
using ServiceStack;
using System.Threading;
using RedisLeaderboard;
using System.Diagnostics;
namespace RedisLeaderboard
{
    class Program
    {
        static void Main(string[] args)
        {

            Leaderboard leaderBoard = new Leaderboard("ASpehre", null, null);
            Console.WriteLine("Begin InsertData");
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Restart();
            for (int i = 0; i < 1000; i++)
            {
                leaderBoard.RankMemeber($"beer{i}", 10.5 + i, "Made in Germany");
            }
            stopWatch.Stop();
            Console.WriteLine($"ElapseMillion: {stopWatch.ElapsedMilliseconds}");

            //Console.WriteLine(leaderBoard.MemberDataFor("beer"));
            //Console.WriteLine(leaderBoard.MemberDataFor("beef"));

            Console.WriteLine("Get Top 100");
            stopWatch.Restart();
            var data = leaderBoard.Top(100, new LeaderboardRequestOptions { WithDataMember=true});
            //            .ForEach(e => Console.WriteLine($"{e.Member}{e.MemberData}{e.Position}"));
            stopWatch.Stop();
            Console.WriteLine($"ElapseMillion: {stopWatch.ElapsedMilliseconds}");
            Console.ReadLine();
            //var redisClient = new RedisClient("redis://localhost:6379");
            //redisClient.AddItemToSet("Alibaa", "33");
            //redisClient.SetEntryInHash("Alpha-Go", "an", "45");
            //redisClient.AddItemToSortedSet("sh1", "p1", 55);
            //Console.WriteLine(redisClient.GetItemScoreInSortedSet("sh1", "p1"));
            //AD(e =>
            //{
            //    if(e == "345")
            //    {
            //        return true;
            //    }
            //    else
            //    {
            //        return false;
            //    }
            //});

            //var val = redisClient.GetItemIndexInSortedSet("myscore","MX");
            //Console.WriteLine("OK");
            //using (redisClient.AcquireLock("testlock"))
            //The number of concurrent clients to run
            //const int noOfClients = 64;
            //var asyncResults = new List<IAsyncResult>(noOfClients);
            //for (var i = 1; i <= noOfClients; i++)
            //{
            //    var clientNo = i;
            //    var actionFn = (Action)delegate
            //    {

            //        var redisClient = new RedisClient("redis://localhost:6379");
            //        using (redisClient.AcquireLock("testlock"))
            //        {
            //            Console.WriteLine("client {0} acquired lock", clientNo);
            //            var counter = redisClient.Get<int>("atomic-counter");

            //            //Add an artificial delay to demonstrate locking behaviour
            //            Thread.Sleep(100);

            //            redisClient.Set("atomic-counter", counter + 1);
            //            Console.WriteLine("client {0} released lock", clientNo);
            //        }
            //    };

            //    //Asynchronously invoke the above delegate in a background thread
            //    asyncResults.Add(actionFn.BeginInvoke(null, null));
            //}

            ////Wait at most 1 second for all the threads to complete
            //asyncResults.WaitAll(TimeSpan.FromSeconds(10));

            ////Print out the 'atomic-counter' result
            //using (var redisClient = new RedisClient("redis://localhost:6379"))
            //{
            //    var counter = redisClient.Get<int>("atomic-counter");
            //    Console.WriteLine("atomic-counter after 1sec: {0}", counter);
            //}

        }

        public static void AD(Predicate<string> ore)
        {
            Console.WriteLine(ore);
        }
    }
}
