using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication2
{
    class Program
    {
        static void Main(string[] args)
        {
            var data = File.ReadAllLines(@"D:\百度云同步盘\Research\my work\IPE_Inference\context_triggering\ServiceRatio.ss.csv")
                .Select(dp => dp.Split(','));

            var totalCount = data.Select(dp => int.Parse(dp[1])).Sum();

            File.WriteAllLines(@"D:\百度云同步盘\Research\my work\IPE_Inference\context_triggering\ServiceRatio_Final.ss.csv", 
                data.Select(dp => Tuple.Create(dp[0], double.Parse(dp[1]) / totalCount))
                    .OrderByDescending(dp => dp.Item2)
                    .Select(dp => dp.Item1 + "," + dp.Item2)
                );
           File.WriteAllText(@"D:\百度云同步盘\Research\my work\IPE_Inference\context_triggering\ServiceTotalCount.ss.csv",      
               totalCount.ToString());
        }
    }
}
