using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CHDataStreaming
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine("          Telephone billing genereator");
            Console.WriteLine("_________________________________________________");
            DBConnention.CHConnection(() => DBConnention.CreateTables());
            var startTimeSpan = TimeSpan.Zero;
            var periodTimeSpan = TimeSpan.FromMinutes(1);
            var timer = new System.Threading.Timer((e) =>
            {
                Methods.ClearNumberList();
                DBConnention.CHConnection(() => Methods.GetNumbers(SQLQuest.NumberOfCalls()));
                Methods.FindDuplicatesInSelect();
                Methods.FindDuplicateCallsInUsage();
                Methods.Exec();
                DBConnention.CHConnection(() => Methods.InsertValues());
                if (DateTime.Now.ToString("t") == "0:00" || DateTime.Now.ToString("t") == "12:00")
                {
                    DBConnention.CHConnection(() => Methods.Backup());
                    DBConnention.CHConnection(() => Methods.ClearCallsTable());
                }
            }, null, startTimeSpan, periodTimeSpan);
            Timer t = new Timer(Methods.TimerCallback, null, 0, 2000);
            Console.Read();
        }
    }
}