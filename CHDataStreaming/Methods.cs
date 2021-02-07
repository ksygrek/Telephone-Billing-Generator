using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CHDataStreaming
{
    class Methods
    {
        public static Random rand = new Random();
        private static int activeCalls = 0, savedCalls = 0, time = 0;
        private static List<object> numbersInUsage = new List<object>(), numberList = new List<object>(), calls = new List<object>();
        private static List<int> t1 = new List<int> { 1, 12, 123, 1234, 41, 341, 412 }, 
            t2 = new List<int> { 2, 23, 234, 1234, 12, 123, 412 },
            t3 = new List<int> { 3, 34, 341, 1234, 23, 234, 123 }, 
            t4 = new List<int> { 4, 41, 412, 1234, 34, 234, 341 };

public static void GetNumbers(string command)
        {
            using (ClickHouseCommand comm = DBConnention.con.CreateCommand(command))
            {
                using (var reader = comm.ExecuteReader())
                {
                    do
                    {
                        while (reader.Read())
                        {
                            object[] obj = new object[] { 0, 0, 0, 0, 0, 0};
                            for (var i = 0; i < reader.FieldCount - 1; i++)
                            {
                                var val = reader.GetValue(i);
                                obj[i] = val;
                            }
                            numberList.Add(obj);
                        }
                    } while (reader.NextResult());
                }
            }
        }
        public static void Exec()
        {
            

            int chanceMin = rand.Next(15, 100);
            foreach (object[] o in numberList)
            {
                if ((int)o[4] >= chanceMin && (int)o[2] != 0)
                    NewCall((int)o[0], (int)o[1], (int)o[2]);

                else if ((int)o[4] < chanceMin && (int)o[2] != 0)
                {
                    switch (SQLQuest.cas)
                    {
                        case 1:
                            if (t1.Contains((int)o[5]))
                                NewCall((int)o[1], (int)o[0], (int)o[2]);
                            break;
                        case 2:
                            if (t2.Contains((int)o[5]))
                                NewCall((int)o[1], (int)o[0], (int)o[2]);
                            break;
                        case 3:
                            if (t3.Contains((int)o[5]))
                                NewCall((int)o[1], (int)o[0], (int)o[2]);
                            break;
                        case 4:
                            if (t4.Contains((int)o[5]))
                                NewCall((int)o[1], (int)o[0], (int)o[2]);
                            break;
                    }
                }
                else if ((int)o[2] == 0)
                    NewCall((int)o[0], (int)o[1], (int)o[2]);
            }
        }

        private static void NewCall(int number, int contactNumber, int averageCallTime)
        {
            CallObj data = new CallObj()
            {
                ContactNumber = contactNumber,
                PhoneNumber = number,
            };
            if (averageCallTime == 0)
                averageCallTime = rand.Next(60000, 1800000);

            System.Timers.Timer timer = new System.Timers.Timer();
            int sleep = rand.Next(1, 60000 - 2);
            timer.Interval = sleep;
            timer.Elapsed += new System.Timers.ElapsedEventHandler((s, e) => Tick(timer, data, averageCallTime));
            timer.Start();
        }

        private static void Tick(System.Timers.Timer timer, CallObj data, int aveTime)
        {
            timer.Stop();
            data.StartTime = DateTime.Now;
            activeCalls++;
            try
            {
                numbersInUsage.Add(data.PhoneNumber);
            } catch { }
            timer = new System.Timers.Timer();
            int phoneCallTime = rand.Next((aveTime * 60 / 100), (aveTime * 120 / 100));
            if (phoneCallTime < 0)
                phoneCallTime = 100;
            timer.Interval = phoneCallTime;
            timer.Elapsed += new System.Timers.ElapsedEventHandler((s, e) => TickStop(timer, data));
            timer.Start();
        }

        private static void TickStop(System.Timers.Timer timer, CallObj data)
        {
            timer.Stop();
            data.EndTime = DateTime.Now;
            activeCalls--;
            numbersInUsage.Remove(data.PhoneNumber);
            object[] obj = new object[] { data.PhoneNumber, data.ContactNumber, 
                data.StartTime, data.EndTime };
            calls.Add(obj);
        }

        public static void InsertValues()
        {
            ClickHouseCommand comm = DBConnention.con.CreateCommand();
            comm.CommandText = "INSERT INTO calls " +
                "(phone_number, contact_number, start, end) VALUES @bulk";
            comm.Parameters.Add(new ClickHouseParameter
            {
                ParameterName = "bulk",
                Value = calls
            });
            try
            {
                comm.ExecuteNonQuery();
                savedCalls = savedCalls + calls.Count;
            }
            catch { }
            calls.Clear();
        }

        public static void ClearNumberList()
        {
            numberList.Clear();
        }

        public static void FindDuplicateCallsInUsage()
        {
            List<object> list = new List<object>();
            foreach (object[] o in numberList)
            {
                list.Add((int)o[0]);
            }
            try
            {
                var duplicates = list.Intersect(numbersInUsage).ToList();
                if (duplicates != null)
                    RemoveDuplicateCalls(duplicates, list);
            }
            catch { }
            
        }

        public static void FindDuplicatesInSelect()
        {
            List<object> list = new List<object>(), list2 = new List<object>();
            foreach (object[] o in numberList)
            {
                list.Add((int)o[0]);
                list2.Add((int)o[1]);
            }
            try
            {
                var duplicates = list.Intersect(list2).ToList();
                if (duplicates != null)
                    RemoveDuplicateCalls(duplicates, list);
            }
            catch { }
        }

        public static void RemoveDuplicateCalls(List<object> duplicates, List<object> list)
        {
            int index;
            for (int i = 0; i < duplicates.Count; i++)
            {
                index = list.IndexOf(duplicates[i]);
                list.RemoveAt(index);
                numberList.RemoveAt(index);
            }
        }

        public static void TimerCallback(Object o)
        {
            time = time + 2;
            Console.Clear();
            Console.WriteLine("              Streaming...");
            Console.WriteLine("_________________________________________________");
            Console.WriteLine($"     Active calls | { activeCalls }");
            Console.WriteLine($"      Saved calls | { savedCalls }");
            Console.WriteLine($"             Time | { time } s");
            GC.Collect();
        }

        public static void Backup()
        {
            string command = "SELECT * FROM default.calls", 
                path = $@"D:\{ DateTime.Now.Date.ToString("d") }-backup.txt";

            using (ClickHouseCommand comm = DBConnention.con.CreateCommand(command))
            {
                using (var reader = comm.ExecuteReader())
                {
                    do
                    {
                        using (StreamWriter file = new StreamWriter(path, true))
                        {
                            while (reader.Read())
                            {
                                string line = "";
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    var val = reader.GetValue(i);
                                    line += $"{ val },";
                                }
                                line = line.Substring(0, line.Length - 1);
                                file.WriteLine(line);
                            }
                        }
                    } while (reader.NextResult());
                }
            }
        }

        public static void ClearCallsTable()
        {
            string command = "TRUNCATE TABLE calls";

            ClickHouseCommand comm = DBConnention.con.CreateCommand(command);
            comm.ExecuteNonQuery();
        }
    }
}

