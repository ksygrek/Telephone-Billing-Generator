using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CHDataStreaming
{

    class SQLQuest
    {
        public static int cas;

        public static string NumberOfCalls()
        {
            int n = 100;
            int d = (int)DateTime.Now.DayOfWeek, h = DateTime.Now.Hour, t = 0;
            string countNum = "SELECT COUNT( ) FROM phone_numbers k ";
            string command = "SELECT k.ph_num , k2.ph_num, average_time, k.typ, ", 
                week = "", week2 = "", day;

            switch (d)
            {
                case 0:
                    week = "k.weekend";
                    week2 = "k2.weekend";
                    day = "k.sun";
                    command += $"{ day }, { week2 }, { week } ";
                    n = n * 6 / 10;
                    break;
                case 1:

                    if (h >= 3)
                    {
                        week = "k.week";
                        week2 = "k2.week";
                    }
                    else
                    {
                        week = "k.weekend";
                        week2 = "k2.weekend";
                    }
                    day = "k.mon";
                    command += $"{ day }, { week2 }, { week } ";
                    break;
                case 2:
                    week = "k.week";
                    week2 = "k2.week";
                    day = "k.tue";
                    command += $"{ day }, { week2 }, { week } ";
                    break;
                case 3:
                    week = "k.week";
                    week2 = "k2.week";
                    day = "k.wed";
                    command += $"{ day }, { week2 }, { week } ";
                    break;
                case 4:
                    week = "k.week";
                    week2 = "k2.week";
                    day = "k.thu";
                    command += $"{ day }, { week2 }, { week } ";
                    break;
                case 5:
                    if (h >= 18)
                    {
                        week = "k.weekend";
                        week2 = "k2.weekend";
                    }
                    else
                    {
                        week = "k.week";
                        week2 = "k2.week";
                    }
                    day = "k.fri";
                    command += $"{ day }, { week2 }, { week } ";
                    break;
                case 6:
                    week = "k.weekend";
                    week2 = "k2.weekend";
                    day = "k.sat";
                    command += $"{ day }, { week2 }, { week } ";
                    n = n * 8 / 10;
                    break;           
            }

            command += "FROM phone_numbers k " +
                "JOIN relation r ON k.id = r.id_num " +
                "JOIN phone_numbers k2 ON r.id_con=k2.id ";

            switch (h)
            {
                case 1:
                    n = n * 1 / 50; t = 1; break;
                case 2:
                    n = n * 1 / 50; t = 1; break;
                case 3:
                    n = n * 1 / 50; t = 1; break;
                case 4:
                    n = n * 1 / 50; t = 1; break;
                case 5:
                    n = n * 1 / 30; t = 1; break;
                case 6:
                    n = n * 1 / 10; t = 2; break;
                case 7:
                    n = n * 1 / 5; t = 2; break;
                case 8:
                    n = n * 1 / 2; t = 2; break;
                case 9:
                    n = n * 10 / 13; t = 2; break;
                case 10:
                    n = n * 10 / 11; t = 2; break;
                case 11:
                    n = n * 10 / 10; t = 2; break;
                case 12:
                    n = n * 10 / 11; t = 3; break;
                case 13:
                    n = n * 10 / 12; t = 3; break;
                case 14:
                    n = n * 10 / 12; t = 3; break;
                case 15:
                    n = n * 10 / 13; t = 3; break;
                case 16:
                    n = n * 10 / 14; t = 3; break;
                case 17:
                    n = n * 10 / 15; t = 3; break;
                case 18:
                    n = n * 10 / 16; t = 4; break;
                case 19:
                    n = n * 1 / 2; t = 4; break;
                case 20:
                    n = n * 1 / 3; t = 4; break;
                case 21:
                    n = n * 1 / 5; t = 4; break;
                case 22:
                    n = n * 1 / 10; t = 4; break;
                case 23:
                    n = n * 1 / 20; t = 4; break;
                case 0:
                    n = n * 1 / 30; t = 1; break;
            }

            string s = "";

            switch (t)
            {
                case 1:
                    s = $"WHERE { week } in (1,12,123,1234,41,341,412) ";
                    command += s;
                    countNum += s;
                    break;
                case 2:
                    s = $"WHERE { week } in (2,23,234,1234,12,123,412) ";
                    command += s;
                    countNum += s;
                    break;
                case 3:
                    s = $"WHERE { week } in (3,34,341,1234,23,234,123) ";
                    command += s;
                    countNum += s;
                    break;
                case 4:
                    s = $"WHERE { week } in (4,41,412,1234,34,234,341) ";
                    command += s;
                    countNum += s;
                    break;
            }
            cas = t;

            int k = CountNum(countNum);
            n = k * n / ( k * 8 / 5);
            if (n <= 2)
                n = Methods.rand.Next(0, 5);
            else
                n = Methods.rand.Next(n * 4 / 10, n * 12 / 10);

            command += "ORDER BY RAND() " +
                "LIMIT 1 by k.ph_num " +
                $"LIMIT { n }";

            return command;
        }

        private static int CountNum(string command)
        {
            int num = 0;
            using (ClickHouseCommand comm = DBConnention.con.CreateCommand(command))
            {
                using (var reader = comm.ExecuteReader())
                {
                    do
                    {
                        while (reader.Read())
                        {
                            num = reader.GetInt32(0);
                        }
                    } while (reader.NextResult());
                }
            }
            return num;
        }
    }
}
