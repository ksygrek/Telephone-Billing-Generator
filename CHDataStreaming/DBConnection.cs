using ClickHouse.Ado;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CHDataStreaming
{
    class DBConnention
    {
        public static ClickHouseConnection con = null;
        public static String str = "Host=127.0.0.1;" +
            "Port=9000;" +
            "User=default;" +
            "Password=;" +
            "Database=default;" +
            "Compress=True;" +
            "CheckCompressedHash=False;" +
            "SocketTimeout=60000000;" +
            "Compressor=lz4";

        private static Random rand = new Random();
        private static int min = 500000000, max = 980000000, numbers;
        private static List<int> relations = new List<int>();
        private static int[] numEachModel;



        public static void CHConnection(Action myMethodName)
        {
            try
            {
                con = new ClickHouseConnection(str);
                con.Open();

                myMethodName();
            }
            catch (ClickHouseException err)
            {
                Console.WriteLine(err);
            }
            finally
            {
                if (con != null)
                {
                    con.Close();
                }
            }
        }

        public static void CreateTables()
        {
            List<String> commands = new List<string>();
            commands.Add("DROP TABLE IF EXISTS default.phone_numbers;");
            commands.Add("DROP TABLE IF EXISTS default.calls;");
            commands.Add("DROP TABLE IF EXISTS default.relation;");
            commands.Add("CREATE TABLE phone_numbers (" +
                "id Int32, " +
                "ph_num Int32, " +
                "mon Int32, " +
                "tue Int32, " +
                "wed Int32, " +
                "thu Int32, " +
                "fri Int32, " +
                "sat Int32, " +
                "sun Int32, " +
                "typ Int32, " +
                "week Int32, " +
                "weekend Int32" +
                ") ENGINE = MergeTree () " +
                "ORDER BY (id)");
            commands.Add("CREATE TABLE calls (" +
                "phone_number Int32, " +
                "contact_number Int32, " +
                "start DateTime, " +
                "end DateTime " +
                ") ENGINE = MergeTree () " +
                "ORDER BY (phone_number)");
            commands.Add("CREATE TABLE relation (" +
                "id_num Int32, " +
                "id_con Int32, " +
                "average_time Int32" +
                ") ENGINE = MergeTree() " +
                "ORDER BY id_num");

            for (int i = 0; i < commands.Count; i++)
            {
                ClickHouseCommand comm = con.CreateCommand(commands[i]);
                comm.ExecuteNonQuery();
            }
            FillDB();
            Console.WriteLine("Streaming will start automatically.");
            BulkToCH();
        }
        public static void FillDB()
        {
            Console.Write("Enter how many numbers you want to use: ");
            int limit = 300000;
            string readL = Console.ReadLine();
            if (!Int32.TryParse(readL, out numbers))
            {
                Console.WriteLine("Incorrect input.");
                FillDB();
            }
            else if (numbers > limit)
            {
                Console.WriteLine($"Too much numbers, limit is { limit }.");
                FillDB();
            }
        }

        private static List<object> ReadModels()
        {
            List<object> result = new List<object>();
            const Int32 BufferSize = 128;
            string filePath = "Models.txt";
            using (var fileStream = File.OpenRead(filePath))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
            {
                String line;
                while ((line = streamReader.ReadLine()) != null)
                {
                    char[] spearator = { ',' };
                    string[] strlist = line.Split(spearator, StringSplitOptions.None);
                    object[] x = new object[11];
                    for (int i = 1; i < 12; i++)
                    {
                        x[i - 1] = strlist[i];
                        int o = 0;
                        if (int.TryParse(strlist[i], out o))
                        {
                            x[i - 1] = o;
                        }
                    }
                    result.Add(x);
                }
            }
            return result;
        }

        public static void BulkToCH()
        {
            int k = numbers, n = 0, x = 3000;
            BulkNumbersToCH(numbers);
            Console.WriteLine("Creating DB...");
            using (var progress = new ProgressBar())
            {
                for (int i = numbers; i > 0; i = i - x)
                {
                    progress.Report((double)(k - numbers) / k);
                    if (numbers <= x)
                    {
                        x = numbers;
                        CreateRelations(x, n, relations);
                        numbers = 0;
                    }
                    else
                    {
                        CreateRelations(x, n, relations);
                        n = n + x;
                        numbers = numbers - x;
                        k++;

                    }
                }
            }
            Console.WriteLine("Done.");
        }

        public object Clone()
        {
            return this.MemberwiseClone();
        }

        public static void BulkNumbersToCH(int num)
        {
            ClickHouseCommand comm = con.CreateCommand();


            List<object> models = ReadModels();
            List<object> numbers = new List<object>();

            numEachModel = SetModelNum(num, models);

            for (int i = 0; i < num; i++)
            {
                for(int l = 0; l < numEachModel.Length; l++)
                {
                    for (int j = 0; j < numEachModel[l]; j++)
                    {
                        var li = (object[])models[l + 1];
                        object[] clone = (object[])li.Clone();
                        for (int k = 0; k < li.Length - 2; k++)
                        {
                            int x = (int)clone[k];
                            if (k != 9 && x != 100)
                                x = rand.Next((x * 80 / 100), (x * 120 / 100));
                            if (x >= 100 && k != 0)
                                x = 100;
                            else if (x <= 0 && k != 0)
                                x = 0;
                            clone[k] = x;
                        }
                        relations.Add((int)clone[0]);
                        object obj = new object[] { i, rand.Next(min, max),
                    clone[1], clone[2], clone[3], clone[4], clone[5], clone[6], clone[7], clone[8], clone[9], clone[10]};
                        numbers.Add(obj);
                        i++;
                    }
                }
                
            }
            comm.CommandText = "INSERT INTO phone_numbers " +
                "( id , ph_num , mon , tue , wed , thu , fri , " +
                "sat , sun , typ, week , weekend ) VALUES @bulk";
            comm.Parameters.Add(new ClickHouseParameter
            {
                ParameterName = "bulk",
                Value = numbers
            });
            comm.ExecuteNonQuery();
        }

        private static void CreateRelations(int num, int n, List<int> rel)
        {
            List<object> data = new List<object>();
            for (int j = n; j < num + n; j++)
            {
                int sum = numEachModel[0];
                int k = 0;

                if (j >= sum)
                {
                    do
                    {
                        k++;
                        if (k < numEachModel.Length || sum != num)
                            sum = sum + numEachModel[k];
                    } while (j > sum);
                    
                }
                int numberOfContacts = rel[j];
                if (numberOfContacts > num)
                    numberOfContacts = rand.Next(num * 5 / 10, num);
                else if(numberOfContacts == 0)
                {
                    List<object> dataA = new List<object>();
                    for (int x = 0; x < num; x++)
                    {
                        
                        object[] obj = new object[] { j, x, 0 };
                        dataA.Add(obj);
                        if(x == 10000)
                        {
                            BulkRelations(dataA);
                            dataA = new List<object>();
                        }
                    }
                    if(dataA.Count > 0)
                        BulkRelations(dataA);
                }

                for (int i = 0; i < numberOfContacts; i++)
                {
                    if (i < (numberOfContacts * 7 / 10))
                    {
                        object[] obj = new object[] { j, rand.Next(sum - numEachModel[k], sum), rand.Next(60000, 1800000) };
                        data.Add(obj);
                    }
                    else
                    {
                        object[] obj = new object[] { j, rand.Next(0, num), rand.Next(60000, 1800000) };
                        data.Add(obj);
                    }
                }
            }
            BulkRelations(data);
        }

        private static void BulkRelations(List<object> data)
        {
            ClickHouseCommand comm = con.CreateCommand();
            comm.CommandText = "INSERT INTO relation (id_num, id_con, average_time) values @bulk";
            comm.Parameters.Add(new ClickHouseParameter
            {
                ParameterName = "bulk",
                Value = data
            });
            comm.ExecuteNonQuery();
        }

        private static int[] SetModelNum(int num, List<object> models)
        {
            int[] numEachMod = new int[models.Count() - 1];
            for (int i = 1; i <= numEachMod.Length; i++)
            {
                var o = (object[])models[i];
                --i;
                int w = 0;

                for(int j = 1; j < o.Length - 3; j++)
                {
                    if ((int)o[j] == 0)
                        w++;
                }
                if ((int)o[8] == 100 && (int)o[10] == 0)
                    numEachMod[i] = num / rand.Next(200, 400);
                else if ((int)o[10] == 0)
                    numEachMod[i] = num / 30;
                else if (((int)o[9] == 1 && (int)o[10] == 41) || ((int)o[9] == 41 && (int)o[10] == 1) || ((int)o[9] == 1 && (int)o[10] == 1) || ((int)o[9] == 41 && (int)o[10] == 41))
                    numEachMod[i] = num / rand.Next(80, 120);
                else if (((int)o[9] == 41 && (int)o[10] == 4) || ((int)o[9] == 4 && (int)o[10] == 41))
                    numEachMod[i] = num / rand.Next(20, 60);
                else if (((int)o[9] == 234 || (int)o[9] == 341) && (int)o[10] >= 234 && ((int)o[5] + (int)o[6]) > 140)
                    numEachMod[i] = num / rand.Next(10, 20);
                else if (w >= 4)
                    numEachMod[i] = num / rand.Next(50, 100);
                ++i;
            }

            int n = 0, restNum = num - numEachMod.Sum();
            for (int i = 0; i < numEachMod.Length; i++)
            {
                if (numEachMod[i] == 0)
                    n++;
            }

            n = restNum / n;
            for (int i = 0; i < numEachMod.Length; i++)
            {
                if (numEachMod[i] == 0)
                    numEachMod[i] = n;
            }

            int sum = numEachMod.Sum();
            if (sum < num)
                numEachMod[0] = numEachMod[0] + num - sum;

            return numEachMod;
        }
    }
}
