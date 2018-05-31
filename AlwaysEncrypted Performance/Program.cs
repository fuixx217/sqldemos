using System;
using System.Collections.Generic;
#using System.Linq;
#using System.Text;
#using System.Threading.Tasks;
using System.Data.SqlClient;

namespace AlwaysEncrypted_Performance
{
    class Program
    {
        private SqlConnection dbconnection;
        private const Int32 defaultExecutionCount = 1000;
        private const Int32 defaultThreadCount = 1;

        private static void PrintUsageAndExit()
        {
            Console.WriteLine("aeperf ServerInstance Database [ExecutionCount] [ThreadCount]");
            Environment.Exit(0);
        }

        private static Dictionary<string, string> ParseArguments(string[] args)
        {
            List<string> argslist = new List<string>(args);
            Dictionary<string, string> results = new Dictionary<string, string>();
            string serverinstance, database;
            Int32 executioncount, threadcount;

            if (argslist[0] == "help") {
                PrintUsageAndExit();
            }

            if (argslist.Count > 0)
            {
                serverinstance = argslist[0];
                argslist.RemoveAt(0);

                if (serverinstance == "" || serverinstance == null)
                {
                    throw new System.ArgumentNullException("ServerInstance name cannot be null or empty", "ServerInstance");
                }

                if (serverinstance.IndexOf("\\") != serverinstance.LastIndexOf("\\"))
                {
                    throw new System.ArgumentException("ServerInstance cannot contain more than one backslash", "ServerInstance");
                }
            } else {
                throw new System.ArgumentException("No ServerInstance provided", "ServerInstance");
            }


            if (argslist.Count > 0)
            {
                database = argslist[0];
                argslist.RemoveAt(0);

                if (database == "" || database == null)
                {
                    throw new System.ArgumentNullException("Database name cannot be null or empty", "Database");
                }
            } else {
                throw new System.ArgumentException("No database provided", "Database");
            }


            if (argslist.Count > 0)
            {
                if (false == Int32.TryParse(argslist[0], executioncount))
                {
                    throw new System.ArgumentException("ExecutionCount could not be parsed", "ExecutionCount");
                }
                argslist.RemoveAt(0);

                if (executioncount == 0)
                {
                    throw new System.ArgumentException("ExecutionCount cannot be zero", "executioncount");
                }
                
                if (executioncount < 0)
                {
                    throw new System.ArgumentException("ExecutionCount cannot be negative", "executioncount");
                }

            } else {
                executioncount = defaultExecutionCount;
            }

            if (argslist.Count > 0)
            {
                if (false == Int32.TryParse(argslist[0], threadcount))
                {
                    throw new System.ArgumentException("threadcount could not be parsed", "threadcount");
                }
                argslist.RemoveAt(0);

                if (threadcount == 0)
                {
                    throw new System.ArgumentException("threadcount cannot be zero", "threadcount");
                }
                
                if (threadcount < 0)
                {
                    throw new System.ArgumentException("threadcount cannot be negative", "threadcount");
                }

            } else {
                threadcount = defaultThreadCount;
            }

            results.Add("ServerInstance", serverinstance);
            results.Add("Database", database);
            results.Add("ExecutionCount", executioncount); //cast
            results.Add("ThreadCount", threadcount); //cast

            return results;
        }

        static void Main(string[] args)
        {
            Dictionary<string, string> arguments = ParseArguments(args);
            dbconnection = new SqlConnection();
        }
    }
}
