using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace AlwaysEncrypted_Performance
{
    class Program
    {
        private const Int32 defaultExecutionCount = 10;
        private const Int32 defaultThreadCount = 10;

        private static void PrintUsageAndExit()
        {
            Console.WriteLine("aeperf ServerInstance Database [ExecutionCount] [ThreadCount]");
            Environment.Exit(0);
        }

        private struct TestParameters
        {
            public readonly string ServerInstance;
            public readonly string Database;
            public readonly UInt32 ExecutionCount;
            public readonly UInt32 ThreadCount;

            public TestParameters(string serverInstance, string database, UInt32 executionCount, UInt32 threadCount)
            {
                ServerInstance = serverInstance;
                Database = database;
                ExecutionCount = executionCount;
                ThreadCount = threadCount;
            }
        }

        private static TestParameters ParseArguments(string[] args)
        {
            List<string> argslist = new List<string>(args);
            //TestParameters results = new TestParameters();
            TestParameters results;
            string serverinstance, database;
            UInt32 executioncount, threadcount;

            if (argslist.Count == 0 || argslist[0] == "help")
            {
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
            }
            else
            {
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
            }
            else
            {
                throw new System.ArgumentException("No database provided", "Database");
            }


            if (argslist.Count > 0)
            {
                if (false == UInt32.TryParse(argslist[0], out executioncount))
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

            }
            else
            {
                executioncount = defaultExecutionCount;
            }

            if (argslist.Count > 0)
            {
                if (false == UInt32.TryParse(argslist[0], out threadcount))
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

            }
            else
            {
                threadcount = defaultThreadCount;
            }

            results = new TestParameters(serverInstance: serverinstance, database: database, executionCount: executioncount, threadCount: threadcount);

            return results;
        }

        static void Main(string[] args)
        {
            TestParameters testParms = ParseArguments(args);
            //string connectionString = "Data Source=localhost; Initial Catalog=aeperf; Integrated Security=true; Column Encryption Setting=enabled";
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.DataSource = "localhost";
            connStringBuilder.InitialCatalog = "aeperf";
            connStringBuilder.IntegratedSecurity = true;
            connStringBuilder.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;



            using (SqlConnection masterConnection = new SqlConnection(connStringBuilder.ConnectionString))
            {
                masterConnection.Open();
                PrepareDatabase(masterConnection);
                PreTest(masterConnection);

                ExecuteTest(testParms);

                System.Threading.Thread.Sleep(1000);

                PostTest(masterConnection);

            }

            Environment.Exit(0);

        }

        static void PrepareDatabase(SqlConnection dbconnection)
        {

        }

        static void PreTest(SqlConnection dbconnection)
        {
            Int32 rowcount;

            using (SqlCommand cmd = dbconnection.CreateCommand())
            {
                cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM [dbo].[Patients]";

                try
                {
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients table at start: {0}", rowcount));

                    if (rowcount != 0)
                    {
                        cmd.CommandText = @"TRUNCATE TABLE [dbo].[Patients];";
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Encountered SQL exception: {0}", ex.ToString());
                    Environment.Exit(1);
                }
            }
        }

        static void ExecuteTest(TestParameters testParms)
        {
            Task[] taskList = new Task[testParms.ThreadCount];
            for (int i = 0; i < testParms.ThreadCount; i++)
            {
                taskList[i] = Task.Run(() => ExecuteTestTask(testParms));
            }

            Task.WaitAll(taskList);
        }

        static void ExecuteTestTask(TestParameters testParms)
        {
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.DataSource = "localhost";
            connStringBuilder.InitialCatalog = "aeperf";
            connStringBuilder.IntegratedSecurity = true;
            connStringBuilder.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

            connStringBuilder.MinPoolSize = (int)testParms.ThreadCount;
            //connStringBuilder.MaxPoolSize = (int)testParms.ThreadCount;

            using (SqlConnection dbConnection = new SqlConnection(connStringBuilder.ConnectionString))
            {
                dbConnection.Open();

                using (SqlCommand cmd = dbConnection.CreateCommand())
                {
                    cmd.CommandText = @"INSERT dbo.Patients (SSN, FirstName, LastName, BirthDate) VALUES (@SSN, @FirstName, @LastName, @BirthDate);";

                    SqlParameter paramSSN, paramFirstName, paramLastName, paramBirthDate;

                    paramSSN = cmd.CreateParameter();
                    paramSSN.ParameterName = @"@SSN";
                    paramSSN.DbType = DbType.AnsiStringFixedLength;
                    paramSSN.Direction = ParameterDirection.Input;
                    paramSSN.Size = 11;
                    cmd.Parameters.Add(paramSSN);


                    paramFirstName = cmd.CreateParameter();
                    paramFirstName.ParameterName = @"@FirstName";
                    paramFirstName.DbType = DbType.String;
                    paramFirstName.Direction = ParameterDirection.Input;
                    paramFirstName.Size = 50;
                    cmd.Parameters.Add(paramFirstName);

                    paramLastName = cmd.CreateParameter();
                    paramLastName.ParameterName = @"@LastName";
                    paramLastName.DbType = DbType.String;
                    paramLastName.Direction = ParameterDirection.Input;
                    paramLastName.Size = 50;
                    cmd.Parameters.Add(paramLastName);

                    paramBirthDate = cmd.CreateParameter();
                    paramBirthDate.ParameterName = @"@BirthDate";
                    paramBirthDate.SqlDbType = SqlDbType.Date;
                    paramBirthDate.Direction = ParameterDirection.Input;
                    cmd.Parameters.Add(paramBirthDate);

                    for (int i = 0; i < testParms.ExecutionCount; i++)
                    {
                        paramSSN.Value = @"123-45-6789";
                        paramFirstName.Value = @"Foo";
                        paramLastName.Value = @"Bar";
                        paramBirthDate.Value = new DateTime(2008, 08, 16);

                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        static void PostTest(SqlConnection dbconnection)
        {
            Int32 rowcount;

            using (SqlCommand cmd = dbconnection.CreateCommand())
            {
                cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM [dbo].[Patients]";

                try
                {
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients table at end: {0}", rowcount));
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Encountered SQL exception: {0}", ex.ToString());
                    Environment.Exit(1);
                }
            }
        }
    }
}
