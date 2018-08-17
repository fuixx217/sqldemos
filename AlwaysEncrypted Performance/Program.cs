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
        private const Int32 DefaultExecutionCount = 10000;
        private const Int32 DefaultThreadCount = 4;
        private const Int32 DefaultBatchSize = 1000;

        private const string EncryptedTableName = "[dbo].[Patients_Encrypted]";
        private const string UnencryptedTableName = "[dbo].[Patients_Unencrypted]";

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
            public readonly UInt32 BatchSize;

            public TestParameters(string serverInstance, string database, UInt32 executionCount, UInt32 threadCount, UInt32 batchSize)
            {
                ServerInstance = serverInstance;
                Database = database;
                ExecutionCount = executionCount;
                ThreadCount = threadCount;
                BatchSize = batchSize;
            }
        }

        private enum EncryptOptions { None = 0, ConnectionStringOnly = 1, EncryptedValues = 2 }

        private static TestParameters ParseArguments(string[] args)
        {
            List<string> argslist = new List<string>(args);
            //TestParameters results = new TestParameters();
            TestParameters results;
            string serverinstance, database;
            UInt32 executionCount, threadCount, batchSize;

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
                if (false == UInt32.TryParse(argslist[0], out executionCount))
                {
                    throw new System.ArgumentException("ExecutionCount could not be parsed", "ExecutionCount");
                }
                argslist.RemoveAt(0);

                if (executionCount == 0)
                {
                    throw new System.ArgumentException("ExecutionCount cannot be zero", "executioncount");
                }

                if (executionCount < 0)
                {
                    throw new System.ArgumentException("ExecutionCount cannot be negative", "executioncount");
                }

            }
            else
            {
                executionCount = DefaultExecutionCount;
            }

            if (argslist.Count > 0)
            {
                if (false == UInt32.TryParse(argslist[0], out threadCount))
                {
                    throw new System.ArgumentException("threadcount could not be parsed", "threadcount");
                }
                argslist.RemoveAt(0);

                if (threadCount == 0)
                {
                    throw new System.ArgumentException("threadcount cannot be zero", "threadcount");
                }

                if (threadCount < 0)
                {
                    throw new System.ArgumentException("threadcount cannot be negative", "threadcount");
                }

                //TODO: enforce a maximum threadcount, possibly compare to max connection pool size

            }
            else
            {
                threadCount = DefaultThreadCount;
            }

            //FIXME
            batchSize = DefaultBatchSize;

            results = new TestParameters(serverInstance: serverinstance, database: database, executionCount: executionCount, threadCount: threadCount, batchSize: batchSize);

            return results;
        }

        static void Main(string[] args)
        {
            TestParameters testParms = ParseArguments(args);
            //string connectionString = "Data Source=localhost; Initial Catalog=aeperf; Integrated Security=true; Column Encryption Setting=enabled";
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "localhost",
                InitialCatalog = "aeperf",
                IntegratedSecurity = true,
                ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled
            };


            List<Tuple<String, DateTime, DateTime>> testResults = new List<Tuple<String, DateTime, DateTime>>();

            using (SqlConnection masterConnection = new SqlConnection(connStringBuilder.ConnectionString))
            {
                Tuple<DateTime, DateTime> testResult;
                masterConnection.Open();
                PrepareDatabase(masterConnection);
                PreTest(masterConnection);

                // Start multiple test runs, capture duration of each

                testResult = ExecuteTest(testParms, EncryptOptions.EncryptedValues);
                testResults.Add(Tuple.Create("EncryptedValues", testResult.Item1, testResult.Item2));

                testResult = ExecuteTest(testParms, EncryptOptions.ConnectionStringOnly);
                testResults.Add(Tuple.Create("ConnectionStringOnly", testResult.Item1, testResult.Item2));

                testResult = ExecuteTest(testParms, EncryptOptions.None);
                testResults.Add(Tuple.Create("NoEncryption", testResult.Item1, testResult.Item2));

                PostTest(masterConnection);

            }

            foreach (Tuple<String, DateTime, DateTime> testResult in testResults)
            {
                TimeSpan testDuration = testResult.Item3 - testResult.Item2;
                Int32 rowsPerSecond = (int)Math.Floor(testParms.ExecutionCount * testParms.ThreadCount / testDuration.TotalSeconds);
                Console.WriteLine(String.Format(
                    "Test {0} began at {1} and completed at {2}. Duration: {3} minutes, {4} seconds. Rows per second: {5}"
                    , testResult.Item1, testResult.Item2, testResult.Item3, Math.Floor(testDuration.TotalMinutes), testDuration.Seconds, rowsPerSecond
                ));
            }

            //System.Threading.Thread.Sleep(30000);
            Console.WriteLine("Press Enter to close this program.");
            Console.ReadLine();

            Environment.Exit(0);

        }

        static void PrepareDatabase(SqlConnection dbConnection)
        {
            using (SqlCommand cmd = dbConnection.CreateCommand())
            {
                // Verify server supports AlwaysEncrypted
                Int32 serverMajorVersion;
                Int32.TryParse(dbConnection.ServerVersion.Split('.')[0], out serverMajorVersion);

                if (serverMajorVersion < 13)
                    throw new NotSupportedException(String.Format("Specified server's major version does not support AlwaysEncrypted: {0}", serverMajorVersion));


                // Verify keys are setup
                cmd.CommandText = @"
IF EXISTS(
	SELECT 1 
	FROM sys.column_master_keys cmk
		INNER JOIN sys.column_encryption_key_values cekv
			ON cmk.column_master_key_id = cekv.column_master_key_id
		INNER JOIN sys.column_encryption_keys cek
			ON cekv.column_encryption_key_id = cek.column_encryption_key_id
	WHERE
		cmk.name = 'CMK1'
		AND cek.name = 'CEK1'
) SELECT 1 AS [keys_are_setup]
ELSE SELECT 0 AS [keys_are_setup]
";
                int keys_are_setup;

                try
                {
                    keys_are_setup = (Int32)cmd.ExecuteScalar();

                    if (keys_are_setup == 0)
                    {
                        throw new NotImplementedException(String.Format("Generating the Column Master Key and Column Encryption Key is not currently supported."));
                    }
                    else
                    {
                        CreateTables(dbConnection);
                        ResetTables(dbConnection);
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Encountered SQL exception: {0}", ex.ToString());
                    Environment.Exit(1);
                }
            }
        }

        static void PreTest(SqlConnection dbConnection)
        {
            Int32 rowcount;

            using (SqlCommand cmd = dbConnection.CreateCommand())
            {

                try
                {
                    cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM " + EncryptedTableName;
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients_Encrypted table at start: {0}", rowcount));

                    cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM " + UnencryptedTableName;
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients_Unencrypted table at start: {0}", rowcount));

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Encountered SQL exception: {0}", ex.ToString());
                    Environment.Exit(1);
                }
            }
        }

        static Tuple<DateTime, DateTime> ExecuteTest(TestParameters testParms, EncryptOptions encryptionOptions)
        {
            DateTime startTime = DateTime.Now;
            DateTime endTime;

            Task[] taskList = new Task[testParms.ThreadCount];
            for (int i = 0; i < testParms.ThreadCount; i++)
            {
                taskList[i] = Task.Run(() => ExecuteTestTask(testParms, encryptionOptions));
            }

            Task.WaitAll(taskList);

            endTime = DateTime.Now;

            return new Tuple<DateTime, DateTime>(startTime, endTime);
        }

        static void ExecuteTestTask(TestParameters testParms, EncryptOptions encryptionOptions)
        {
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "localhost",
                InitialCatalog = "aeperf",
                IntegratedSecurity = true
            };

            switch (encryptionOptions)
            {
                case EncryptOptions.EncryptedValues:
                case EncryptOptions.ConnectionStringOnly:
                    connStringBuilder.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;
                    break;

                case EncryptOptions.None:
                    connStringBuilder.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Disabled;
                    break;

                default:
                    throw new NotImplementedException(String.Format("Provided EncryptOptions value is not implemeneted: {0}", encryptionOptions));
            }


            connStringBuilder.MinPoolSize = (int)testParms.ThreadCount;
            //connStringBuilder.MaxPoolSize = (int)testParms.ThreadCount;

            using (SqlConnection dbConnection = new SqlConnection(connStringBuilder.ConnectionString))
            {
                dbConnection.Open();

                using (SqlCommand cmd = dbConnection.CreateCommand())
                {
                    switch (encryptionOptions)
                    {
                        case EncryptOptions.EncryptedValues:
                            cmd.CommandText = @"INSERT [dbo].[Patients_Encrypted] (SSN, FirstName, LastName, BirthDate) VALUES (@SSN, @FirstName, @LastName, @BirthDate);";
                            break;

                        case EncryptOptions.ConnectionStringOnly:
                        case EncryptOptions.None:
                            cmd.CommandText = @"INSERT [dbo].[Patients_Unencrypted] (SSN, FirstName, LastName, BirthDate) VALUES (@SSN, @FirstName, @LastName, @BirthDate);";
                            break;

                        default:
                            throw new NotImplementedException(String.Format("Provided EncryptOptions value is not implemeneted: {0}", encryptionOptions));
                    }
                    

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

                    SqlTransaction transaction = dbConnection.BeginTransaction();
                    cmd.Transaction = transaction;

                    for (int i = 0; i < testParms.ExecutionCount; i++)
                    {
                        if (i % testParms.BatchSize == 0)
                        {
                            transaction.Commit();
                            transaction = dbConnection.BeginTransaction();
                            cmd.Transaction = transaction;
                        }

                        paramSSN.Value = @"123-45-6789";
                        paramFirstName.Value = @"Foo";
                        paramLastName.Value = @"Bar";
                        paramBirthDate.Value = new DateTime(2018, 08, 16);

                        cmd.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
            }
        }

        static void PostTest(SqlConnection dbConnection)
        {
            Int32 rowcount;

            using (SqlCommand cmd = dbConnection.CreateCommand())
            {

                try
                {
                    cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM " + EncryptedTableName;
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients_Encrypted table at end: {0}", rowcount));

                    cmd.CommandText = @"SELECT COUNT(*) AS [RowCount] FROM " + UnencryptedTableName;
                    rowcount = (Int32)cmd.ExecuteScalar();
                    Console.WriteLine(String.Format("Rows in Patients_Unencrypted table at end: {0}", rowcount));
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Encountered SQL exception: {0}", ex.ToString());
                    Environment.Exit(2);
                }
            }
        }

        private static void CreateTables(SqlConnection dbConnection)
        {
            using (SqlCommand cmd = dbConnection.CreateCommand())
            {
                //NOTE: Accepts the default filegroup when creating the table and index
                //NOTE: Does not verify if the existing table has matching schema

                //Create the Encrypted table
                cmd.CommandText = @"
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF (SELECT OBJECT_ID ('[dbo].[Patients_Encrypted]')) IS NULL
BEGIN;
	CREATE TABLE [dbo].[Patients_Encrypted] (
		[PatientId] [int] IDENTITY(1,1) NOT NULL,
		[SSN] [char](11) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [CEK1], ENCRYPTION_TYPE = Deterministic, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
		[FirstName] [nvarchar](50) NULL,
		[LastName] [nvarchar](50) NULL,
		[BirthDate] [date] ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [CEK1], ENCRYPTION_TYPE = Randomized, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
		PRIMARY KEY CLUSTERED 
		(
			[PatientId] ASC
		) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
	)
END;
";

                cmd.ExecuteNonQuery();

                //Create the Unencrypted table
                cmd.CommandText = @"
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF (SELECT OBJECT_ID ('[dbo].[Patients_Unencrypted]')) IS NULL
BEGIN;
	CREATE TABLE [dbo].[Patients_Unencrypted] (
		[PatientId] [int] IDENTITY(1,1) NOT NULL,
		[SSN] [char](11) COLLATE Latin1_General_BIN2 NOT NULL,
		[FirstName] [nvarchar](50) NULL,
		[LastName] [nvarchar](50) NULL,
		[BirthDate] [date] NOT NULL,
		PRIMARY KEY CLUSTERED 
		(
			[PatientId] ASC
		) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
	)
END;
";

                cmd.ExecuteNonQuery();
            }
        }

        private static void ResetTables(SqlConnection dbConnection)
        {
            using (SqlCommand cmd = dbConnection.CreateCommand())
            {
                cmd.CommandText = @"
TRUNCATE TABLE [dbo].[Patients_Unencrypted];
TRUNCATE TABLE [dbo].[Patients_Encrypted];
";
                cmd.ExecuteNonQuery();
            }
        }
    }
}
