using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using Google.Cloud.Firestore;
using System.Data.SqlClient;
using Quartz.Impl;
using Quartz;
using System.Threading;
using dataMigrationService.Controllers;

namespace dataMigrationService.services
{
    public class DataExtractor
    {
        string projectId;
        FirestoreDb fireStoreDb;
        public async System.Threading.Tasks.Task FirebaseSave(String tableName, String id, Dictionary<string, object> data, String companyName)
        {
            //service key
            string filepath = "./fas.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "fleet-administration-system";
            fireStoreDb = FirestoreDb.Create(projectId);
            //................................................
            //saving to db
            await fireStoreDb.Collection(companyName).Document(tableName).Collection("tables").Document(id).SetAsync(data);
        }
        public int FirebaseCheck(String tableName, String companyName)
        {
            //service key
            string filepath = "./fas.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "fleet-administration-system";
            fireStoreDb = FirestoreDb.Create(projectId);
            //................................................
            //saving to db
            int number = fireStoreDb.Collection(companyName).Document(tableName).Collection("tables").GetSnapshotAsync().Result.Documents.Count;
            return number;
        }
        public  Boolean StateCheck( String key)
        {
            //service key
            string filepath = "./test.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "test1-5d360";
            fireStoreDb = FirestoreDb.Create(projectId);
            DocumentReference docRef = fireStoreDb.Collection("states").Document(key);
            var result = docRef.GetSnapshotAsync().Result.GetValue<string>("state");
            Boolean active = false;
            if (result == "active")
            {
                active = true;
            }
            else
            {
                active = false;
            }
            return active;

        }
        public async System.Threading.Tasks.Task writeLog(string log)
        {
            //service key
            DateTime now = DateTime.Now;
            string currentDateTime = now.ToString("yyyy-MM-dd-hh:mm:ss");
            Dictionary<string, object> docData = new Dictionary<string, object> { };
            docData.Add("date", currentDateTime);
            docData.Add("log", log);
            string filepath = "./test.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "test1-5d360";
            fireStoreDb = FirestoreDb.Create(projectId);
            //................................................
            //saving to db
            await fireStoreDb.Collection("logs").Document(currentDateTime).SetAsync(docData);
        }
        public void getData(string connectionString, string companyName)
        {
            DataTable myTable = new DataTable("tables");
            //var connectionString = "Data Source=ROB-PC\\SQLEXPRESS;Database=AKHATECH_FLEET;Integrated Security=false;User ID=FasTestDbUser1;Password=qwerty";
            var sqlConnection = new System.Data.SqlClient.SqlConnection(connectionString);

            sqlConnection.Open();

            DataTable dt = sqlConnection.GetSchema("Tables");
            int i = 0;
            foreach (DataRow row in dt.Rows)
            {

                string tablename = (string)row[2];
                var s = row.Table.Rows;
                System.Diagnostics.Debug.WriteLine("..........................................................");
                writeLog("this is the table name").Wait();
                System.Diagnostics.Debug.WriteLine("this is the table name");
                System.Diagnostics.Debug.WriteLine(tablename);
                writeLog(tablename).Wait();


                string oString = "Select * from " + tablename;

                SqlDataAdapter sda = new SqlDataAdapter(oString, sqlConnection);

                var rows = row.Table.Rows;
                DataTable dt1 = new DataTable();
                sda.Fill(dt1);

                DataRowCollection realRows = dt1.Rows;
                int a = 0;
                int firebaseCount = FirebaseCheck(tablename, companyName);
                System.Diagnostics.Debug.WriteLine("started table " + tablename);
                System.Diagnostics.Debug.WriteLine("IN FIREBASE: " + firebaseCount + " ACTUAL : " + realRows.Count);
                writeLog("started table " + tablename).Wait();
                writeLog("IN FIREBASE: " + firebaseCount + " ACTUAL : " + realRows.Count).Wait();
                if (realRows.Count <= firebaseCount)
                {
                    System.Diagnostics.Debug.WriteLine("skipped");
                    writeLog("skipped").Wait();
                    continue;
                }
                var tasks = new List<Task>();
                foreach (DataRow realRow in realRows)
                {

                    int count = 0;
                    Dictionary<string, object> docData = new Dictionary<string, object> { };

                    realRow.ItemArray.ToList().ForEach(col =>
                    {
                        var property = realRow.Table.Columns[count].ToString();
                        var value = col;
                        if (value != null)
                        {
                            docData.Add(property, value.ToString());
                        }
                        else
                        {
                            docData.Add(property, null);
                        }
                        count++;
                    });
                    string id = "000000f" + a;
                    var storing = FirebaseSave(tablename, id, docData, companyName);
                    tasks.Add(storing);
                    long modulus = a % 100;
                    if (modulus == 0)
                    {
                        System.Diagnostics.Debug.WriteLine(a);
                        System.Diagnostics.Debug.WriteLine("waiting for 100 rows to save...");
                        Task.WhenAll(tasks).Wait();
                        System.Diagnostics.Debug.WriteLine("100 rows saved");


                    }
                    a++;
                }
                System.Diagnostics.Debug.WriteLine("waiting for data to save for table...");
                Task.WhenAll(tasks).Wait();
                System.Diagnostics.Debug.WriteLine("table " + tablename + "complete");
                writeLog("table " + tablename + "complete").Wait();
                i++;
            }
            System.Diagnostics.Debug.WriteLine(i);
            sqlConnection.Close();

        }
        public  string startDataMigration(string connString, string companyName, string key) {
            Program program = new Program();
            try
            {
                if (StateCheck(key) ==true) {
                    System.Diagnostics.Debug.WriteLine(StateCheck(key));
                    System.Diagnostics.Debug.WriteLine("started Migration");
                    writeLog("started migration").Wait();
                    getData(connString, companyName);
                    return "started";

                }
                else {
                    return "state not active";
                }
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine("Exceptions caught");
                System.Diagnostics.Debug.WriteLine(e);

                writeLog("Exceptions caught").Wait();
                writeLog(e.ToString()).Wait();
                if(StateCheck(key) == true)
                {
                    scheduleTest(connString, companyName, key).Wait();

                }
                return "started wait";
            }
        }
        public async System.Threading.Tasks.Task scheduleTest(string connString, string companyName, string key)
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();

            // get a scheduler
            IScheduler scheduler = await factory.GetScheduler();
            await scheduler.Start();
            // define the job and tie it to our HelloJob class
            IJobDetail job = JobBuilder.Create<Job>()
                .WithIdentity("myJob", "group1").WithDescription(connString+"%%"+ companyName+ "%%" + key)
                .Build();

            // Trigger the job to run now, and then every 40 seconds
            ITrigger trigger = TriggerBuilder.Create()
               .WithIdentity("trigger3", "group1")
               .WithCronSchedule("0 * * ? * *")
               .ForJob("myJob", "group1")
               .Build();

            await scheduler.ScheduleJob(job, trigger);
        }
        public async Task streamTest() {
            CancellationTokenSource source = new CancellationTokenSource();
            await Task.Delay(1000, source.Token);

        }

    }
    public class Job : IJob
    {
        DataExtractor extractor = new DataExtractor();
        public async Task Execute(IJobExecutionContext context)
        {
            System.Diagnostics.Debug.WriteLine("STARTING AGAIN");
            extractor.writeLog("started again").Wait();
            await CancelJob();
            System.Diagnostics.Debug.WriteLine("job cancelled");
            extractor.writeLog("job cancelled").Wait();

        }
        public async Task CancelJob()
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            IScheduler scheduler = await factory.GetScheduler();
            JobKey key = JobKey.Create("myJob", "group1");
            var desc = scheduler.GetJobDetail(key).Result.Description;
            var messageArray =  desc.Split("%%");
            string connString = messageArray[0];
            string companyName = messageArray[1];
            string myKey = messageArray[2];
            System.Diagnostics.Debug.WriteLine(desc);
            await scheduler.Shutdown();
            extractor.startDataMigration(connString, companyName, myKey);
        }
    }
}
