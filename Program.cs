using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Mail;
using Renci.SshNet;

namespace BFS_Sell_Out_Program
{
    class Program
    {
        private static DataTable dtLog;      
        private static string m_errorMessage;
        private static int m_errorCount;    
        private static int m_recordsSent;
        
        [STAThread]
        static void Main(string[] args)
        {
            Console.WriteLine("Launching Program at " + System.DateTime.Now.ToString());

            ClearErrors();
            Init_JobActivityLog();
            DateTime dtStart = System.DateTime.Now;
            DateTime dtEnd;

            DataRow dr = dtLog.NewRow();
            dr["JobName"] = "BFS_SELL_OUT_PROGRAM";
            dr["JobEvent"] = "Job Start";
            dr["JobEventStartDateTime"] = dtStart.ToString();
                       
            SqlConnection connection = new SqlConnection("Data Source=gbsql01v2;Initial Catalog=Dealer_Programs;Persist Security Info=True;User ID=sa;Password=4aCN4Ns");
            try
            {
                string logFileYear, logFileMonth, logFileDay, yesterdayDay;
                logFileYear = DateTime.Today.Year.ToString();

                if (DateTime.Today.Month < 10) { logFileMonth = "0" + DateTime.Today.Month.ToString(); } else { logFileMonth = DateTime.Today.Month.ToString(); }
                if (DateTime.Today.Day < 10) { logFileDay = "0" + DateTime.Today.Day.ToString(); } else { logFileDay = DateTime.Today.Day.ToString(); }
                if (DateTime.Today.AddDays(-1).Day < 10) { yesterdayDay = "0" + DateTime.Today.AddDays(-1).Day.ToString(); } else { yesterdayDay = DateTime.Today.AddDays(-1).Day.ToString(); }

                Download_Purchases_Receipt(connection, logFileYear, logFileMonth, yesterdayDay);
                WriteDataFile(connection.ConnectionString, logFileYear, logFileMonth, logFileDay);
            }
            catch (Exception ex)
            { SetError(ex.Message); }
            finally
            {
                dtEnd = System.DateTime.Now;
                TimeSpan intvl = dtEnd - dtStart;
                Console.WriteLine("All Tasks Completed. Writing To Job Log");

                dr["JobEventCompletedDateTime"] = dtEnd.ToString();

                if (m_errorCount > 0)
                    dr["JobEventResults"] = "Errors have occured. See log file for details.";
                else
                    dr["JobEventResults"] = m_recordsSent.ToString() + " Rows sent with no errors";

                dr["JobEventErrorsCount"] = m_errorCount.ToString();
                dtLog.Rows.Add(dr);
                UpdateJobLog(connection);

                if (m_errorCount > 0)
                    WriteJobActivitTextFileLog();

                Console.WriteLine("Job Completed With " + m_errorCount.ToString() + " Errors at " + System.DateTime.Now.ToString());
                Console.WriteLine(intvl.Minutes.ToString() + " Minutes, " + intvl.Seconds.ToString() + " Seconds Total Run Time.");
                //Console.WriteLine("\r\nPress Any Key...");
                //Console.ReadKey();
                System.Environment.Exit(1);
            }            
        }

        private static void UpdateJobLog(SqlConnection objConn)
        {
            String sql1 = "EXEC Dealer_Programs.dbo.up_DealerPrograms_JobsActivityLog_Update ";
            SqlCommand objComm = new SqlCommand();
            objComm.Connection = objConn;
            StringBuilder sbSql2 = new StringBuilder();
            objConn.Open();
            try
            {
                foreach (DataRow dr in dtLog.Rows)
                {
                    sbSql2.Clear();
                    sbSql2.Append("@PKID = 0,");
                    sbSql2.Append("@JobName = '" + dr["JobName"].ToString() + "', ");
                    sbSql2.Append("@JobEvent = '" + dr["JobEvent"].ToString() + "', ");
                    sbSql2.Append("@JobEventStartDateTime = '" + dr["JobEventStartDateTime"].ToString() + "', ");
                    sbSql2.Append("@JobEventCompletedDateTime = '" + dr["JobEventCompletedDateTime"].ToString() + "', ");
                    sbSql2.Append("@JobEventResults = '" + dr["JobEventResults"].ToString() + "', ");
                    sbSql2.Append("@JobEventErrorsCount = " + dr["JobEventErrorsCount"].ToString());
                    objComm.CommandText = sql1 + sbSql2.ToString();
                    objComm.ExecuteNonQuery();
                }
            }
            catch(Exception ex)
            {
                SetError(ex.Message);
            }
            finally
            {
                objConn.Close();
            }
        }

        private static void WriteJobActivitTextFileLog()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Errors Occured In BFS_Sell_Out_Program. \r\n");
            sb.Append(System.DateTime.Now.ToString() + "\r\n");
            sb.Append("-----------------------------------------\r\n");
            using (StreamWriter sw = File.CreateText(@"BFS_Sell_Out_Program_ErrorLog.txt"))
            {
                sw.Write(sb.ToString() + m_errorMessage);                
            }                
        }

        private static void Download_Purchases_Receipt(SqlConnection connection, string logFileYear, string logFileMonth, string logFileDay)
        {
            string filePath, downloadFileName, host, username, password, workingdirectory;
            try
            {
                filePath = @"C:\Scheduled Tasks\BFS_Sell_Out_Report\Documents\";
                //filePath = @"C:\Temp\BFS\";

                DirectoryInfo di = new DirectoryInfo(filePath);
  
                downloadFileName = "ack_309354_" + logFileYear + logFileMonth + logFileDay + ".csv";
                Console.WriteLine("Downloading " + downloadFileName);
                host = "data.shift365.com";
                username = "309354mc";
                password = "EqG%-$rfP-15Yt";
                workingdirectory = "/outbound/";

                using (var client = new SftpClient(host, 22, username, password))
                {
                    client.Connect();                    
                    client.ChangeDirectory(workingdirectory);                    
                    using (var file = File.OpenWrite(filePath + downloadFileName))
                    {
                        client.DownloadFile(downloadFileName, file);
                    }
                    client.Disconnect();
                }
            }
            catch (Exception error)
            {
                SetError("Download_Purchases_Reciept: " + error.Message);
            }
        }

        private static void Upload_BFS_Sell_Out_File(string logFileName)
        {
            string host, username, password;
            host = "data.shift365.com";
            username = "309354mc";
            password = "EqG%-$rfP-15Yt";
            Console.WriteLine("Uploading Data File");
            try
            {
                using (var client = new SftpClient(host, 22, username, password))
                {
                    client.Connect();
                    client.ChangeDirectory("/inbound/");
                    using (var fileStream = new FileStream(logFileName, FileMode.Open))
                    {
                        client.BufferSize = 4 * 1024;
                        client.UploadFile(fileStream, Path.GetFileName(logFileName));
                    }
                    client.Disconnect();                    
                }
            }
            catch (Exception ex)
            {
                SetError("Upload Data File: " + ex.Message);                         
            }
        }

        private static void WriteDataFile(string sqlConnString, string logFileYear, string logFileMonth, string logFileDay)
        {
            string logFileName;
            string sql = "EXEC Dealer_Programs.dbo.up_BFS_Sellout_Program_v2";
            StringBuilder sb = new StringBuilder("");

            logFileName = "C:\\Scheduled Tasks\\BFS_Sell_Out_Report\\Documents\\309354_" + logFileYear + logFileMonth + logFileDay + ".csv";
            //logFileName = @"C:\temp\BFS\309354_" + logFileYear + logFileMonth + logFileDay + ".csv";
            DataTable dt = new DataTable();

            Console.WriteLine("Generating Report Data");
            try
            {
                SqlDataAdapter da = new SqlDataAdapter(sql, sqlConnString);
                da.SelectCommand.CommandTimeout = 100000;                
                da.Fill(dt);
            }
            catch(Exception ex)
            { SetError(ex.Message); }

            Console.WriteLine("Writing Data To File");
            StreamWriter outputFile;
            outputFile = File.CreateText(logFileName);
            m_recordsSent = 0;
            foreach (DataRow dr in dt.Rows)
            {
                sb.Clear();
                try
                {
                    sb.Append(DateTime.Parse(dr["Invoice_Date"].ToString()).ToShortDateString() + ",");
                    sb.Append(dr["Manufacturer_Product_Number"].ToString().Trim() + ",");
                    sb.Append(dr["Quantity"].ToString().Trim() + ",");
                    sb.Append(dr["Invoice_Number"].ToString().Trim() + ",");
                    sb.Append(dr["BSF_Location"].ToString().Trim());
                    outputFile.WriteLine(sb.ToString());
                    m_recordsSent++;
                }
                catch(Exception ex)
                { SetError(ex.Message); }
            }            
            outputFile.Close();
            Upload_BFS_Sell_Out_File(logFileName);
        }

        private static void Init_JobActivityLog()
        {
            dtLog = new DataTable();
            DataColumn[] dc = new DataColumn[6];
            dc[0] = new DataColumn("JobName");
            dc[1] = new DataColumn("JobEvent");
            dc[2] = new DataColumn("JobEventStartDateTime");
            dc[3] = new DataColumn("JobEventCompletedDateTime");
            dc[4] = new DataColumn("JobEventResults");
            dc[5] = new DataColumn("JobEventErrorsCount");
            dtLog.Columns.AddRange(dc);
        }

        private static void ClearErrors()
        {    
            m_errorMessage = "";
            m_errorCount = 0;
        }

        private static void SetError(string msg)
        {
            if (m_errorMessage.Length > 0)
                m_errorMessage += "\r\n";

            m_errorMessage += msg;
            m_errorCount++;
        }
    }
}
