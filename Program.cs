using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.Net;
using System.Net.Mail;
using Renci.SshNet;

namespace BFS_Sell_Out_Program
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection connection = new SqlConnection("Data Source=gbsql01v2;Initial Catalog=Dealer_Programs;Persist Security Info=True;User ID=sa;Password=4aCN4Ns");
            OdbcConnection maddenco = new OdbcConnection("Dsn=Maddenco DTA577;uid=U577READO6;pwd=IWEXM6");
            string logFileYear, logFileMonth, logFileDay,yesterdayDay;

            logFileYear = DateTime.Today.Year.ToString();

            if (DateTime.Today.Month < 10) { logFileMonth = "0" + DateTime.Today.Month.ToString(); } else { logFileMonth = DateTime.Today.Month.ToString(); }
            if (DateTime.Today.Day < 10) { logFileDay = "0" + DateTime.Today.Day.ToString(); } else { logFileDay = DateTime.Today.Day.ToString(); }
            if (DateTime.Today.AddDays(-1).Day < 10) { yesterdayDay = "0" + DateTime.Today.AddDays(-1).Day.ToString(); } else { yesterdayDay = DateTime.Today.AddDays(-1).Day.ToString(); }

            Download_Purchases_Receipt(connection, logFileYear, logFileMonth, yesterdayDay);
            WriteDatafile(maddenco, logFileYear, logFileMonth, logFileDay);
            //Console.Read();
            System.Environment.Exit(1);
        }
        private static void Email_Notification(string emailFrom, string emailSubject, string msg)
        {
            MailMessage Notification = new MailMessage();
            SmtpClient client = new SmtpClient();
            client.Port = 25;
            client.Host = "mail.pompstire.com";
            client.Timeout = 100000;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.UseDefaultCredentials = false;
            client.Credentials = new System.Net.NetworkCredential("anonymous", "");
            Notification.From = new MailAddress(emailFrom);
            Notification.To.Add(new MailAddress("dbarrett@pompstire.com"));
            //Notification.CC.Add(new MailAddress("dbarrett@pompstire.com"));
            Notification.Subject = emailSubject;
            Notification.IsBodyHtml = true;
            Notification.Body = msg;
            client.Send(Notification);
        }

        private static void Download_Purchases_Receipt(SqlConnection connection, string logFileYear, string logFileMonth, string logFileDay)
        {
            string filePath, downloadFileName, host, username, password, workingdirectory;
            try
            {
                filePath = @"C:\\Scheduled Tasks\\BFS_Sell_Out_Report\\Documents\\";
                DirectoryInfo di = new DirectoryInfo(filePath);
                //DeleteFiles(di);

                downloadFileName = "ack_309354_" + logFileYear + logFileMonth + logFileDay + ".csv";
                Console.WriteLine(downloadFileName);
                host = "data.shift365.com";
                username = "309354mc";
                password = "EqG%-$rfP-15Yt";
                workingdirectory = "/outbound/";

                using (var client = new SftpClient(host, 22, username, password))

                {

                    client.Connect();

                    Console.WriteLine("Connected to {0}", host);

                    client.ChangeDirectory(workingdirectory);
                    Console.WriteLine("Changed directory to {0}", workingdirectory);

                    //using (Stream fileStream = File.Create(@"C:\\Bridgestone\\test\\" + downloadFileName))
                    //{
                    //client.DownloadFile(downloadFileName, fileStream);
                    //}

                    using (var file = File.OpenWrite(filePath + downloadFileName))
                    {
                        client.DownloadFile(downloadFileName, file);
                    }

                    client.Disconnect();
                }

                //Upload_Receipt_File(connection, filePath, downloadFileName);
                Console.WriteLine("BFS Sell Out Report acknowledgment file has been downloaded.");
            }
            catch (Exception error)
            {
                string msg = "GBSQL01v2\nWhile Downloading BFS Sell Out Purchases Receipt\n" + error.Message;
                Email_Notification("BFS_Sell_Out@pompstire.com", "BFS Sell Out Upload Error", msg);
            }
        }

        private static void Upload_BFS_Sell_Out_File(string logFileName)
        {
            string host, username, password;
            host = "data.shift365.com";
            username = "309354mc";
            password = "EqG%-$rfP-15Yt";
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
                    Console.WriteLine("BFS Tier Purchases has been uploaded to BFS sftp");
                }
            }
            catch (Exception ex)
            {
                string msg = "GBSQL01v2\nBFS Sell Out SFTP Upload Error\n" + ex.Message;
                Email_Notification("BFS_Sell_Out@pompstire.com", "BFS_Sell_Out Notification: Error", msg);
            }

        }

        private static void WriteDatafile(OdbcConnection cn, string logFileYear, string logFileMonth, string logFileDay)
        {
            string dataLine, myQuery, logFileName;

            myQuery = @"select tihhclscst as Customer_Class,To_Date(tihhdteinv,'YYYYMMDD') as Invoice_Date,pdmfgprdno as Manufacturer_Product_Number
,cast(tihlqty as integer) as Quantity,tihhnuminv as Invoice_Number,sxcvstrcv as BSF_Location
from dta577.tmihsh inner join dta577.tmihsl on tihhnuminv = tihlnuminv inner join dta577.tmsxcv
on tihhnumstr = sxcvstrmc inner join dta577.tmprod on tihlnumstr = pdstore and tihlprd = pdnumber
where Year(To_Date(tihhdteinv,'YYYYMMDD')) = ?
and Month(To_Date(tihhdteinv,'YYYYMMDD')) = ?
and tihhclscst <> 'I' and tihhvoidyn <> 'Y' and tihlclsprd in('04','08') and tihlvndprd in('010','011','013') and tihlcoddel <> 'D'
and tihlprd not like('%GOV%') and sxcvnumvnd = '0031370'";

            logFileName = "C:\\Scheduled Tasks\\BFS_Sell_Out_Report\\Documents\\309354_" + logFileYear + logFileMonth + logFileDay + ".csv";
            //logFileName = "C:\\Scheduled Tasks\\BFS_Sell_Out_Report\\Documents\\309354_20190831.csv";
            StreamWriter outputFile;
            outputFile = File.CreateText(logFileName);

            cn.Open();
            OdbcCommand cmd = new OdbcCommand(myQuery, cn);
            cmd.Parameters.Add("Parameter1", OdbcType.Text).Value = DateTime.Today.AddDays(-1).Year.ToString();
            cmd.Parameters.Add("Parameter2", OdbcType.Text).Value = DateTime.Today.AddDays(-1).Month.ToString();
            try
            {
                string invDate;
                using (OdbcDataReader dataReader = cmd.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        DateTime invoiceDate = Convert.ToDateTime(dataReader["Invoice_Date"].ToString());
                        invDate = invoiceDate.ToShortDateString();

                        dataLine = invDate + ",";
                        dataLine = dataLine + dataReader["Manufacturer_Product_Number"].ToString() + ",";
                        dataLine = dataLine + dataReader["Quantity"].ToString() + ",";
                        dataLine = dataLine + dataReader["Invoice_Number"].ToString() + ",";
                        dataLine = dataLine + dataReader["BSF_Location"].ToString();
                        //dataLine = dataReader["Customer_Class"].ToString() + ",";

                        outputFile.WriteLine(dataLine);
                        Console.Write(dataLine);

                    }
                    cn.Close();
                    outputFile.Close();
                    Upload_BFS_Sell_Out_File(logFileName);
                }
            }
            catch (Exception ex)
            {
                //throw ex;
                string msg = "GBSQL01v2\nDealer_Programs.dbo.sp_BFS_Sell_Out_Program\n" + ex.Message;
                Email_Notification("GBSQL01v2@pompstire.com", "BFS Sell Out Upload Error", msg);
            }

        }
    }
}
