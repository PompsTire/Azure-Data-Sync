using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure_Data_Sync
{
    class Program
    {
        static DBUtilities objDB;
        
        static void Main(string[] args)
        {
            Init();
            bool HasArgs = false;

            foreach (string arg in args)
            {
                HasArgs = false;
                Console.WriteLine("Running: " + arg);
                Console.WriteLine("Starting Job @:" + System.DateTime.Now);
                objDB.ResetAll();
                switch (arg.ToUpper())
                {                    
                    case "-TREADRUBBERINVENTORY":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.TreadRubberInventory);
                            break;
                        }

                    case "-BARCODEAGING":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.BarcodeAging);
                            break;
                        }

                    case "-DATACOMM":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.DataComm);
                            break;
                        }

                    case "-DATACOMMPROVIDERS":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.DataCommProviders);
                            break;
                        }

                    case "-STORES":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.Stores);
                            break;
                        }

                    case "-REGIONS":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.Regions);
                            break;
                        }

                    case "-INCOMINGTIRES":
                        {
                            HasArgs = true;
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.IncomingTires);
                            break;
                        }

                    default:
                        {
                            Console.WriteLine("\r\n  ERROR: Invalid Command Line Parameter\r\n");
                            HasArgs = false;
                            break;
                        }
                }

                if (HasArgs)
                {
                    Console.WriteLine("Download Start: " + objDB.DL_StartedAt + ", Download Completed: " + objDB.DL_CompletedAt + ", Rows Downloaded: " + objDB.RowsDownloaded.ToString());
                    Console.WriteLine("Upload Start: " + objDB.UP_StartedAt + ", Upload Completed: " + objDB.UP_CompletedAt + ", Rows Uploaded: " + objDB.RowsUploaded.ToString());

                    if (objDB.IsError)
                        Console.WriteLine("Errors Encountered: " + objDB.ErrorMessage);
                    else
                        Console.WriteLine("No Errors");
                }
            }
            
            if(HasArgs == false)
                Console.Write(HowToUse);

            //Console.WriteLine("\r\nPress Any Key...");
            //Console.ReadKey();

        }

        private static void Init()
        {
            objDB = new DBUtilities();
            objDB.connectionString_AZ = connectionstring_AZ;
            objDB.connectionString_BASYS = connectionstring_BASYS;
            objDB.connectionString_GBSQL01v2 = connection_GBSQL01v2;
        }

        private static string connectionstring_BASYS
        { get => System.Configuration.ConfigurationManager.AppSettings["BASYS_CONNECTION"]; }

        private static string connectionstring_AZ
        { get => System.Configuration.ConfigurationManager.AppSettings["AZ_CONNECTION"]; }

        private static string connection_GBSQL01v2
        { get => System.Configuration.ConfigurationManager.AppSettings["GBSQL01v2_CONNECTION"]; }


        private static string HowToUse
        {
            get
            {
                string sH = "--------------------------------------------------------\r\n"; 
                StringBuilder sb = new StringBuilder("\r\nUse command line parameters to refresh the data in 'powerbi - pomps' database in Azure:\r\n\r\n");
                sb.Append(sH);
                sb.Append(" Command Line Parameters  |    Sql tables refreshed\r\n");
                sb.Append(sH);
                sb.Append("  -TREADRUBBERINVENTORY   |  tb_TreadRubberInventory\r\n");
                sb.Append(sH);
                sb.Append("  -BARCODEAGING           |  tb_BarcodeAging\r\n");
                sb.Append(sH);
                sb.Append("  -DATACOMM               |  tb_GL_DataCommBilling\r\n");
                sb.Append(sH);
                sb.Append("  -DATACOMMPROVIDERS      |  tb_GL_DataCommProviders\r\n");
                sb.Append(sH);
                sb.Append("  -STORES                 |  tb_Stores\r\n");
                sb.Append(sH);
                sb.Append("  -REGIONS                |  tb_Regions\r\n");
                sb.Append(sH);
                sb.Append("  -INCOMINGTIRES                 |  tb_IncomingTires\r\n");
                sb.Append(sH);
                return sb.ToString();
            }
        }

    }
}
