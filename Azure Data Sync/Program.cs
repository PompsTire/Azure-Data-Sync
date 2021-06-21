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
            foreach (string arg in args)
            {
                Console.WriteLine("Running: " + arg);
                Console.WriteLine("Starting Job @:" + System.DateTime.Now);
                objDB.ResetAll();
                switch (arg.ToUpper())
                {                    
                    case "TREADRUBBERINVENTORY":
                        {                            
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.TreadRubberInventory);
                            break;
                        }

                    case "BARCODEAGING":
                        {
                            objDB.ExecuteDataRefresh(DBUtilities.ReportDefs.BarcodeAging);
                            break;
                        }
                }

                Console.WriteLine("Download Start: " + objDB.DL_StartedAt + ", Download Completed: " + objDB.DL_CompletedAt + ", Rows Downloaded: " + objDB.RowsDownloaded.ToString());
                Console.WriteLine("Upload Start: " + objDB.UP_StartedAt + ", Upload Completed: " + objDB.UP_CompletedAt + ", Rows Uploaded: " + objDB.RowsUploaded.ToString());

                if (objDB.IsError)
                    Console.WriteLine("Errors Encountered: " + objDB.ErrorMessage);
                else
                    Console.WriteLine("No Errors");
            }

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

    }
}
