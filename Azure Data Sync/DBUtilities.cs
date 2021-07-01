using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;

namespace Azure_Data_Sync
{
    class DBUtilities
    {
        public enum ReportDefs
        {
            TreadRubberInventory = 0,
            BarcodeAging = 1,
            DataComm = 2,
            DataCommProviders = 3,
            Stores = 4,
            Regions = 5
        };

        private DataTable dt;

        public DBUtilities()
        {
            ResetAll();
        }

        public void ResetAll()
        {
            ClearError();
            RowsDownloaded = 0;
            RowsUploaded = 0;
            JobLogID = -1;
            DL_StartedAt = System.DateTime.Now;
            DL_CompletedAt = System.DateTime.Now;
            UP_StartedAt = System.DateTime.Now;
            UP_CompletedAt = System.DateTime.Now;
            dt = new DataTable();
        }

        public bool ExecuteDataRefresh(ReportDefs rptDef)
        {
            String sTableName = "";
            String sSql = "";
            switch (rptDef)
            {
                case ReportDefs.TreadRubberInventory:
                    {
                        sTableName = "dbo.tb_TreadRubberInventory";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_TireRubberInventory;
                        DownloadBasysSource(sSql);
                        break;
                    }
                case ReportDefs.BarcodeAging:
                    {
                        sTableName = "dbo.tb_BarcodeAging";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_BarcodeAging;
                        DownloadBasysSource(sSql);
                        break;
                    }
                case ReportDefs.DataComm:
                    {
                        sTableName = "dbo.tb_GL_DataCommBilling";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_DataComm;
                        DownloadSqlSource(sSql);
                        break;
                    }
                case ReportDefs.DataCommProviders:
                    {
                        sTableName = "dbo.tb_GL_DataCommProviders";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_DataCommProviders;
                        DownloadSqlSource(sSql);
                        break;
                    }
                case ReportDefs.Stores:
                    {
                        sTableName = "dbo.tb_Stores";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_Stores;
                        DownloadSqlSource(sSql);
                        break;
                    }
                case ReportDefs.Regions:
                    {
                        sTableName = "dbo.tb_Regions";
                        InitLog("Refresh " + sTableName);
                        sSql = SQL_Regions;
                        DownloadSqlSource(sSql);
                        break;
                    }
            }            
            
            TruncateTargetSqlTable(sTableName);
            BulkCopyToSQL(sTableName);
            UpdateLog();
            return !IsError;
        }
               
        private void BulkCopyToSQL(string tableName)
        {
            SqlBulkCopy bkcp = new SqlBulkCopy(connectionString_AZ);
            bkcp.DestinationTableName = tableName;
            RowsUploaded = 0;
            UP_StartedAt = System.DateTime.Now;
            try
            {
                foreach (DataColumn col in dt.Columns)
                {
                    // This is assuming that the source column names and character casing exactly matches the target table column names and character casing. 
                    // Mapping names are case sensitive. Will need to change this if source and target names do not match on future data uploads
                    bkcp.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }
                bkcp.WriteToServer(dt);
                RowsUploaded = dt.Rows.Count;
            }
            catch (Exception ex)
            { SetError(ex.Message); }
            finally
            { UP_CompletedAt = System.DateTime.Now; }
        }

        private void InitLog(string TaskName)
        {
            StringBuilder sb = new StringBuilder("Insert Into dbo.tb_DataMaintenanceLog ");
            sb.Append("(DateTimeStart,TaskName, TaskResults, ErrorsOccured, ErrorMessages) ");
            sb.Append("VALUES (");
            sb.Append("'" + System.DateTime.Now + "','" + TaskName + "','',0,'') " );
            sb.Append("SELECT SCOPE_IDENTITY() as TASKID ");

            object objID = ExecScalarSql(sb.ToString());
            JobLogID = int.Parse(objID.ToString());
        }

        private void UpdateLog()
        {
            StringBuilder sb = new StringBuilder("UPDATE dbo.tb_DataMaintenanceLog SET ");
            sb.Append("DateTimeEnd = '" + System.DateTime.Now + "', TaskResults = '" + TaskResults + "', ErrorsOccured = '" + IsError);
            sb.Append("', ErrorMessages = '" + ErrorMessage + "' Where PKID = " + JobLogID.ToString());
            ExecScalarSql(sb.ToString());
        }

        private void TruncateTargetSqlTable(string tableName)
        {
            String sql = "truncate table " + tableName;
            ExecScalarSql(sql);
        }

        private object ExecScalarSql(string sql)
        {
            SqlCommand objComm = new SqlCommand();
            object objRslt = new object();
            try
            {
                objComm.Connection = new SqlConnection(connectionString_AZ);
                objComm.CommandText = sql;
                objComm.Connection.Open();
                objRslt = objComm.ExecuteScalar();
            }
            catch (Exception ex)
            {
                SetError(ex.Message);
            }
            finally
            {
                objComm.Connection.Close();
                objComm.Dispose();
            }
            return objRslt;
        }

        private void DownloadSqlSource(string sql)
        {
            SqlDataAdapter objDA = new SqlDataAdapter(sql, connectionString_GBSQL01v2);
            dt = new DataTable();
            RowsDownloaded = 0;
            try
            {
                DL_StartedAt = System.DateTime.Now;
                objDA.Fill(dt);
                RowsDownloaded = dt.Rows.Count;
            }
            catch (Exception ex)
            { SetError(ex.Message); }
            finally
            { DL_CompletedAt = System.DateTime.Now; }
        }


        private void DownloadBasysSource(string sql)
        {
            OdbcDataAdapter objDA = new OdbcDataAdapter(sql, connectionString_BASYS);
            dt = new DataTable();
            RowsDownloaded = 0;
            try
            {
                DL_StartedAt = System.DateTime.Now;
                objDA.Fill(dt);
                RowsDownloaded = dt.Rows.Count;
            }
            catch (Exception ex)
            { SetError(ex.Message); }
            finally
            { DL_CompletedAt = System.DateTime.Now; }
        }

        // Internal Performance Metrics
        public int RowsDownloaded { get; set; }
        public int RowsUploaded { get; set; }
        public DateTime DL_StartedAt { get; set; }
        public DateTime DL_CompletedAt { get; set; }
        public DateTime UP_StartedAt { get; set; }
        public DateTime UP_CompletedAt { get; set; }

        public string TaskResults
        {
            get
            {
                StringBuilder sb = new StringBuilder("Task Run Time: " + TaskRuntimeSecs.ToString() + " secs, Rows Downloaded From BASYS :" + RowsDownloaded.ToString() + ", ");
                sb.Append("Rows Uploaded to SQL: " + RowsUploaded.ToString());
                return sb.ToString();
            }
        }

        public double TaskRuntimeSecs
        {
            get
            {
                return Math.Round((System.DateTime.Now -  DL_StartedAt).TotalSeconds,0);
            }
        }

        // Exception Reporting
        private void SetError(string msg)
        {
            IsError = true;
            if (ErrorMessage.Length > 0)
                ErrorMessage += "; ";
            ErrorMessage += msg;
        }

        private void ClearError()
        {
            IsError = false;
            ErrorMessage = "";
        }

        // Connection Strings
        public string connectionString_BASYS { get; set; }
        public string connectionString_AZ { get; set; }
        public string connectionString_GBSQL01v2 { get; set; }

        // Sql Statements
        public string SQL_Stores
        {
            get
            {
                StringBuilder sb = new StringBuilder("Select S.StoreNo as StoreNumber, RIGHT('000' + convert(varchar(3),S.StoreNo), 3) as StoreNo, rtrim(S.StoreName) as StoreName, ");
                sb.Append("rtrim(S.StoreAddress) as StoreAddress, rtrim(S.StoreCity) as StoreCity, rtrim(S.StoreState) as StoreState, S.StoreZip, ");
                sb.Append("rtrim(S.StoreType) as StoreType, rtrim(S.StoreManager) as StoreManager, R.RegionNo From Store S INNER JOIN ");
                sb.Append("Region R ON S.StoreRegionID = R.RegionID Where S.StoreActive = 1 AND S.StoreNo <> 0  Order By StoreNo ");
                return sb.ToString();
            }
        }

        public string SQL_Regions
        {
            get
            {
                StringBuilder sb = new StringBuilder("Select RegionNo, rtrim(RegionName) RegionName, rtrim(RegionManagerName) as RegionManagerName, ");
                sb.Append("rtrim(RegionManagerSupervisor) as RegionManagerSupervisor From Region ");
                return sb.ToString();
            }
        }

        public string SQL_TireRubberInventory
        {
            get
            {
                StringBuilder sb = new StringBuilder("SELECT raw_mat.rmstore,raw_mat.rmitem,raw_mat.rmtread,raw_mat.rmdesc,");
                sb.Append("raw_mat.freeze_lbs,raw_mat.rmunit,raw_mat.rmoh,raw_mat.rmpunit,raw_mat.rmmfritem2,raw_mat.rmmfritem,");
                sb.Append("raw_mat.rmmfritem3,raw_mat.ecl_tread,raw_mat.stock_status," + JobLogID.ToString() + " as fk_maintenancelogid ");
                sb.Append("FROM pt.raw_mat raw_mat WHERE(raw_mat.rmstore >= 501) AND(raw_mat.rmcat = 1) ");
                sb.Append("ORDER BY raw_mat.rmtread");
                return sb.ToString();
            }
        }

        public string SQL_BarcodeAging
        {
            get
            {
                StringBuilder sb = new StringBuilder("SELECT wo_det_rpt_view.wodstore, wo_det_rpt_view._xfr_store, wo_det_rpt_view.tracs_no, wo_det_rpt_view.wodno,");
                sb.Append("wo_det_rpt_view.wodline, wo_det_rpt_view.wodate, wo_det_rpt_view._wostdate, wo_det_rpt_view._wodwanted, ");
                sb.Append("wo_det_rpt_view._modified, wo_det_rpt_view._line_code, wo_det_rpt_view._wotype, wo_det_rpt_view.cust_no,");
                sb.Append("wo_det_rpt_view._wonashploc, wo_det_rpt_view._cust_name, wo_det_rpt_view.line_status,");
                sb.Append("wo_det_rpt_view._line_stat, wo_det_rpt_view.wodbrand, wo_det_rpt_view.wodtsize, wo_det_rpt_view.orig_casing,");
                sb.Append("wo_det_rpt_view._rtdesc, wo_det_rpt_view.wodtrsize," + JobLogID.ToString() + " as fk_maintenancelogid FROM pt.wo_det_rpt_view wo_det_rpt_view ");
                sb.Append("WHERE (wo_det_rpt_view.line_status In (6,8,11,13)) ");
                sb.Append("AND(wo_det_rpt_view._modified<now() - interval '2 days') AND(wo_det_rpt_view._line_code<>'COM') ");
                sb.Append("ORDER BY wo_det_rpt_view.wodstore, wo_det_rpt_view.line_status, wo_det_rpt_view.wodate");
                return sb.ToString();
            }
        }

        public string SQL_DataComm
        {
            get
            {
                StringBuilder sb = new StringBuilder("Select Right('000' + convert(varchar(3), gmnbdiv), 3) as StoreNumber ");
                sb.Append(",gadtcrt as DateCreated ");
                sb.Append(",Replace(LEFT(REPLACE(RTRIM(gadsr), '     ','*'), CharIndex('*',REPLACE(RTRIM(gadsr), '     ','*'))),'*','') as ProviderName ");
                sb.Append(",RTRIM(Replace(Substring(REPLACE(RTRIM(gadsr), '     ','*'), CharIndex('*',REPLACE(RTRIM(gadsr), '     ','*')), LEN(REPLACE(RTRIM(gadsr), '     ','*'))),'*','')) as AccountNumber ");
                sb.Append(",case when cramt_gaamt = 0 then null else cramt_gaamt end as CreditAmount ");
                sb.Append(",case when dbamt_gaamt = 0 then null else dbamt_gaamt end as DebitAmount ");
                sb.Append(",gacdsys  as [Source], gmtypact as AccountType, GetDate() as LastUpdated ");
                sb.Append("From openquery(maddenco_dta577,");
                sb.Append("'select GA.GADTCRT,	");
                sb.Append("GA.GADTPST,	");
                sb.Append("GA.GAJEDTECRT, ");
                sb.Append("GA.GAJETIMCRT, ");
                sb.Append("GA.GADSR, ");
                sb.Append("GA.GACDSYS, ");
                sb.Append("CASE WHEN IFNULL(GA.GACDDBCR,'''') = ''CR'' THEN IFNULL(GA.GAAMT,0) ELSE ''0'' END as CRAMT_GAAMT, ");
                sb.Append("CASE WHEN COALESCE(GA.GACDDBCR,'''') = ''DB'' THEN GA.GAAMT ELSE ''0'' END as DBAMT_GAAMT, ");
                sb.Append("GA.ganbref, GA.ganbrefrc, ");
                sb.Append("GA.gapr, GA.gaseq, GA.gayr, GA.gmnb, GA.gmnbdiv, GA.gmnbdpt, ");
                sb.Append("GM.gmdcract, GM.gmtypact, GM.gmnbco ");
                sb.Append("from gmad GA LEFT JOIN ");
                sb.Append("gmgm GM ON GA.gmnb = GM.GMNB AND GA.GMNBDIV = GM.GMNBDIV AND GA.GMNBDPT = GM.GMNBDPT ");
                sb.Append("where GA.gadtcrt between ''20210101'' AND ''20210630'' ");
                sb.Append("AND GA.GMNB = ''72740'' ')");
                return sb.ToString();
            }
        }

        public string SQL_DataCommProviders
        {
            get
            {
                StringBuilder sb = new StringBuilder("Select ServiceProviderName as ProviderName, GetDate() as LastUpdated ");
                sb.Append("From Pomps.dbo.tb_TelecommProviders ");
                sb.Append("Where LEN(ServiceProviderName) > 0 ");
                return sb.ToString();
            }

        }

        public bool IsError { get; set; }
        public string ErrorMessage { get; set; }
        private int JobLogID { get; set; }
    }
}
