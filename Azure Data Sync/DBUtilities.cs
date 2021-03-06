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
            Regions = 5,
            IncomingTires = 6,
            WorkorderRepairDetails = 7
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
            switch (rptDef)
            {
                case ReportDefs.TreadRubberInventory:
                    {
                        sTableName = "dbo.tb_TreadRubberInventory";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);    
                        DownloadBasysSource(SQL_TireRubberInventory);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.BarcodeAging:
                    {
                        sTableName = "dbo.tb_BarcodeAging";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);
                        DownloadBasysSource(SQL_BarcodeAging);
                        BulkCopyToSQL(sTableName);                        
                        break;
                    }
                case ReportDefs.DataComm:
                    {
                        sTableName = "dbo.tb_GL_DataCommBilling";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);    
                        DownloadSqlSource(SQL_DataComm);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.DataCommProviders:
                    {
                        sTableName = "dbo.tb_GL_DataCommProviders";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);
                        DownloadSqlSource(SQL_DataCommProviders);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.Stores:
                    {
                        sTableName = "dbo.tb_Stores";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);
                        DownloadSqlSource(SQL_Stores);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.Regions:
                    {
                        sTableName = "dbo.tb_Regions";
                        InitLog("Refresh " + sTableName);
                        TruncateTargetSqlTable(sTableName);     
                        DownloadSqlSource(SQL_Regions);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.IncomingTires:
                    {
                        // Data will be appended rather than replaced. Maintain 2 months of data then delete older
                        sTableName = "dbo.tb_IncomingTires";
                        InitLog("Refresh " + sTableName);    
                        // Purge data that is older than our 2 month window
                        ExecScalarSql(SQL_IncomingTires_TrimTable);                      
                        DownloadSqlSource(SQL_IncomingTires);
                        BulkCopyToSQL(sTableName);
                        break;
                    }
                case ReportDefs.WorkorderRepairDetails:
                    {
                        // Updating monthly with large number of records. _row_id has to be unique so we are using a staging table. 
                        // Use bulkcopy to upload to staging table. Then call store proc that will only insert unique records into
                        // production table.
                        sTableName = "dbo.tb_WorkorderRepairdetails_STAGING";
                        InitLog("Refresh " + sTableName);                                                
                        // Download all records within next reporting period from source db
                        DownloadBasysSource(SQL_WorkorderRepairDetails);
                        // Ensure staging table is empty
                        TruncateTargetSqlTable(sTableName);
                        // Fastest way to upload large datatable to staging DB
                        BulkCopyToSQL(sTableName);
                        // Run proc to insert unique records into production
                        ExecScalarSql("EXEC [powerbi-pomps].dbo.up_SyncStagingToProd");
                        break;
                    }
            }                        
            UpdateLog();
            return !IsError;
        }
               
        private void BulkCopyToSQL(string tableName)
        {
            SqlBulkCopy bkcp = new SqlBulkCopy(connectionString_AZ);
            bkcp.DestinationTableName = tableName;
            bkcp.BulkCopyTimeout = 3000;
            RowsUploaded = 0;
            UP_StartedAt = System.DateTime.Now;
            try
            {
                foreach (DataColumn col in dt.Columns)
                {
                    // This is assuming that the source column names and character casing exactly matches the target table column names and character casing. 
                    // Mapping names are case sensitive. 
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
            objDA.SelectCommand.CommandTimeout = 3000;
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

        public string SQL_IncomingTires
        {
            get
            {
                StringBuilder sb = new StringBuilder("Select * From openquery(basys,'SELECT wo_det_rpt_view._row_id");
                sb.Append(",wo_det_rpt_view._created,wo_det_rpt_view.wodate,wo_det_rpt_view.tracs_no,wo_det_rpt_view.wodno");
                sb.Append(",wo_det_rpt_view.wodline,wo_det_rpt_view.wodstore,wo_det_rpt_view._wostbill");
                sb.Append(",cast(wo_det_rpt_view._line_code as varchar(50)) as _line_code,wo_det_rpt_view.wodbrand,wo_det_rpt_view.wodtsize");
                sb.Append(",wo_det_rpt_view.wodabbr,cast(wo_det_rpt_view._line_stat as varchar(50)) as _line_stat,wo_det_rpt_view._nsnact");
                sb.Append(",wo_det_rpt_view._wonashploc,wo_det_rpt_view.cust_no,wo_det_rpt_view._cust_name, " + JobLogID.ToString() + " as fk_maintenancelogid ");
                sb.Append("FROM pt.wo_det_rpt_view WHERE wo_det_rpt_view._created > (now() - INTERVAL ''24 hours'') ");
                sb.Append("AND wo_det_rpt_view._line_stat In (''ORDERED'',''INTRANSIT'',''RECEIVED'') ");
                sb.Append("AND wo_det_rpt_view._line_code Not In (''ADJ'',''COM'',''SCP'') ");
                sb.Append("ORDER BY _created ' ) ");
                return sb.ToString();
            }
        }

        public string SQL_WorkorderRepairDetails
        {
            get
            {
                StringBuilder sb = new StringBuilder("SELECT wo_det_rpt_view._row_id, wo_det_rpt_view.wodno, wo_det_rpt_view.wodline, wo_det_rpt_view.wodstore, wo_det_rpt_view.woddate, ");
                sb.Append("wo_det_rpt_view.bead, wo_det_rpt_view.spot, wo_det_rpt_view.nail, wo_det_rpt_view.rr1, wo_det_rpt_view.section, ");
                sb.Append("wo_det_rpt_view.line_status, wo_det_rpt_view._line_stat, wo_det_rpt_view.wodcode, wo_det_rpt_view._line_code, ");
                sb.Append("wo_det_rpt_view.ins_upd, wo_det_rpt_view.wodprod, wo_det_rpt_view._nrt_tire, wo_det_rpt_view._nrt_code, wo_det_rpt_view._nrt_reason ");
                sb.Append("FROM pt.wo_det_rpt_view wo_det_rpt_view ");
                sb.Append("WHERE (wo_det_rpt_view.woddate >= {d '" + LastMonth(false) + "'} ");
                sb.Append("And wo_det_rpt_view.woddate < {d '" + LastMonth(true) + "'}) ");
                sb.Append("AND (wo_det_rpt_view.line_status Not In (0,1,4)) ");
                sb.Append("AND (wo_det_rpt_view.wodcode Not In (8)) ");
                sb.Append("AND (wo_det_rpt_view._line_stat Not In ('SERVICE')) ");
                return sb.ToString();
            }
        }

        public string SQL_IncomingTires_TrimTable
        {
            get
            {
                String MM = System.DateTime.Today.AddMonths(-1).Month.ToString();
                String YY = System.DateTime.Today.AddMonths(-1).Year.ToString();
                String MDYY = MM + "/01/" + YY;

                StringBuilder sb = new StringBuilder("Delete From [dbo].[tb_TreadRubberInventory] ");
                sb.Append("Where fk_maintenancelogid IN ");
                sb.Append("(Select PKID ");
                sb.Append(" From [dbo].[tb_DataMaintenanceLog] ");
                sb.Append(" where taskname like '%incomingtires%' ");
                sb.Append(" AND DateTimeStart < '" + MDYY + "') ");
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

        public string LastMonth(bool UseEndOfMonth)
        {
            String YY = "";
            String MM = "";
            String DD = "01";
            String RtnDate = "";

            YY = System.DateTime.Today.AddMonths(-1).Year.ToString();

            if (UseEndOfMonth == false)
            {
                MM = System.DateTime.Today.AddMonths(-1).Month.ToString();
                RtnDate = YY + "-" + MM + "-" + DD;
                //Testing: // RtnDate = "2021-07-01";
            }
            else
            {
                MM = System.DateTime.Today.Month.ToString();
                DD = (DateTime.Parse(YY + "-" + MM + "-01")).AddDays(-1).Day.ToString();
                MM = System.DateTime.Today.AddMonths(-1).Month.ToString();
                RtnDate = YY + "-" + MM + "-" + DD;
                // Testing: //RtnDate = "2021-08-16";
            }
            return RtnDate;                                                  
        }

        public bool IsError { get; set; }
        public string ErrorMessage { get; set; }
        private int JobLogID { get; set; }
    }
}
