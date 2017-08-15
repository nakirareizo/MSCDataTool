﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace MSCDataTool.Classes
{

    class SyncApprovedAccount
    {
        internal static DataTable wizardData = new DataTable();
        static bool isRollBack = false;
        internal static void StartSync(bool isStartUp)
        {
            CleanFile();
            LogFileHelper.logList = new ArrayList();
            string sSource = null;
            string sLog = null;
            string sMachine = null;

            sSource = "Wizard Sync";
            sLog = "Application";
            sMachine = ".";

            #region CRM_PRD


            using (SqlConnection Connection = SQLHelper.GetConnection())
            {
                try
                {
                    Console.WriteLine(string.Format("[{0}] : Start Sync ACApprovedAccount", DateTime.Now.ToString()));
                    LogFileHelper.logList.Add(string.Format("[{0}] : Start Sync ACApprovedAccount", DateTime.Now.ToString()));
                    string SyncedDate = "";

                    if (isStartUp)
                    {
                        //READ FROM EXCEL FILE:
                        string ExcelFileName = getLatestExcelFile();
                        wizardData = ExcelToDT.exceldata(ExcelFileName);
                        SyncedDate = DateTime.Now.ToString("dd-MM-yyyy");
                    }
                    else
                    {
                        //READ FROM SPBigFIle:
                        wizardData = SelectACApprovedAccountList(out SyncedDate);
                    }
                    if (wizardData.Rows.Count > 0)
                    {
                        foreach (DataRow dr in wizardData.Rows)
                        {
                            //dr["CoreActivities"] =dr["CoreActivities"].ToString().Replace("&#xB", "")
                            dr["CoreActivities"] = CleanInvalidXmlChars(dr["CoreActivities"].ToString());
                        }
                        if (wizardData.Rows.Count > 0)
                        {
                            ExcelFileHelper.GenerateExcelFile(wizardData, SyncedDate);

                        }
                    }
                    wizardData.TableName = "ACApproved";

                    int totalRecord = wizardData.Rows.Count;
                    int count = 0;

                    Console.WriteLine(String.Format("Total from spbigfileeirmaxid >> {0}", totalRecord.ToString()));
                    LogFileHelper.logList.Add(String.Format("Total from spbigfileeirmaxid >> {0}", totalRecord.ToString()));

                    if (wizardData.Rows.Count > 0)
                    {
                        foreach (DataRow row in wizardData.Rows)
                        {
                            SqlTransaction Transaction = default(SqlTransaction);
                            Transaction = Connection.BeginTransaction("WizardSync");
                            try
                            {
                                count += 1;
                                string FileID = row["FileID"].ToString();
                                string SubmitType = row["SubmitType"].ToString();

                                //if (FileID.ToUpper().Trim() == "CS/3/10539" || FileID.ToUpper().Trim() == "CS/3/10567"
                                //    || FileID.ToUpper().Trim() == "CS/3/10591" || FileID.ToUpper().Trim() == "CS/3/10597"
                                //    || FileID.ToUpper().Trim() == "CS/3/10603" || FileID.ToUpper().Trim() == "CS/3/10609"
                                //    || FileID.ToUpper().Trim() == "CS/3/10619" || FileID.ToUpper().Trim() == "CS/3/10704")
                                //{
                                switch (SubmitType)
                                {
                                    #region "NEWLY AWARDED COMPANY"
                                    case "S":
                                        //Console.WriteLine("HIT THE BREAKPOINT and Line No : " + count);
                                        Console.WriteLine(string.Format("[{0}] : New ACApprovedAccount, FileID: {1}", DateTime.Now.ToString(), FileID));
                                        LogFileHelper.logList.Add(string.Format("[{0}] : New ACApprovedAccount , FileID {1}: ", DateTime.Now.ToString(), FileID));
                                        Nullable<int> MeetingNo = SyncHelper.ConvertToInteger(row["MeetingNo"].ToString());
                                        Guid? AccountID = GetAccountIDByFileID(Connection, Transaction, FileID);

                                        int OperationalStatus = 0;

                                        try
                                        {
                                            //Get OperationalStatus enum, Default to Null if not found
                                            string Code = SyncHelper.GetMappingValue_Wizard(Connection, Transaction, "OperationalStatus", row["OperationalStatus"].ToString());
                                            OperationalStatus = Convert.ToInt32((EnumSync.OperationalStatus)Enum.Parse(typeof(EnumSync.OperationalStatus), Code));

                                            if (!AccountID.HasValue)
                                            {
                                                AccountID = Guid.NewGuid();
                                                ACApproved_CreateAccount(Connection, Transaction, AccountID, row["CompanyName"].ToString(), row["ROCNumber"].ToString(), FileID, OperationalStatus);
                                                Console.WriteLine(string.Format("DONE Created Account: {0}", AccountID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :Created Account : {1}", DateTime.Now.ToString(), AccountID));
                                            }

                                            if (MeetingNo.HasValue && !IsUpdated(Connection, Transaction, MeetingNo.Value, FileID, SubmitType))
                                            //if (MeetingNo.HasValue)
                                            {
                                                Console.WriteLine(FileID);
                                                LogFileHelper.logList.Add(FileID);
                                                string AccountName = row["CompanyName"].ToString();
                                                string CompanyRegNo = row["ROCNumber"].ToString();
                                                Nullable<DateTime> DateOfIncorporation = SyncHelper.ConvertStringToDateTime(row["ROCDate"].ToString());
                                                Nullable<int> RequirementSpace = null;
                                                int myInt;
                                                bool isNumerical = int.TryParse(row["RequirementSpace"].ToString(), out myInt);
                                                if (isNumerical)
                                                    RequirementSpace = Convert.ToInt32(row["RequirementSpace"]);
                                                else
                                                    RequirementSpace = null;
                                                string PlanMoveTo = string.Empty;
                                                if (row["PlanToMoveTo"] == null)
                                                    PlanMoveTo = string.Empty;
                                                else
                                                    PlanMoveTo = row["PlanToMoveTo"].ToString();
                                                Decimal Acc5YearsTax = SyncHelper.ConvertToDecimal(row["CumulativeTaxLoss"].ToString());
                                                string CoreActivities = string.Empty;
                                                if (row["CoreActivities"] == null)
                                                    CoreActivities = string.Empty;
                                                else
                                                    CoreActivities = row["CoreActivities"].ToString();
                                                string LeadGenerator = string.Empty;
                                                if (row["LeadGenerator"] == null || row["LeadGenerator"].ToString() == "")
                                                    LeadGenerator = "Direct Client – Website";
                                                else
                                                    LeadGenerator = row["LeadGenerator"].ToString();
                                                Nullable<DateTime> LeadSubmitDate = SyncHelper.ConvertStringToDateTime(row["LeadSubmitDate"].ToString());
                                                string BusinessPhoneCountryCode = row["PhCountryCode"].ToString();
                                                string BusinessPhoneAC = row["PhCalAreaCode"].ToString();
                                                string BusinessPhoneSC = row["PhAreaCode"].ToString();
                                                string BusinessPhoneISD = row["PhISDCode"].ToString();
                                                string BusinessPhone = row["PhoneNumber"].ToString();
                                                string BusinessPhoneExt = row["PhExtension"].ToString();
                                                string FaxCountryCode = row["FaxCountryCode"].ToString();
                                                string FaxSC = row["FaxAreaCode"].ToString();
                                                string FaxCC = row["FaxISDCode"].ToString();
                                                string Fax = row["Fax"].ToString();
                                                string WebSiteUrl = row["URL"].ToString();
                                                Console.WriteLine(string.Format("[{0}] :1- DONE get Parameter for Contacts", DateTime.Now.ToString()));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :1- DONE get Parameter for Contacts", DateTime.Now.ToString()));

                                                Guid? AccountTypeCID = SyncHelper.GetCodeMasterID(Connection, Transaction, "Pending Verification", BOL.AppConst.CodeType.AccountType, true);
                                                Console.WriteLine(string.Format("2 - DONE getAccountTypeCID: {0}", AccountTypeCID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :2- DONE getAccountTypeCID", DateTime.Now.ToString()));

                                                Guid? FinancialIncentiveCID = null;
                                                if (!string.IsNullOrEmpty(row["FinancialIncentive"].ToString()))
                                                {
                                                    FinancialIncentiveCID = SyncHelper.GetCodeMasterID(Connection, Transaction, row["FinancialIncentive"].ToString(), BOL.AppConst.CodeType.FinancialIncentive, true);
                                                }

                                                Guid? AccountCategoryCID = null;
                                                if (!string.IsNullOrEmpty(row["Stage"].ToString()))
                                                {
                                                    AccountCategoryCID = SyncHelper.GetCodeMasterID(Connection, Transaction, row["Stage"].ToString(), BOL.AppConst.CodeType.AccountCategory, true);
                                                }
                                                Console.WriteLine(string.Format("3 - DONE AccountCategoryCID: {0} and FinancialIncentiveCID {1}", AccountCategoryCID, FinancialIncentiveCID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :3-  DONE AccountCategoryCID: {1} and FinancialIncentiveCID {2}", DateTime.Now.ToString(), AccountCategoryCID, FinancialIncentiveCID));

                                                //Insert Draft MSCStatusHistory if not exists
                                                Guid? Draft_MSCApprovalStatusCID = SyncHelper.GetCodeMasterID(Connection, Transaction, "Draft", BOL.AppConst.CodeType.MSCApprovalStatus, true);
                                                DateTime? FirstDFTDate = SyncHelper.ConvertStringToDateTime(row["FirstDFTDate"].ToString(), false);
                                                if (FirstDFTDate.HasValue)
                                                {
                                                    SyncMSCStatusHistory mgr = new SyncMSCStatusHistory();
                                                    Nullable<Guid> MSCStatusHistoryID = mgr.GetMSCStatusHistoryID(Connection, Transaction, AccountID, Draft_MSCApprovalStatusCID);
                                                    if (!MSCStatusHistoryID.HasValue)
                                                    {
                                                        mgr.CreateMSCStatusHistory(Connection, Transaction, AccountID, Draft_MSCApprovalStatusCID, FirstDFTDate);
                                                        Console.WriteLine(string.Format("DONE Created Draft_MSCApprovalStatusCID: {0} ", Draft_MSCApprovalStatusCID));
                                                        LogFileHelper.logList.Add(string.Format("[{0}] :DONE Created Draft_MSCApprovalStatusCID: {1}", DateTime.Now.ToString(), Draft_MSCApprovalStatusCID));
                                                    }
                                                    else
                                                    {
                                                        mgr.UpdateMSCStatusHistory(Connection, Transaction, AccountID, MSCStatusHistoryID, FirstDFTDate);
                                                        Console.WriteLine(string.Format("DONE Updated Draft_MSCApprovalStatusCID: {0} ", Draft_MSCApprovalStatusCID));
                                                        LogFileHelper.logList.Add(string.Format("[{0}] :DONE Updated Draft_MSCApprovalStatusCID: {1}", DateTime.Now.ToString(), Draft_MSCApprovalStatusCID));
                                                    }
                                                }


                                                //Insert AC Approved MSCStatusHistory if not exists
                                                Guid? ACApproved_MSCApprovalStatusCID = SyncHelper.GetCodeMasterID(Connection, Transaction, "AC Meeting", BOL.AppConst.CodeType.MSCApprovalStatus, true);
                                                DateTime? ACApprovedDate = SyncHelper.ConvertStringToDateTime(row["DateOfApproval"].ToString(), false);
                                                if (ACApprovedDate.HasValue)
                                                {
                                                    SyncMSCStatusHistory mgr = new SyncMSCStatusHistory();
                                                    Nullable<Guid> MSCStatusHistoryID = mgr.GetMSCStatusHistoryID(Connection, Transaction, AccountID, ACApproved_MSCApprovalStatusCID);
                                                    if (!MSCStatusHistoryID.HasValue)
                                                    {
                                                        mgr.CreateMSCStatusHistory(Connection, Transaction, AccountID, ACApproved_MSCApprovalStatusCID, ACApprovedDate);
                                                        Console.WriteLine(string.Format("DONE Created ACApproved_MSCApprovalStatusCID: {0} ", ACApproved_MSCApprovalStatusCID));
                                                        LogFileHelper.logList.Add(string.Format("[{0}] :DONE Created ACApproved_MSCApprovalStatusCID: {1}", DateTime.Now.ToString(), ACApproved_MSCApprovalStatusCID));
                                                    }
                                                    else
                                                    {
                                                        mgr.UpdateMSCStatusHistory(Connection, Transaction, AccountID, MSCStatusHistoryID, ACApprovedDate);
                                                        Console.WriteLine(string.Format("DONE Updated ACApproved_MSCApprovalStatusCID: {0} ", ACApproved_MSCApprovalStatusCID));
                                                        LogFileHelper.logList.Add(string.Format("[{0}] :DONE Updated ACApproved_MSCApprovalStatusCID: {1}", DateTime.Now.ToString(), ACApproved_MSCApprovalStatusCID));
                                                    }
                                                }
                                                Console.WriteLine(string.Format("4 - DONE Create/Update MSCStatusHistory table"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :4 - DONE Create/Update MSCStatusHistory table", DateTime.Now.ToString()));


                                                //Insert/Update Relocation Deadline(NEW COMPANY)
                                                Guid? RelocationStatus_Under6MonthsGracePeriod = SyncHelper.GetCodeMasterID(Connection, Transaction, "Under 6 Months Grace Period", BOL.AppConst.CodeType.RelocationStatus, true);
                                                Guid? RelocationStatus_Exemption = SyncHelper.GetCodeMasterID(Connection, Transaction, "Exemption", BOL.AppConst.CodeType.RelocationStatus, true);
                                                DateTime? RelocationDeadline = Convert.ToDateTime(ACApprovedDate).AddMonths(6);
                                                if (RelocationDeadline.HasValue)
                                                {
                                                    SyncRelocationStatus Relocation = new SyncRelocationStatus();
                                                    Guid? RelocationID = Relocation.GetRelocationStatusID(Connection, Transaction, AccountID, RelocationStatus_Under6MonthsGracePeriod);
                                                    if (RelocationID.HasValue)
                                                    {
                                                        //(NEW COMPANY)
                                                        Relocation.UpdateRelocationDeadline(Connection, Transaction, AccountID, RelocationID, RelocationDeadline);
                                                        Console.WriteLine(string.Format("DONE Update Relocation Deadline, RelocationID {0}", RelocationID));
                                                        LogFileHelper.logList.Add(string.Format("[{0}] :Update Relocation Deadline, RelocationID {1}", DateTime.Now.ToString(), RelocationID));
                                                    }
                                                    else
                                                    {
                                                        if (row["MainCluster"].ToString().Trim().ToUpper() == "INCUBATOR" || row["MainCluster"].ToString().Trim().ToUpper() == "IHL")
                                                        {
                                                            //(NEW COMPANY)
                                                            Relocation.CreateRelocationDeadline(Connection, Transaction, AccountID, RelocationStatus_Exemption, null);
                                                            Console.WriteLine(string.Format("DONE Update Relocation Deadline, RelocationID {0}", RelocationID));
                                                            LogFileHelper.logList.Add(string.Format("[{0}] :Update Relocation Deadline, RelocationID {1}", DateTime.Now.ToString(), RelocationID));
                                                        }
                                                        else
                                                        {
                                                            //(NEW COMPANY)
                                                            Relocation.CreateRelocationDeadline(Connection, Transaction, AccountID, RelocationStatus_Under6MonthsGracePeriod, RelocationDeadline);
                                                            Console.WriteLine(string.Format("DONE Created Relocation Deadline, RelocationID {0}", RelocationID));
                                                            LogFileHelper.logList.Add(string.Format("[{0}] :DONE Created Relocation Deadline, RelocationID {1}", DateTime.Now.ToString(), RelocationID));
                                                        }
                                                    }
                                                }
                                                Console.WriteLine(string.Format("5 - DONE Insert/Update Relocation Deadline, RelocationID "));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :5 - Insert/Update Relocation Deadline", DateTime.Now.ToString()));


                                                // Insert/update FinancialAndWorkerForecast
                                                int? ProjectionYear = SyncHelper.ConvertToInteger(row["ProjectionYear"]);
                                                if (ProjectionYear.HasValue)
                                                {
                                                    for (int i = 1; i <= 5; i++)
                                                    {
                                                        Nullable<int> LocalKW = SyncHelper.ConvertToInteger(row["LocalKWYR" + i]);
                                                        Nullable<int> ForeignKW = SyncHelper.ConvertToInteger(row["ForeignKWYR" + i]);
                                                        Nullable<int> LocalWorker = SyncHelper.ConvertToInteger(row["TWYR" + i + "Local"]);
                                                        Nullable<int> ForeignWorker = SyncHelper.ConvertToInteger(row["TWYR" + i + "Foreign"]);
                                                        Nullable<decimal> Investment = SyncHelper.ConvertToDecimal(row["InvYR" + i]);
                                                        Nullable<decimal> RnDExpenditure = SyncHelper.ConvertToDecimal(row["RDExpYR" + i]);
                                                        Nullable<decimal> LocalSales = SyncHelper.ConvertToDecimal(row["LocalSalesYR" + i]);
                                                        Nullable<decimal> ExportSales = SyncHelper.ConvertToDecimal(row["ExportSalesYR" + i]);
                                                        Nullable<decimal> NetProfit = SyncHelper.ConvertToDecimal(row["NetProfitYR" + i]);
                                                        Nullable<decimal> CashFlow = SyncHelper.ConvertToDecimal(row["CashFlowYR" + i]);
                                                        Nullable<decimal> Asset = SyncHelper.ConvertToDecimal(row["AssetsYR" + i]);
                                                        Nullable<decimal> Equity = SyncHelper.ConvertToDecimal(row["EquityYR" + i]);
                                                        Nullable<decimal> Liabilities = SyncHelper.ConvertToDecimal(row["LiabilitiesYR" + i]);
                                                        ACApproved_CreateUpdateFinancialAndWorkerForecast(Connection, Transaction, AccountID, ProjectionYear, LocalKW, ForeignKW, LocalWorker, ForeignWorker, Investment, RnDExpenditure,
                                                        LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities);

                                                        ProjectionYear += 1;
                                                    }
                                                }

                                                Console.WriteLine(string.Format("6 - DONE Insert/Update FinancialAndWorkerForecast "));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :6 - DONE Insert/Update FinancialAndWorkerForecast, ", DateTime.Now.ToString()));

                                                //Delete & Create/Update ShareHolder
                                                DataTable dtShareHolder = GetShareHolder(Connection, Transaction, FileID);
                                                DateTime SyncDate = DateTime.Now;
                                                if ((dtShareHolder != null) & dtShareHolder.Rows.Count > 0)
                                                {
                                                    ACApproved_DeleteShareholder(Connection, Transaction, AccountID);
                                                    foreach (DataRow dr in dtShareHolder.Rows)
                                                    {
                                                        string ShareholderName = dr["OwnershipSHName"].ToString();
                                                        Nullable<decimal> Percentage = SyncHelper.ConvertToDecimal(dr["OwnershipPer"]);
                                                        bool BumiShare = SyncHelper.ConvertToBoolean(dr["OwnershipBumi"]);
                                                        Nullable<Guid> CountryRegionID = SyncHelper.GetRegionID(Connection, Transaction, dr["OwnershipCName"].ToString());
                                                        ACApproved_CreateUpdateShareholder(Connection, Transaction, AccountID, ShareholderName, Percentage, BumiShare, CountryRegionID, SyncDate);
                                                    }
                                                }
                                                Console.WriteLine(string.Format("7 - DONE Delete/Insert&Update ShareHolder "));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :7 - DONE Delete/Insert&Update ShareHolder, ", DateTime.Now.ToString()));

                                                //Insert Account Cluster
                                                Nullable<Guid> ClusterID = null;
                                                string Cluster = SyncHelper.GetMappingValue_Wizard(Connection, Transaction, "Cluster", row["MainCluster"].ToString());

                                                if (!string.IsNullOrEmpty(Cluster))
                                                {
                                                    ClusterID = SyncHelper.GetSubClusterID_Wizard(Connection, Transaction, Cluster);
                                                }
                                                Console.WriteLine(string.Format("8 - DONE get Cluster: {0}, and ClusterID: {1} ", Cluster, ClusterID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :8 - DONE get Cluster: {1}, and ClusterID: {2}, ", DateTime.Now.ToString(), Cluster, ClusterID));

                                                if (ClusterID.HasValue)
                                                {
                                                    //(NEW COMPANY)
                                                    ACApproved_CreateAccountCluster(Connection, Transaction, AccountID, ClusterID);
                                                }

                                                //Insert AccountManagerAssignment
                                                if (!string.IsNullOrEmpty(row["BusinestAnalyst"].ToString()))
                                                {
                                                    ACApproved_CreateUpdateBusinessAnalystAssignment(Connection, Transaction, AccountID, row["BusinestAnalyst"].ToString(), ACApprovedDate);
                                                }
                                                Console.WriteLine(string.Format("9 - DONE Insert AccountManagerAssignment-Business Analyst"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :9 - DONE Insert AccountManagerAssignment-Business Analyst", DateTime.Now.ToString()));


                                                #region Insert/Update Account Address
                                                for (int i = 1; i <= 2; i++)
                                                {
                                                    if (!string.IsNullOrEmpty(row["Add" + i + "AddessL1"].ToString()))
                                                    {
                                                        if (i == 1)
                                                        {
                                                            bool flagSttCty = true;
                                                            string DefaultCountry = SyncHelper.GetRegionID(Connection, Transaction, row["Add" + i + "CountryDesc"].ToString()).ToString();
                                                            Console.WriteLine(string.Format("10 - DONE DefaultCountry"));
                                                            LogFileHelper.logList.Add(string.Format("[{0}] :10 - DONE get DefaultCountry : {1}", DateTime.Now.ToString(), DefaultCountry));
                                                            if (DefaultCountry.ToUpper().Equals(BOL.Common.Modules.Parameter.DEFAULT_COUNTRY.ToUpper()))
                                                            {
                                                                if (string.IsNullOrEmpty(SyncHelper.GetCity(Connection, Transaction, row["Add" + i + "City"].ToString())) & string.IsNullOrEmpty(SyncHelper.GetState(row["Add" + i + "StateDesc"].ToString())))
                                                                {
                                                                    flagSttCty = false;
                                                                    throw new Exception("City and State cannot be NULL for this MSCFileID : " + FileID);
                                                                    LogFileHelper.logList.Add(string.Format("[{0}] :City and State cannot be NULL for this MSCFileID : " + FileID, DateTime.Now.ToString(), row["Add" + i + "StateDesc"].ToString()));
                                                                }
                                                            }
                                                            Console.WriteLine(string.Format("11 - DONE get City"));
                                                            LogFileHelper.logList.Add(string.Format("[{0}] :11 - DONE get City : {1}", DateTime.Now.ToString(), row["Add" + i + "StateDesc"].ToString()));
                                                            if (flagSttCty)
                                                            {
                                                                ACApproved_CreateUpdateAccountAddress(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "Headquarters", BOL.AppConst.CodeType.AddressType, true),
                                                                    row["Add" + i + "AddessL1"].ToString(), row["Add" + i + "AddessL2"].ToString(), row["Add" + i + "AddessL3"].ToString(), SyncHelper.GetCity(Connection, Transaction,
                                                                    row["Add" + i + "City"].ToString()), row["Add" + i + "PostCode"].ToString(), SyncHelper.GetState(row["Add" + i + "StateDesc"].ToString()),
                                                                SyncHelper.GetRegionID(Connection, Transaction, row["Add" + i + "CountryDesc"].ToString()), BusinessPhoneCountryCode, BusinessPhoneSC, BusinessPhoneAC, BusinessPhoneISD, BusinessPhone, BusinessPhoneExt, FaxCountryCode, FaxSC, FaxCC,
                                                                Fax);
                                                                Console.WriteLine(string.Format("12 - DONE CreateUpdateAccountAddress"));
                                                                LogFileHelper.logList.Add(string.Format("[{0}] :12 - DONE CreateUpdateAccountAddress, AccountID: {1}", DateTime.Now.ToString(), AccountID));
                                                            }
                                                        }
                                                        else
                                                        {
                                                            bool flagSttCty = true;
                                                            string DefaultCountry = SyncHelper.GetRegionID(Connection, Transaction, row["Add" + i + "CountryDesc"].ToString()).ToString();

                                                            if (DefaultCountry.ToUpper().Equals(BOL.Common.Modules.Parameter.DEFAULT_COUNTRY.ToUpper()))
                                                            {
                                                                if (string.IsNullOrEmpty(SyncHelper.GetCity(Connection, Transaction, row["Add" + i + "City"].ToString())) & string.IsNullOrEmpty(SyncHelper.GetState(row["Add" + i + "StateDesc"].ToString())))
                                                                {
                                                                    flagSttCty = false;
                                                                    throw new Exception("City and State cannot be NULL for this MSCFileID : " + FileID);
                                                                    LogFileHelper.logList.Add(string.Format("[{0}] :City and State cannot be NULL for this MSCFileID : " + FileID, DateTime.Now.ToString(), row["Add" + i + "StateDesc"].ToString()));
                                                                }
                                                            }

                                                            if (flagSttCty)
                                                            {
                                                                ACApproved_CreateUpdateAccountAddress(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, row["Add" + i + "AddressType"].ToString(), BOL.AppConst.CodeType.AddressType, true),
                                                                    row["Add" + i + "AddessL1"].ToString(), row["Add" + i + "AddessL2"].ToString(), row["Add" + i + "AddessL3"].ToString(), SyncHelper.GetCity(Connection, Transaction, row["Add" + i + "City"].ToString()),
                                                                    row["Add" + i + "PostCode"].ToString(), SyncHelper.GetState(row["Add" + i + "StateDesc"].ToString()),
                                                                SyncHelper.GetRegionID(Connection, Transaction, row["Add" + i + "CountryDesc"].ToString()), null, null, null, null, null, null, null, null, null,
                                                                null);
                                                                Console.WriteLine(string.Format("12.1 - DONE CreateUpdateAccountAddress"));
                                                                LogFileHelper.logList.Add(string.Format("[{0}] :12.1 - DONE CreateUpdateAccountAddress, AccountID: {1}", DateTime.Now.ToString(), AccountID));
                                                            }
                                                        }
                                                    }
                                                }
                                                #endregion
                                                #region "CEO"
                                                //Insert/Update Account Contact
                                                if (!string.IsNullOrEmpty(row["CEO"].ToString()))
                                                {
                                                    string CEOHPCountryCode = null;
                                                    string CEOHPAreaCode = null;
                                                    string CEOHPNumber = null;
                                                    string CEOHPISDCode = null;
                                                    string CEOOPCountryCode = null;
                                                    string CEOOPAreaCode = null;
                                                    string CEOOPCalAreaCode = null;
                                                    string CEOOPNumber = null;
                                                    string CEOOPExtension = null;
                                                    string CEOOPISDCode = null;
                                                    string CEOFaxCountryCode = null;
                                                    string CEOFaxAreaCode = null;
                                                    string CEOFaxISDCode = null;
                                                    string CEOFaxNumber = null;

                                                    if (row["CEOHPCountryCode"] == null)
                                                        CEOHPCountryCode = string.Empty;
                                                    else
                                                        CEOHPCountryCode = row["CEOHPCountryCode"].ToString();
                                                    if (row["CEOHPAreaCode"] == null)
                                                        CEOHPAreaCode = string.Empty;
                                                    else
                                                        CEOHPAreaCode = row["CEOHPAreaCode"].ToString();
                                                    if (row["CEOHPNumber"] == null)
                                                        CEOHPNumber = string.Empty;
                                                    else
                                                        CEOHPNumber = row["CEOHPNumber"].ToString();
                                                    if (row["CEOHPISDCode"] == null)
                                                        CEOHPISDCode = string.Empty;
                                                    else
                                                        CEOHPISDCode = row["CEOHPISDCode"].ToString();
                                                    if (row["CEOOPCountryCode"] == null)
                                                        CEOOPCountryCode = string.Empty;
                                                    else
                                                        CEOOPCountryCode = row["CEOOPCountryCode"].ToString();
                                                    if (row["CEOOPAreaCode"] == null)
                                                        CEOOPAreaCode = string.Empty;
                                                    else
                                                        CEOOPAreaCode = row["CEOOPAreaCode"].ToString();
                                                    if (row["CEOOPCalAreaCode"] == null)
                                                        CEOOPCalAreaCode = string.Empty;
                                                    else
                                                        CEOOPCalAreaCode = row["CEOOPCalAreaCode"].ToString();
                                                    if (row["CEOOPNumber"] == null)
                                                        CEOOPNumber = string.Empty;
                                                    else
                                                        CEOOPNumber = row["CEOOPNumber"].ToString();
                                                    if (row["CEOOPExtension"] == null)
                                                        CEOOPExtension = string.Empty;
                                                    else
                                                        CEOOPExtension = row["CEOOPExtension"].ToString();
                                                    if (row["CEOOPISDCode"] == null)
                                                        CEOOPISDCode = string.Empty;
                                                    else
                                                        CEOOPISDCode = row["CEOOPISDCode"].ToString();
                                                    if (row["CEOFaxCountryCode"] == null)
                                                        CEOFaxCountryCode = string.Empty;
                                                    else
                                                        CEOFaxCountryCode = row["CEOFaxCountryCode"].ToString(); ;
                                                    if (row["CEOFaxAreaCode"] == null)
                                                        CEOFaxAreaCode = string.Empty;
                                                    else
                                                        CEOFaxAreaCode = row["CEOFaxAreaCode"].ToString();
                                                    if (row["CEOFaxISDCode"] == null)
                                                        CEOFaxISDCode = string.Empty;
                                                    else
                                                        CEOFaxISDCode = row["CEOFaxISDCode"].ToString();
                                                    if (row["CEOFaxNumber"] == null)
                                                        CEOFaxNumber = string.Empty;
                                                    else
                                                        CEOFaxNumber = row["CEOFaxNumber"].ToString();

                                                    ACApproved_CreateUpdateAccountContact(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "CEO", BOL.AppConst.CodeType.Designation, true), "CEO", row["CEO"].ToString(), row["EmailAddress1"].ToString(), CEOOPCountryCode, CEOOPAreaCode, CEOOPCalAreaCode,
                                                    CEOOPISDCode, CEOOPNumber, CEOOPExtension, CEOHPCountryCode, CEOHPISDCode, CEOHPNumber, CEOFaxCountryCode, CEOFaxAreaCode, CEOFaxISDCode, CEOFaxNumber);
                                                }
                                                Console.WriteLine(string.Format("CEO...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :CEO...Finished", DateTime.Now.ToString()));
                                                #endregion
                                                #region "CTO"
                                                if (!string.IsNullOrEmpty(row["CTO"].ToString()))
                                                {
                                                    ACApproved_CreateUpdateAccountContact(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "CTO", BOL.AppConst.CodeType.Designation, true), "CTO", row["CTO"].ToString(), row["EmailAddress2"].ToString());
                                                }
                                                Console.WriteLine(string.Format("CTO...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :CTO...Finished", DateTime.Now.ToString()));
                                                #endregion
                                                #region CFO                                    
                                                if (!string.IsNullOrEmpty(row["CFO"].ToString()))
                                                {
                                                    ACApproved_CreateUpdateAccountContact(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "CFO", BOL.AppConst.CodeType.Designation, true), "CFO", row["CFO"].ToString(), row["EmailAddress3"].ToString());
                                                }
                                                Console.WriteLine(string.Format("CFO...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :CFO...Finished", DateTime.Now.ToString()));
                                                #endregion
                                                #region "MD"
                                                if (!string.IsNullOrEmpty(row["MD"].ToString()))
                                                {
                                                    string MDHPCountryCode = null;
                                                    string MDHPAreaCode = null;
                                                    string MDHPNumber = null;
                                                    string MDHPISDCode = null;
                                                    string MDOPCountryCode = null;
                                                    string MDOPAreaCode = null;
                                                    string MDOPCalAreaCode = null;
                                                    string MDOPNumber = null;
                                                    string MDOPExtension = null;
                                                    string MDOPISDCode = null;
                                                    string MDFaxCountryCode = null;
                                                    string MDFaxAreaCode = null;
                                                    string MDFaxISDCode = null;
                                                    string MDFaxNumber = null;

                                                    if (row["MDHPCountryCode"] == null)
                                                        MDHPCountryCode = string.Empty;
                                                    else
                                                        MDHPCountryCode = row["MDHPCountryCode"].ToString();
                                                    if (row["MDHPAreaCode"] == null)
                                                        MDHPAreaCode = string.Empty;
                                                    else
                                                        MDHPAreaCode = row["MDHPAreaCode"].ToString();
                                                    if (row["MDHPNumber"] == null)
                                                        MDHPNumber = string.Empty;
                                                    else
                                                        MDHPNumber = row["MDHPNumber"].ToString();
                                                    if (row["MDHPISDCode"] == null)
                                                        MDHPISDCode = string.Empty;
                                                    else
                                                        MDHPISDCode = row["MDHPISDCode"].ToString();
                                                    if (row["MDOPCountryCode"] == null)
                                                        MDOPCountryCode = string.Empty;
                                                    else
                                                        MDOPCountryCode = row["MDOPCountryCode"].ToString();
                                                    if (row["MDOPAreaCode"] == null)
                                                        MDOPAreaCode = string.Empty;
                                                    else
                                                        MDOPAreaCode = row["MDOPAreaCode"].ToString();
                                                    if (row["MDOPCalAreaCode"] == null)
                                                        MDOPCalAreaCode = string.Empty;
                                                    else
                                                        MDOPCalAreaCode = row["MDOPCalAreaCode"].ToString();
                                                    if (row["MDOPNumber"] == null)
                                                        MDOPNumber = string.Empty;
                                                    else
                                                        MDOPNumber = row["MDOPNumber"].ToString();
                                                    if (row["MDOPExtension"] == null)
                                                        MDOPExtension = string.Empty;
                                                    else
                                                        MDOPExtension = row["MDOPExtension"].ToString();
                                                    if (row["MDOPISDCode"] == null)
                                                        MDOPISDCode = string.Empty;
                                                    else
                                                        MDOPISDCode = row["MDOPISDCode"].ToString();
                                                    if (row["MDFaxCountryCode"] == null)
                                                        MDFaxCountryCode = string.Empty;
                                                    else
                                                        MDFaxCountryCode = row["MDFaxCountryCode"].ToString();
                                                    if (row["MDFaxAreaCode"] == null)
                                                        MDFaxAreaCode = string.Empty;
                                                    else
                                                        MDFaxAreaCode = row["MDFaxAreaCode"].ToString();
                                                    if (row["MDFaxISDCode"] == null)
                                                        MDFaxISDCode = string.Empty;
                                                    else
                                                        MDFaxISDCode = row["MDFaxISDCode"].ToString();
                                                    if (row["MDFaxNumber"] == null)
                                                        MDFaxNumber = string.Empty;
                                                    else
                                                        MDFaxNumber = row["MDFaxNumber"].ToString();

                                                    ACApproved_CreateUpdateAccountContact(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "MD", BOL.AppConst.CodeType.Designation, true), "MD", row["MD"].ToString(), row["EmailAddress4"].ToString(), MDOPCountryCode, MDOPAreaCode, MDOPCalAreaCode,
                                                    MDOPISDCode, MDOPNumber, MDOPExtension, MDHPCountryCode, MDHPISDCode, MDHPNumber, MDFaxCountryCode, MDFaxAreaCode, MDFaxISDCode, MDFaxNumber);
                                                }
                                                Console.WriteLine(string.Format("MD...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :MD...Finished", DateTime.Now.ToString()));
                                                #endregion
                                                #region "OTHERS"
                                                if (!string.IsNullOrEmpty(row["Others"].ToString()))
                                                {
                                                    string OthHPCountryCode = null;
                                                    string OthHPAreaCode = null;
                                                    string OthHPNumber = null;
                                                    string OthHPISDCode = null;
                                                    string OthOPCountryCode = null;
                                                    string OthOPAreaCode = null;
                                                    string OthOPCalAreaCode = null;
                                                    string OthOPNumber = null;
                                                    string OthOPExtension = null;
                                                    string OthOPISDCode = null;
                                                    string OthFaxCountryCode = null;
                                                    string OthFaxAreaCode = null;
                                                    string OthFaxISDCode = null;
                                                    string OthFaxNumber = null;

                                                    if (row["OthHPCountryCode"] == null)
                                                        OthHPCountryCode = string.Empty;
                                                    else
                                                        OthHPCountryCode = row["OthHPCountryCode"].ToString();
                                                    if (row["OthHPAreaCode"] == null)
                                                        OthHPAreaCode = string.Empty;
                                                    else
                                                        OthHPAreaCode = row["OthHPAreaCode"].ToString();
                                                    if (row["OthHPNumber"] == null)
                                                        OthHPNumber = string.Empty;
                                                    else
                                                        OthHPNumber = row["OthHPNumber"].ToString();
                                                    if (row["OthHPISDCode"] == null)
                                                        OthHPISDCode = string.Empty;
                                                    else
                                                        OthHPISDCode = row["OthHPISDCode"].ToString();
                                                    if (row["OthOPCountryCode"] == null)
                                                        OthOPCountryCode = string.Empty;
                                                    else
                                                        OthOPCountryCode = row["OthOPCountryCode"].ToString();
                                                    if (row["OthOPAreaCode"] == null)
                                                        OthOPAreaCode = string.Empty;
                                                    else
                                                        OthOPAreaCode = row["OthOPAreaCode"].ToString();
                                                    if (row["OthOPCalAreaCode"] == null)
                                                        OthOPCalAreaCode = string.Empty;
                                                    else
                                                        OthOPCalAreaCode = row["OthOPCalAreaCode"].ToString();
                                                    if (row["OthOPNumber"] == null)
                                                        OthOPNumber = string.Empty;
                                                    else
                                                        OthOPNumber = row["OthOPNumber"].ToString();
                                                    if (row["OthOPExtension"] == null)
                                                        OthOPExtension = string.Empty;
                                                    else
                                                        OthOPExtension = row["OthOPExtension"].ToString();
                                                    if (row["OthOPISDCode"] == null)
                                                        OthOPISDCode = string.Empty;
                                                    else
                                                        OthOPISDCode = row["OthOPISDCode"].ToString();
                                                    if (row["OthFaxCountryCode"] == null)
                                                        OthFaxCountryCode = string.Empty;
                                                    else
                                                        OthFaxCountryCode = row["OthFaxCountryCode"].ToString();
                                                    if (row["OthFaxAreaCode"] == null)
                                                        OthFaxAreaCode = string.Empty;
                                                    else
                                                        OthFaxAreaCode = row["OthFaxAreaCode"].ToString();
                                                    if (row["OthFaxISDCode"] == null)
                                                        OthFaxISDCode = string.Empty;
                                                    else
                                                        OthFaxISDCode = row["OthFaxISDCode"].ToString();
                                                    if (row["OthFaxNumber"] == null)
                                                        OthFaxNumber = string.Empty;
                                                    else
                                                        OthFaxNumber = row["OthFaxNumber"].ToString();

                                                    ACApproved_CreateUpdateAccountContact(Connection, Transaction, AccountID, SyncHelper.GetCodeMasterID(Connection, Transaction, "Others", BOL.AppConst.CodeType.Designation, true), "Others", row["Others"].ToString(), row["EmailAddress5"].ToString(), OthOPCountryCode, OthOPAreaCode, OthOPCalAreaCode,
                                                    OthOPISDCode, OthOPNumber, OthOPExtension, OthHPCountryCode, OthHPISDCode, OthHPNumber, OthFaxCountryCode, OthFaxAreaCode, OthFaxISDCode, OthFaxNumber);
                                                }
                                                Console.WriteLine(string.Format("OTHERS...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :OTHERS...Finished", DateTime.Now.ToString()));
                                                #endregion

                                                // Insert/update Relocation(NEW COMPANY)
                                                ACApproved_CreateUpdateAccountRelocation(Connection, Transaction, AccountID);
                                                Console.WriteLine(string.Format("Create/Update Relocation...Finished"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :Create/Update Relocation...Finished", DateTime.Now.ToString()));


                                                //Update Account
                                                ACApproved_UpdateAccount(Connection, Transaction, AccountID, AccountTypeCID, AccountName, CompanyRegNo, DateOfIncorporation, OperationalStatus, RequirementSpace, PlanMoveTo,
                                                FinancialIncentiveCID, Acc5YearsTax, CoreActivities, LeadGenerator, LeadSubmitDate, BusinessPhoneCountryCode, BusinessPhoneSC, BusinessPhoneAC, BusinessPhoneISD, BusinessPhone,
                                                BusinessPhoneExt, FaxCountryCode, FaxSC, FaxCC, Fax, WebSiteUrl, AccountCategoryCID, ACApprovedDate, string.Empty, string.Empty,
                                                string.Empty, SubmitType);
                                                Console.WriteLine(string.Format("Update Account...Finished , AccountID : {0}", AccountID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :Update Account, AccountID : {1}", DateTime.Now.ToString(), AccountID));

                                                //Create ACApprovedAccountHistory
                                                DataTable xmlDt = wizardData.Clone();
                                                DataRow xmlRow = xmlDt.NewRow();

                                                for (int i = 0; i <= xmlDt.Columns.Count - 1; i++)
                                                {
                                                    xmlRow[i] = row[i];
                                                }

                                                xmlDt.Rows.Add(xmlRow);

                                                StringWriter writer = new StringWriter();
                                                xmlDt.WriteXml(writer);

                                                CreateACApprovedAccountHistory(Connection, Transaction, MeetingNo, FileID, writer.ToString());
                                                Console.WriteLine(string.Format("DONE -Create ACApprovedAccountHistory"));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :DONE -Create ACApprovedAccountHistory, MeetingNo: {1} & FileID: {2}", DateTime.Now.ToString(), MeetingNo, FileID));

                                                //Aryo 20120112 to Insert into MSCChangesHistory
                                                Guid? SubmitTypeID = default(Guid);
                                                BOL.AccountContact.odsAccount mgrSubmitType = new BOL.AccountContact.odsAccount();

                                                SubmitTypeID = mgrSubmitType.GetSubmitType_Wizard(Connection, Transaction, BOL.AppConst.SubmitType.S);
                                                CreateMSCChangesHistory(Connection, Transaction, AccountID, SubmitTypeID);
                                                Console.WriteLine(string.Format("DONE - Create MSCChangesHistory, AccountID: {0} & SubmitTypeID{1} ", AccountID, SubmitTypeID));
                                                LogFileHelper.logList.Add(string.Format("[{0}] :DONE - Create MSCChangesHistory, AccountID: {1} & SubmitTypeID{2}", DateTime.Now.ToString(), AccountID, SubmitTypeID));

                                                Console.WriteLine(string.Format("[{0}] {2}/{3} : Insert/Update AC Approved Account FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));

                                                break;

                                            }
                                            else
                                            {
                                                Console.WriteLine(String.Format("[{0}] {2}/{3} : Skip Record FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            OperationalStatus = 0;
                                            Transaction.Rollback();
                                            isRollBack = true;
                                        }

                                        break;
                                    #endregion
                                    #region "POST MSC COMP CHANGES"
                                    case "A":
                                    case "P":
                                    case "E":
                                    case "N":
                                        MeetingNo = SyncHelper.ConvertToInteger(row["MeetingNo"]);
                                        AccountID = GetAccountIDByFileID(Connection, Transaction, FileID);

                                        if (AccountID.HasValue)
                                        {
                                            if (MeetingNo.HasValue && !IsUpdated(Connection, Transaction, MeetingNo.Value, FileID, SubmitType))
                                            //if (MeetingNo.HasValue)
                                            {
                                                DataRow AccountDetails = GetAccountDetailsNameByAccountID(Connection, Transaction, AccountID);

                                                Guid? SubmitTypeID = default(Guid);
                                                BOL.AccountContact.odsAccount mgrSubmitType = new BOL.AccountContact.odsAccount();
                                                #region "GET SUBMIT TYPE ID"
                                                Console.WriteLine("507");
                                                switch ((SubmitType))
                                                {
                                                    case "A":
                                                        SubmitTypeID = mgrSubmitType.GetSubmitType_Wizard(Connection, Transaction, BOL.AppConst.SubmitType.A);
                                                        break;
                                                    case "P":
                                                        SubmitTypeID = mgrSubmitType.GetSubmitType_Wizard(Connection, Transaction, BOL.AppConst.SubmitType.P);
                                                        break;
                                                    case "E":
                                                        SubmitTypeID = mgrSubmitType.GetSubmitType_Wizard(Connection, Transaction, BOL.AppConst.SubmitType.E);
                                                        break;
                                                    case "N":
                                                        SubmitTypeID = mgrSubmitType.GetSubmitType_Wizard(Connection, Transaction, BOL.AppConst.SubmitType.N);
                                                        break;
                                                }
                                                #endregion
                                                Console.WriteLine(FileID);
                                                if (AccountDetails != null)
                                                {
                                                    Guid AccountDVID = Guid.NewGuid();
                                                    CreateAccountDV(Connection, Transaction, SubmitTypeID, AccountDVID, AccountID);

                                                    string AccountName = AccountDetails["AccountName"].ToString();

                                                    if ("N".Equals(SubmitType))
                                                    {
                                                        AccountName = row["CompanyName"].ToString() + " (Formerly known as " + AccountName + ")";
                                                    }

                                                    Nullable<DateTime> ACApprovedDate = SyncHelper.ConvertStringToDateTime(row["DateOfApproval"].ToString(), false);
                                                    string CoreActivities = AccountDetails["CoreActivities"].ToString();
                                                    string BusinessPhoneCountryCode = row["PhCountryCode"].ToString();
                                                    string BusinessPhoneAC = row["PhCalAreaCode"].ToString();
                                                    string BusinessPhoneSC = row["PhAreaCode"].ToString();
                                                    //Dim BusinessPhoneCC As String = MergeField(PhCountryCode, PhAreaCode, ";")
                                                    string BusinessPhoneISD = row["PhISDCode"].ToString();
                                                    string BusinessPhone = row["PhoneNumber"].ToString();
                                                    string BusinessPhoneExt = row["PhExtension"].ToString();
                                                    string FaxCountryCode = row["FaxCountryCode"].ToString();
                                                    string FaxCC = row["FaxISDCode"].ToString();
                                                    string FaxSC = row["FaxAreaCode"].ToString();
                                                    string Fax = row["Fax"].ToString();
                                                    string WebSiteUrl = row["URL"].ToString();
                                                    OperationalStatus = 0;
                                                    try
                                                    {
                                                        //Get OperationalStatus enum, Default to Null if not found
                                                        OperationalStatus = 6;
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        OperationalStatus = 0;
                                                    }
                                                    #region "A" OR "P"
                                                    if ("A".Equals(SubmitType) | "P".Equals(SubmitType))
                                                    {
                                                        CoreActivities = row["CoreActivities"].ToString();
                                                        Console.WriteLine("553");
                                                        // Insert/update FinancialAndWorkerForecast
                                                        int? ProjectionYear = SyncHelper.ConvertToInteger(row["ProjectionYear"]);
                                                        if (ProjectionYear.HasValue)
                                                        {

                                                            for (int i = 1; i <= 5; i++)
                                                            {
                                                                Nullable<int> LocalKW = SyncHelper.ConvertToInteger(row["LocalKWYR" + i]);
                                                                Nullable<int> ForeignKW = SyncHelper.ConvertToInteger(row["ForeignKWYR" + i]);
                                                                Nullable<int> LocalWorker = SyncHelper.ConvertToInteger(row["TWYR" + i + "Local"]);
                                                                Nullable<int> ForeignWorker = SyncHelper.ConvertToInteger(row["TWYR" + i + "Foreign"]);
                                                                Nullable<decimal> Investment = SyncHelper.ConvertToDecimal(row["InvYR" + i]);
                                                                Nullable<decimal> RnDExpenditure = SyncHelper.ConvertToDecimal(row["RDExpYR" + i]);
                                                                Nullable<decimal> LocalSales = SyncHelper.ConvertToDecimal(row["LocalSalesYR" + i]);
                                                                Nullable<decimal> ExportSales = SyncHelper.ConvertToDecimal(row["ExportSalesYR" + i]);
                                                                Nullable<decimal> NetProfit = SyncHelper.ConvertToDecimal(row["NetProfitYR" + i]);
                                                                Nullable<decimal> CashFlow = SyncHelper.ConvertToDecimal(row["CashFlowYR" + i]);
                                                                Nullable<decimal> Asset = SyncHelper.ConvertToDecimal(row["AssetsYR" + i]);
                                                                Nullable<decimal> Equity = SyncHelper.ConvertToDecimal(row["EquityYR" + i]);
                                                                Nullable<decimal> Liabilities = SyncHelper.ConvertToDecimal(row["LiabilitiesYR" + i]);

                                                                ACApproved_CreateUpdateFinancialAndWorkerForecastDV(Connection, Transaction, AccountDVID, ProjectionYear, LocalKW, ForeignKW, LocalWorker, ForeignWorker, Investment, RnDExpenditure,
                                                                 LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities);

                                                                ProjectionYear += 1;
                                                            }
                                                        }
                                                    }
                                                    #endregion
                                                    #region "Changed of EQUITY"
                                                    if ("E".Equals(SubmitType))
                                                    {
                                                        // Insert/Update Shareholder

                                                        DataTable dtShareHolder = GetShareHolder(Connection, Transaction, FileID);
                                                        if (dtShareHolder.Rows.Count > 0)
                                                        {
                                                            foreach (DataRow dr in dtShareHolder.Rows)
                                                            {
                                                                string ShareholderName = dr["OwnershipSHName"].ToString();
                                                                Nullable<decimal> Percentage = SyncHelper.ConvertToDecimal(dr["OwnershipPer"]);
                                                                bool BumiShare = SyncHelper.ConvertToBoolean(dr["OwnershipBumi"]);
                                                                Nullable<Guid> CountryRegionID = SyncHelper.GetRegionID(Connection, Transaction, dr["OwnershipCName"].ToString());
                                                                ACApproved_CreateUpdateShareholderDV(Connection, Transaction, AccountDVID, ShareholderName, Percentage, BumiShare, CountryRegionID);
                                                            }
                                                        }

                                                    }
                                                    #endregion
                                                    #region AccountManagerAssignment
                                                    if (!string.IsNullOrEmpty(row["BusinestAnalyst"].ToString()))
                                                    {
                                                        ACApproved_CreateUpdateBusinessAnalystAssignmentDV(Connection, Transaction, AccountDVID, row["BusinestAnalyst"].ToString(), ACApprovedDate);
                                                    }
                                                    #endregion
                                                    #region "CEO"
                                                    if (!string.IsNullOrEmpty(row["CEO"].ToString()))
                                                    {
                                                        string CEOHPCountryCode = null;
                                                        string CEOHPAreaCode = null;
                                                        string CEOHPNumber = null;
                                                        string CEOHPISDCode = null;
                                                        string CEOOPCountryCode = null;
                                                        string CEOOPAreaCode = null;
                                                        string CEOOPCalAreaCode = null;
                                                        string CEOOPNumber = null;
                                                        string CEOOPExtension = null;
                                                        string CEOOPISDCode = null;
                                                        string CEOFaxCountryCode = null;
                                                        string CEOFaxAreaCode = null;
                                                        string CEOFaxISDCode = null;
                                                        string CEOFaxNumber = null;

                                                        if (row["CEOHPCountryCode"] == null)
                                                            CEOHPCountryCode = string.Empty;
                                                        else
                                                            CEOHPCountryCode = row["CEOHPCountryCode"].ToString();
                                                        if (row["CEOHPAreaCode"] == null)
                                                            CEOHPAreaCode = string.Empty;
                                                        else
                                                            CEOHPAreaCode = row["CEOHPAreaCode"].ToString();
                                                        if (row["CEOHPNumber"] == null)
                                                            CEOHPNumber = string.Empty;
                                                        else
                                                            CEOHPNumber = row["CEOHPNumber"].ToString();
                                                        if (row["CEOHPISDCode"] == null)
                                                            CEOHPISDCode = string.Empty;
                                                        else
                                                            CEOHPISDCode = row["CEOHPISDCode"].ToString();
                                                        if (row["CEOOPCountryCode"] == null)
                                                            CEOOPCountryCode = string.Empty;
                                                        else
                                                            CEOOPCountryCode = row["CEOOPCountryCode"].ToString();
                                                        if (row["CEOOPAreaCode"] == null)
                                                            CEOOPAreaCode = string.Empty;
                                                        else
                                                            CEOOPAreaCode = row["CEOOPAreaCode"].ToString();
                                                        if (row["CEOOPCalAreaCode"] == null)
                                                            CEOOPCalAreaCode = string.Empty;
                                                        else
                                                            CEOOPCalAreaCode = row["CEOOPCalAreaCode"].ToString();
                                                        if (row["CEOOPNumber"] == null)
                                                            CEOOPNumber = string.Empty;
                                                        else
                                                            CEOOPNumber = row["CEOOPNumber"].ToString();
                                                        if (row["CEOOPExtension"] == null)
                                                            CEOOPExtension = string.Empty;
                                                        else
                                                            CEOOPExtension = row["CEOOPExtension"].ToString();
                                                        if (row["CEOOPISDCode"] == null)
                                                            CEOOPISDCode = string.Empty;
                                                        else
                                                            CEOOPISDCode = row["CEOOPISDCode"].ToString();
                                                        if (row["CEOFaxCountryCode"] == null)
                                                            CEOFaxCountryCode = string.Empty;
                                                        else
                                                            CEOFaxCountryCode = row["CEOFaxCountryCode"].ToString();
                                                        if (row["CEOFaxAreaCode"] == null)
                                                            CEOFaxAreaCode = string.Empty;
                                                        else
                                                            CEOFaxAreaCode = row["CEOFaxAreaCode"].ToString();
                                                        if (row["CEOFaxISDCode"] == null)
                                                            CEOFaxISDCode = string.Empty;
                                                        else
                                                            CEOFaxISDCode = row["CEOFaxISDCode"].ToString();
                                                        if (row["CEOFaxNumber"] == null)
                                                            CEOFaxNumber = string.Empty;
                                                        else
                                                            CEOFaxNumber = row["CEOFaxNumber"].ToString();

                                                        ACApproved_CreateUpdateAccountContactDV(Connection, Transaction, AccountDVID, SyncHelper.GetCodeMasterID("CEO", BOL.AppConst.CodeType.Designation, true), "CEO", row["CEO"].ToString(), row["EmailAddress1"].ToString(), CEOOPCountryCode, CEOOPAreaCode, CEOOPCalAreaCode,
                                                        CEOOPISDCode, CEOOPNumber, CEOOPExtension, CEOHPCountryCode, CEOHPISDCode, CEOHPNumber, CEOFaxCountryCode, CEOFaxAreaCode, CEOFaxISDCode, CEOFaxNumber);
                                                    }
                                                    #endregion
                                                    #region "CTO"
                                                    Console.WriteLine("CTO");
                                                    if (!string.IsNullOrEmpty(row["CTO"].ToString()))
                                                    {
                                                        ACApproved_CreateUpdateAccountContactDV(Connection, Transaction, AccountDVID, SyncHelper.GetCodeMasterID("CTO", BOL.AppConst.CodeType.Designation, true), "CTO", row["CTO"].ToString(), row["EmailAddress2"].ToString());
                                                    }
                                                    #endregion
                                                    #region "CFO"
                                                    Console.WriteLine("CFO");
                                                    if (!string.IsNullOrEmpty(row["CFO"].ToString()))
                                                    {
                                                        ACApproved_CreateUpdateAccountContactDV(Connection, Transaction, AccountDVID, SyncHelper.GetCodeMasterID("CFO", BOL.AppConst.CodeType.Designation, true), "CFO", row["CFO"].ToString(), row["EmailAddress3"].ToString());
                                                    }
                                                    #endregion
                                                    #region "MD"
                                                    Console.WriteLine("MD");
                                                    if (!string.IsNullOrEmpty(row["MD"].ToString()))
                                                    {
                                                        string MDHPCountryCode = null;
                                                        string MDHPAreaCode = null;
                                                        string MDHPNumber = null;
                                                        string MDHPISDCode = null;
                                                        string MDOPCountryCode = null;
                                                        string MDOPAreaCode = null;
                                                        string MDOPCalAreaCode = null;
                                                        string MDOPNumber = null;
                                                        string MDOPExtension = null;
                                                        string MDOPISDCode = null;
                                                        string MDFaxCountryCode = null;
                                                        string MDFaxISDCode = null;
                                                        string MDFaxAreaCode = null;
                                                        string MDFaxNumber = null;

                                                        if (row["MDHPCountryCode"] == null)
                                                            MDHPCountryCode = string.Empty;
                                                        else
                                                            MDHPCountryCode = row["MDHPCountryCode"].ToString();
                                                        if (row["MDHPAreaCode"] == null)
                                                            MDHPAreaCode = string.Empty;
                                                        else
                                                            MDHPAreaCode = row["MDHPAreaCode"].ToString();
                                                        if (row["MDHPNumber"] == null)
                                                            MDHPNumber = string.Empty;
                                                        else
                                                            MDHPNumber = row["MDHPNumber"].ToString();
                                                        if (row["MDHPISDCode"] == null)
                                                            MDHPISDCode = string.Empty;
                                                        else
                                                            MDHPISDCode = row["MDHPISDCode"].ToString();
                                                        if (row["MDOPCountryCode"] == null)
                                                            MDOPCountryCode = string.Empty;
                                                        else
                                                            MDOPCountryCode = row["MDOPCountryCode"].ToString();
                                                        if (row["MDOPAreaCode"] == null)
                                                            MDOPAreaCode = string.Empty;
                                                        else
                                                            MDOPAreaCode = row["MDOPAreaCode"].ToString();
                                                        if (row["MDOPCalAreaCode"] == null)
                                                            MDOPCalAreaCode = string.Empty;
                                                        else
                                                            MDOPCalAreaCode = row["MDOPCalAreaCode"].ToString();
                                                        if (row["MDOPNumber"] == null)
                                                            MDOPNumber = string.Empty;
                                                        else
                                                            MDOPNumber = row["MDOPNumber"].ToString();
                                                        if (row["MDOPExtension"] == null)
                                                            MDOPExtension = string.Empty;
                                                        else
                                                            MDOPExtension = row["MDOPExtension"].ToString();
                                                        if (row["MDOPISDCode"] == null)
                                                            MDOPISDCode = string.Empty;
                                                        else
                                                            MDOPISDCode = row["MDOPISDCode"].ToString();
                                                        if (row["MDFaxCountryCode"] == null)
                                                            MDFaxCountryCode = string.Empty;
                                                        else
                                                            MDFaxCountryCode = row["MDFaxCountryCode"].ToString();
                                                        if (row["MDFaxAreaCode"] == null)
                                                            MDFaxAreaCode = string.Empty;
                                                        else
                                                            MDFaxAreaCode = row["MDFaxAreaCode"].ToString();
                                                        if (row["MDFaxISDCode"] == null)
                                                            MDFaxISDCode = string.Empty;
                                                        else
                                                            MDFaxISDCode = row["MDFaxISDCode"].ToString();
                                                        if (row["MDFaxNumber"] == null)
                                                            MDFaxNumber = string.Empty;
                                                        else
                                                            MDFaxNumber = row["MDFaxNumber"].ToString();

                                                        ACApproved_CreateUpdateAccountContactDV(Connection, Transaction, AccountDVID, SyncHelper.GetCodeMasterID("MD", BOL.AppConst.CodeType.Designation, true), "MD", row["MD"].ToString(), row["EmailAddress4"].ToString(), MDOPCountryCode, MDOPAreaCode, MDOPCalAreaCode,
                                                        MDOPISDCode, MDOPNumber, MDOPExtension, MDHPCountryCode, MDHPISDCode, MDHPNumber, MDFaxCountryCode, MDFaxAreaCode, MDFaxISDCode, MDFaxNumber);
                                                    }
                                                    #endregion
                                                    #region "OTHERS"
                                                    Console.WriteLine("OTHERS");
                                                    if (!string.IsNullOrEmpty(row["Others"].ToString()))
                                                    {
                                                        string OthHPCountryCode = null;
                                                        string OthHPAreaCode = null;
                                                        string OthHPNumber = null;
                                                        string OthHPISDCode = null;
                                                        string OthOPCountryCode = null;
                                                        string OthOPAreaCode = null;
                                                        string OthOPCalAreaCode = null;
                                                        string OthOPNumber = null;
                                                        string OthOPExtension = null;
                                                        string OthOPISDCode = null;
                                                        string OthFaxCountryCode = null;
                                                        string OthFaxAreaCode = null;
                                                        string OthFaxISDCode = null;
                                                        string OthFaxNumber = null;

                                                        if (row["OthHPCountryCode"] == null)
                                                            OthHPCountryCode = string.Empty;
                                                        else
                                                            OthHPCountryCode = row["OthHPCountryCode"].ToString();
                                                        if (row["OthHPAreaCode"] == null)
                                                            OthHPAreaCode = string.Empty;
                                                        else
                                                            OthHPAreaCode = row["OthHPAreaCode"].ToString();
                                                        if (row["OthHPNumber"] == null)
                                                            OthHPNumber = string.Empty;
                                                        else
                                                            OthHPNumber = row["OthHPNumber"].ToString();
                                                        if (row["OthHPISDCode"] == null)
                                                            OthHPISDCode = string.Empty;
                                                        else
                                                            OthHPISDCode = row["OthHPISDCode"].ToString();
                                                        if (row["OthOPCountryCode"] == null)
                                                            OthOPCountryCode = string.Empty;
                                                        else
                                                            OthOPCountryCode = row["OthOPCountryCode"].ToString();
                                                        if (row["OthOPAreaCode"] == null)
                                                            OthOPAreaCode = string.Empty;
                                                        else
                                                            OthOPAreaCode = row["OthOPAreaCode"].ToString();
                                                        if (row["OthOPCalAreaCode"] == null)
                                                            OthOPCalAreaCode = string.Empty;
                                                        else
                                                            OthOPCalAreaCode = row["OthOPCalAreaCode"].ToString();
                                                        if (row["OthOPNumber"] == null)
                                                            OthOPNumber = string.Empty;
                                                        else
                                                            OthOPNumber = row["OthOPNumber"].ToString();
                                                        if (row["OthOPExtension"] == null)
                                                            OthOPExtension = string.Empty;
                                                        else
                                                            OthOPExtension = row["OthOPExtension"].ToString();
                                                        if (row["OthOPISDCode"] == null)
                                                            OthOPISDCode = string.Empty;
                                                        else
                                                            OthOPISDCode = row["OthOPISDCode"].ToString();
                                                        if (row["OthFaxCountryCode"] == null)
                                                            OthFaxCountryCode = string.Empty;
                                                        else
                                                            OthFaxCountryCode = row["OthFaxCountryCode"].ToString();
                                                        if (row["OthFaxAreaCode"] == null)
                                                            OthFaxAreaCode = string.Empty;
                                                        else
                                                            OthFaxAreaCode = row["OthFaxAreaCode"].ToString();
                                                        if (row["OthFaxISDCode"] == null)
                                                            OthFaxISDCode = string.Empty;
                                                        else
                                                            OthFaxISDCode = row["OthFaxISDCode"].ToString();
                                                        if (row["OthFaxNumber"] == null)
                                                            OthFaxNumber = string.Empty;
                                                        else
                                                            OthFaxNumber = row["OthFaxNumber"].ToString();

                                                        ACApproved_CreateUpdateAccountContactDV(Connection, Transaction, AccountDVID, SyncHelper.GetCodeMasterID("Others", BOL.AppConst.CodeType.Designation, true), "Others", row["Others"].ToString(), row["EmailAddress5"].ToString(), OthOPCountryCode, OthOPAreaCode, OthOPCalAreaCode,
                                                        OthOPISDCode, OthOPNumber, OthOPExtension, OthHPCountryCode, OthHPISDCode, OthHPNumber, OthFaxCountryCode, OthFaxAreaCode, OthFaxISDCode, OthFaxNumber);
                                                    }
                                                    #endregion
                                                    #region Update Account
                                                    ACApproved_UpdateAccountDV(Connection, Transaction, AccountID, AccountDVID, AccountName, OperationalStatus, CoreActivities, BusinessPhoneCountryCode, BusinessPhoneSC, BusinessPhoneAC,
                                                    BusinessPhoneISD, BusinessPhone, BusinessPhoneExt, FaxCountryCode, FaxSC, FaxCC, Fax, WebSiteUrl, string.Empty, string.Empty,
                                                    string.Empty, SubmitType);
                                                    #endregion
                                                }

                                                //
                                                DataTable xmlDt = wizardData.Clone();
                                                DataRow xmlRow = xmlDt.NewRow();

                                                for (int i = 0; i <= xmlDt.Columns.Count - 1; i++)
                                                {
                                                    xmlRow[i] = row[i];
                                                }

                                                xmlDt.Rows.Add(xmlRow);

                                                StringWriter writer = new StringWriter();
                                                xmlDt.WriteXml(writer);

                                                CreateACApprovedUpdatedHistoryWithXML(Connection, Transaction, MeetingNo, FileID, SubmitType, writer.ToString());

                                                //Aryo 20120112 to Insert into MSCChangesHistory
                                                CreateMSCChangesHistory(Connection, Transaction, AccountID, SubmitTypeID);

                                                Console.WriteLine(string.Format("[{0}] {2}/{3} : Update AC Approved Account FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));
                                                LogFileHelper.logList.Add(string.Format("[{0}] {2}/{3} : Update AC Approved Account FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));
                                            }
                                            else
                                            {
                                                Console.WriteLine(string.Format("[{0}] {2}/{3} :Post MSC Changes already updated, skip Record FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));
                                                LogFileHelper.logList.Add(string.Format("[{0}] {2}/{3} : Post MSC Changes already updated, skip Record FileID {1}", DateTime.Now.ToString(), FileID, count, totalRecord));
                                            }


                                        }
                                        else
                                        {
                                            Console.WriteLine(string.Format("[{0}] {2}/{3} : Record FileID {1} not found", DateTime.Now.ToString(), FileID, count, totalRecord));
                                            LogFileHelper.logList.Add(string.Format("[{0}] {2}/{3} : Record FileID {1} not found", DateTime.Now.ToString(), FileID, count, totalRecord));
                                        }
                                        break;
                                        #endregion
                                }
                                //}
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(string.Format("[{0}] : Error -  {1} ", DateTime.Now.ToString(), ex.Message));
                                LogFileHelper.logList.Add(string.Format("[{0}] : Error -  {1} ", DateTime.Now.ToString(), ex.Message));
                                Transaction.Rollback();
                                List<string> TOs = new List<string>();
                                //TOs.AddRange(BOL.Common.Modules.Parameter.WIZARD_RCPNT.Split(','));
                                TOs.Add("appanalyst@mdec.com.my");
                                bool SendSuccess = BOL.Utils.Email.SendMail(TOs.ToArray(), null, null, BOL.Common.Modules.Parameter.WIZARD_SUBJ, string.Format("{0} SyncACApprovedAccount {1}", BOL.Common.Modules.Parameter.WIZARD_DESC, ex.Message.ToString()), null);
                                //WriteEmailLog("WizardSync", Guid.Empty, Guid.Empty, "TO : " + string.Join(",", TOs.ToArray()) + ";", SendSuccess);
                                isRollBack = true;
                            }
                            if (isRollBack == false)
                                Transaction.Commit();
                        }

                    }
                    else
                    {
                        Console.WriteLine(string.Format("[{0}] : No Record found for sync. process", DateTime.Now.ToString()));
                        LogFileHelper.logList.Add(string.Format("[{0}] : No Record found for sync. process", DateTime.Now.ToString()));
                    }

                    UpdateParamValue(BOL.AppConst.ParamCode.WIZARD_TMS, SyncHelper.AdminName, new Guid(SyncHelper.AdminID));
                    Console.WriteLine(string.Format("[{0}] : End Sync ACApprovedAccount", DateTime.Now.ToString()));
                    LogFileHelper.logList.Add(string.Format("[{0}] : End Sync ACApprovedAccount", DateTime.Now.ToString()));
                }
                catch (Exception ex)
                {
                    //LogFileHelper.logList.Add(ex.Message);
                    //List<string> TOs = new List<string>();
                    //TOs.AddRange(BOL.Common.Modules.Parameter.WIZARD_RCPNT.Split(','));
                    //TOs.Add("appanalyst@mdec.com.my");
                    //bool SendSuccess = BOL.Utils.Email.SendMail(TOs.ToArray(), null, null, BOL.Common.Modules.Parameter.WIZARD_SUBJ, string.Format("{0} SyncACApprovedAccount {1}", BOL.Common.Modules.Parameter.WIZARD_DESC, ex.Message), null);
                    //WriteEmailLog("WizardSync", Guid.Empty, Guid.Empty, "TO : " + string.Join(",", TOs.ToArray()) + ";", SendSuccess);
                }

            }


            //if (LogFileHelper.logList.Count > 0)
            //{
            //    string ModeSync = "ApprovedAccountSyncLog_";
            //    LogFileHelper.WriteLog(orniLogFileHelper.logList, ModeSync);
            #endregion
        }

        private static string getLatestExcelFile()
        {
            string FileName = "";
            string folderPath = ConfigurationSettings.AppSettings["ExcelStartUpLocation"].ToString();
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }
            string ExcelFile = "";
            var directory = new DirectoryInfo(folderPath);
            var fileName = directory.GetFiles()
            .OrderByDescending(f => f.LastWriteTime)
            .First();
            if (fileName != null)
                FileName = ConfigurationSettings.AppSettings["ExcelStartUpLocation"].ToString() + fileName.ToString();
            //if (IsDirectoryEmpty(folderPath))
            //{
            //    ExcelFile = ConfigurationSettings.AppSettings["ExcelStartUpLocation"].ToString() + "StarUp_" + DateTime.Now.ToString("ddMMyyyy") + ".xlsx";
            //}
            //else
            //{
            //    int LastCounter = getLastFileCounter(folderPath);

            //    if (LastCounter == 0)
            //        ExcelFile = ConfigurationSettings.AppSettings["ExcelLocation"].ToString() + "StarUp_" + DateTime.Now.ToString("ddMMyyyy") + ".xlsx";
            //    else
            //        ExcelFile = ConfigurationSettings.AppSettings["ExcelLocation"].ToString() + "StarUp_" + DateTime.Now.ToString("ddMMyyyy") + "_" + LastCounter.ToString() + ".xlsx";
            //}
            return FileName;
        }

        private static int getLastFileCounter(string folderPath)
        {
            int output = 0;
            var directory = new DirectoryInfo(folderPath);
            var fileName = directory.GetFiles()
            .OrderByDescending(f => f.LastWriteTime)
            .First();

            string[] arrFile = fileName.ToString().Split('_');
            string sDate = arrFile[1].ToString().Substring(0, 8); //21032016
            DateTime dDate = getDateTime(sDate);
            //1.xlsx, 10.xlsx
            string lastCounter = "";
            if (dDate.ToString("ddMMyyyy") == DateTime.Now.ToString("ddMMyyyy"))
            {
                if (arrFile.Count() > 2)
                {
                    if (arrFile[2].Length <= 6)
                        lastCounter = arrFile[2].Substring(0, 1);
                    else
                        lastCounter = arrFile[2].Substring(0, 2);
                }
                if (arrFile.Length == 2)
                    output = 1;
                else if (Convert.ToInt32(lastCounter) == 10)
                {
                    output = 0;
                }
                else
                {
                    output = Convert.ToInt32(lastCounter) + 1;
                }
            }
            else
                output = 0;
            return output;
        }

        private static DateTime getDateTime(string sDate)
        {
            DateTime myDate = new DateTime();
            string[] formats = { "ddMMyyyy" };
            return myDate = DateTime.ParseExact(sDate, formats, new CultureInfo(Thread.CurrentThread.CurrentCulture.Name), DateTimeStyles.None);
        }

        private static bool IsDirectoryEmpty(string path)
        {
            return !Directory.EnumerateFileSystemEntries(path).Any();
        }

        public static string CleanInvalidXmlChars(string text)
        {
            string re = @"[^\x09\x0A\x0D\x20-\xD7FF\xE000-\xFFFD\x10000-x10FFFF]";
            return Regex.Replace(text, re, "");
        }
        public static bool WriteEmailLog(string OwnerName, Guid OwnerID, Guid TicketID, string Recipient, bool SendSuccess)
        {
            int affectedRows = 0;

            SqlConnection con = SQLHelper.GetConnection();
            SqlCommand com = new SqlCommand();

            StringBuilder sql = new StringBuilder();
            sql.Append("INSERT INTO EmailLog ");
            sql.Append("(EmailLogID, OwnerName, OwnerID, ModuleName, ID, Recipient, SendSuccess, SendDate) ");
            sql.Append("VALUES ");
            sql.Append("(NEWID(), @OwnerName, @OwnerID, 'Service Desk', @TicketID, @Recipient, @SendSuccess, GETDATE())");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = con;

            com.Parameters.Add(new SqlParameter("@OwnerName", OwnerName));
            com.Parameters.Add(new SqlParameter("@OwnerID", OwnerID));
            com.Parameters.Add(new SqlParameter("@TicketID", TicketID));
            com.Parameters.Add(new SqlParameter("@Recipient", Recipient));
            com.Parameters.Add(new SqlParameter("@SendSuccess", SendSuccess));

            con.Open();

            affectedRows += com.ExecuteNonQuery();

            return (affectedRows > 0);
        }

        public static int UpdateParamValue(string ParamCode, string ActionByName, Guid ActionBy)
        {

            int affectedRows = 0;

            SqlConnection con = SQLHelper.GetConnection();
            SqlCommand com = new SqlCommand();
            Guid NotesID = Guid.NewGuid();
            string TodayDate = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss tt");
            StringBuilder sql = new StringBuilder();
            sql.AppendLine("UPDATE Parameter SET ");
            sql.AppendLine("ParamValue =  '" + TodayDate + "', ");
            sql.AppendLine("ModifiedDate = GETDATE(),");
            sql.AppendLine("ModifiedBy = @ActionBy,");
            sql.AppendLine("ModifiedByName = @ActionByName");
            sql.AppendLine("WHERE ParamCode = @ParamCode");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = con;

            com.Parameters.Add(new SqlParameter("@ParamCode", ParamCode));
            com.Parameters.Add(new SqlParameter("@ActionByName", ActionByName));
            com.Parameters.Add(new SqlParameter("@ActionBy", ActionBy));
            try
            {
                affectedRows = com.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                con.Close();
            }

            return affectedRows;
        }

        private static void CleanFile()
        {
            //DELETE LOG FILE
            if (Directory.Exists(ConfigurationSettings.AppSettings["LogFileLocation"].ToString()))
            {
                var files = new DirectoryInfo(ConfigurationSettings.AppSettings["LogFileLocation"].ToString()).GetFiles("*.txt");
                foreach (var file in files)
                {
                    if (DateTime.UtcNow - file.CreationTimeUtc > TimeSpan.FromDays(30))
                    {
                        File.Delete(file.FullName);
                    }
                }
            }
            //DELETE EXCEL FILE
            if (Directory.Exists(ConfigurationSettings.AppSettings["ExcelLocation"].ToString()))
            {
                var files1 = new DirectoryInfo(ConfigurationSettings.AppSettings["ExcelLocation"].ToString()).GetFiles("*.xlsx");
                foreach (var file in files1)
                {
                    if (DateTime.UtcNow - file.CreationTimeUtc > TimeSpan.FromDays(30))
                    {
                        File.Delete(file.FullName);
                    }
                }
            }
        }
        private static void ACApproved_CreateUpdateAccountContact(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? DesignationCID, string DesignationName, string Name, string Email)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            BOL.AccountContact.odsAccount mgrAccount = new BOL.AccountContact.odsAccount();
            Nullable<Guid> ContactID = GetAccountContactID(Connection, Transaction, AccountID, DesignationCID);
            if (ContactID.HasValue && DesignationName != "Others")
            {
                try
                {
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow currentData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);

                    sql.AppendLine("UPDATE Contact SET ");
                    sql.AppendLine("Name = @Name,");
                    sql.AppendLine("Email = @Email,");
                    sql.AppendLine("ModifiedBy = @UserID,");
                    sql.AppendLine("ModifiedByName = @UserName,");
                    sql.AppendLine("ModifiedDate = getdate(),");
                    sql.AppendLine("DataSource = '" + SyncHelper.DataSource + "',");
                    sql.AppendLine("MSCKeyContact = 1");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine("WHERE ContactID = @ContactID");
                    com.Parameters.Add(new SqlParameter("@ContactID", ContactID));

                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@ContactStatus", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;
                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    //NO1
                    //SqlCommand comSTG = new SqlCommand();
                    //System.Text.StringBuilder sqlSTG = new System.Text.StringBuilder();
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactID, AccountID, Name, SalutationCID, DesignationCID, DesignationName, Department, ReportToContactID, Role, ContactStatus, Email, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone,");
                    //sqlSTG.AppendLine("BusinessPhoneExt, MobilePhoneCtry, MobilePhoneCC, MobilePhone, FaxCtry, FaxStt, FaxCC, Fax, IMAddress, SkypeName, ContactCategoryCID, Gender, DateOfBirth, RaceCID, SpouseName, Anniversary, AccessMode, KeyContact,");
                    //sqlSTG.AppendLine(" MSCKeyContact,  CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,Published,OtherEmail, OtherMobilePhone, OtherBusinessPhone,OtherFax, DataSource,  Deleted,  CREATEDDATEMIGRATE,ContactTypeCID,");
                    //sqlSTG.AppendLine("CEOEmailFlag, AQIRContact, MGSContact,AQIRID,ContactClassificationID,PAName, MDeCPrimaryContact,MDeCBackupContact, AQIRSyncTimestamp");
                    //sqlSTG.AppendLine("FROM Contact WHERE ContactID = @ContactID");
                    //sqlSTG.AppendLine("UPDATE ContactSTG");
                    //sqlSTG.AppendLine("  SET Name=@Name");
                    //sqlSTG.AppendLine("  , Email=@Email");
                    //sqlSTG.AppendLine("  , ModifiedDate=@ActionDate");
                    //sqlSTG.AppendLine("  , ModifiedBy=@ActionBy");
                    //sqlSTG.AppendLine("  , ModifiedByName=@ActionByName");
                    //sqlSTG.AppendLine("WHERE ContactID=@ContactID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.Parameters.AddWithValue("@ContactID", SyncHelper.ReturnNull(ContactID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);

                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.ExecuteNonQuery();

                    ////To update contact in AQIR via WebService
                    //string Result = string.Empty;
                    //bool Success = false;

                    //if (mgrContact.IsAQIRContactSTG_Wizard(Connection, Transaction, ContactID))
                    //{
                    //    Result = mgrContact.UpdateAQIRContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID));
                    //    string AQIRID = mgrContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactID);
                    //    string XMLForContact = mgrContact.GetXMLForContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //    XmlDocument XMLDoc = new XmlDocument();

                    //    XmlDocument Doc = new XmlDocument();
                    //    XMLDoc.LoadXml(XMLForContact);

                    //    Doc.LoadXml(Result);
                    //    Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //    if (Success)
                    //    {
                    //        mgrContact.MoveContactSTGToActualContactUpdate_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContact", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    }
                    //    else {
                    //        mgrContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContact", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //        throw new Exception("Updating new contact failed in AQIR.");
                    //    }
                    //}
                    //End update
                    #endregion
                    //Log
                    //DataRow newData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);
                    //if (currentData != null && newData != null)
                    //{
                    //    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactID.Value, AccountID, currentData, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //}
                    alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactID.Value, guidAccountID, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                }
                catch (Exception ex)
                {
                }

            }
            else
            {
                CodeMaster mgr = new CodeMaster();
                Guid ContactTypeCID = mgr.GetCodeMasterID_Wizard(Connection, Transaction, BOL.AppConst.CodeType.ContactType, "Key Contact");

                try
                {

                    //con.Open()
                    //NO2

                    sql.AppendLine("INSERT INTO Contact ");
                    sql.AppendLine("(");
                    sql.AppendLine("ContactID, AccountID, DesignationCID, DesignationName, Name, Email, ContactTypeCID, Published, ContactStatus,");
                    sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate, DataSource, MSCKeyContact,Deleted");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine(")");
                    sql.AppendLine("VALUES(");
                    sql.AppendLine("@ContactID, @AccountID, @DesignationCID, @DesignationName, @Name, @Email, @ContactTypeCID, @Published, @ContactStatus,");
                    sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate(), @DataSource, 1,'N'");
                    sql.AppendLine(")");

                    ContactID = Guid.NewGuid();
                    com.Parameters.Add(new SqlParameter("@ContactID", ContactID.Value));
                    com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                    com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));
                    com.Parameters.Add(new SqlParameter("@DesignationName", DesignationName));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@ContactTypeCID", ContactTypeCID));
                    com.Parameters.Add(new SqlParameter("@Published", true));
                    com.Parameters.Add(new SqlParameter("@ContactStatus", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    ////To update contact in AQIR via WebService
                    //string Result = string.Empty;
                    //bool Success = false;

                    //if (mgrContact.IsAQIRContactSTG_Wizard(Connection, Transaction, ContactID))
                    //{
                    //    Result = mgrContact.UpdateAQIRContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID));
                    //    string AQIRID = mgrContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactID);
                    //    string XMLForContact = mgrContact.GetXMLForContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //    XmlDocument XMLDoc = new XmlDocument();

                    //    XmlDocument Doc = new XmlDocument();
                    //    XMLDoc.LoadXml(XMLForContact);

                    //    Doc.LoadXml(Result);
                    //    Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //    AQIRID = Doc.SelectSingleNode("//MSCFile/MainAqirID").InnerText;
                    //    if (Success)
                    //    {
                    //        mgrContact.UpdateContactAQIRID_Wizard(Connection, Transaction, AQIRID, ContactID);
                    //        mgrContact.MoveContactSTGToActualContactUpdate_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContact", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    }
                    //    else {
                    //        mgrContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContact", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //        throw new Exception("Inserting new contact failed in AQIR.");
                    //    }
                    //}
                    //End update
                    #endregion
                    //Log
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactID.Value, guidAccountID, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                }
            }
        }


        private static void ACApproved_CreateUpdateFinancialAndWorkerForecast(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, int? Year, int? LocalKW, int? ForeignKW, int? LocalWorker, int? ForeignWorker, decimal? Investment, Nullable<decimal> RnDExpenditure, Nullable<decimal> LocalSales, Nullable<decimal> ExportSales, Nullable<decimal> NetProfit, Nullable<decimal> CashFlow, Nullable<decimal> Asset, Nullable<decimal> Equity, Nullable<decimal> Liabilities)
        {

            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            Nullable<Guid> FinancialAndWorkerForecastID = GetFinancialAndWorkerForecastID(Connection, Transaction, AccountID, Year);
            if (FinancialAndWorkerForecastID.HasValue)
            {
                Guid guidFinancialAndWorkerForecastID = new Guid(FinancialAndWorkerForecastID.ToString());
                BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                DataRow currentLogData = alMgr.SelectAccountForLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, guidFinancialAndWorkerForecastID);

                sql.AppendLine("UPDATE FinancialAndWorkerForecast SET ");
                sql.AppendLine("LocalKW = @LocalKW,");
                sql.AppendLine("ForeignKW = @ForeignKW,");
                sql.AppendLine("LocalWorker = @LocalWorker,");
                sql.AppendLine("ForeignWorker = @ForeignWorker,");
                sql.AppendLine("Investment = @Investment,");
                sql.AppendLine("RnDExpenditure = @RnDExpenditure,");
                sql.AppendLine("LocalSales = @LocalSales,");
                sql.AppendLine("ExportSales = @ExportSales,");
                sql.AppendLine("NetProfit = @NetProfit,");
                sql.AppendLine("CashFlow = @CashFlow,");
                sql.AppendLine("Asset = @Asset,");
                sql.AppendLine("Equity = @Equity,");
                sql.AppendLine("Liabilities = @Liabilities,");
                sql.AppendLine("ModifiedBy = @UserID,");
                sql.AppendLine("ModifiedByName = @UserName,");
                sql.AppendLine("ModifiedDate = getdate()");
                sql.AppendLine("WHERE FinancialAndWorkerForecastID = @FinancialAndWorkerForecastID");

                com.Parameters.Add(new SqlParameter("@FinancialAndWorkerForecastID", FinancialAndWorkerForecastID));
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@LocalKW", SyncHelper.ReturnNull(LocalKW)));
                com.Parameters.Add(new SqlParameter("@ForeignKW", SyncHelper.ReturnNull(ForeignKW)));
                com.Parameters.Add(new SqlParameter("@LocalWorker", SyncHelper.ReturnNull(LocalWorker)));
                com.Parameters.Add(new SqlParameter("@ForeignWorker", SyncHelper.ReturnNull(ForeignWorker)));
                com.Parameters.Add(new SqlParameter("@Investment", SyncHelper.ReturnNull(Investment)));
                com.Parameters.Add(new SqlParameter("@RnDExpenditure", SyncHelper.ReturnNull(RnDExpenditure)));
                com.Parameters.Add(new SqlParameter("@LocalSales", SyncHelper.ReturnNull(LocalSales)));
                com.Parameters.Add(new SqlParameter("@ExportSales", SyncHelper.ReturnNull(ExportSales)));
                com.Parameters.Add(new SqlParameter("@NetProfit", SyncHelper.ReturnNull(NetProfit)));
                com.Parameters.Add(new SqlParameter("@CashFlow", SyncHelper.ReturnNull(CashFlow)));
                com.Parameters.Add(new SqlParameter("@Asset", SyncHelper.ReturnNull(Asset)));
                com.Parameters.Add(new SqlParameter("@Equity", SyncHelper.ReturnNull(Equity)));
                com.Parameters.Add(new SqlParameter("@Liabilities", SyncHelper.ReturnNull(Liabilities)));
                com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Company Change Log - Financial & Worker Forecast
                    DataRow newLogData = alMgr.SelectAccountForLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, guidFinancialAndWorkerForecastID);
                    if (currentLogData != null && newLogData != null)
                    {
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        alMgr.CreateAccountLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, guidFinancialAndWorkerForecastID, guidAccountID, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }
            }
            else
            {
                sql.AppendLine("INSERT INTO FinancialAndWorkerForecast ");

                sql.AppendLine("(");

                sql.AppendLine("FinancialAndWorkerForecastID, AccountID, Year, LocalKW, ForeignKW, LocalWorker, ForeignWorker, Investment, ");

                sql.AppendLine("RnDExpenditure, LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities, ");

                sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");

                sql.AppendLine(")");

                sql.AppendLine("VALUES(");

                sql.AppendLine("@FinancialAndWorkerForecastID, @AccountID, @Year, @LocalKW, @ForeignKW, @LocalWorker, @ForeignWorker, @Investment, ");

                sql.AppendLine("@RnDExpenditure, @LocalSales, @ExportSales, @NetProfit, @CashFlow, @Asset, @Equity, @Liabilities, ");

                sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate()");

                sql.AppendLine(")");

                FinancialAndWorkerForecastID = Guid.NewGuid();
                com.Parameters.Add(new SqlParameter("@FinancialAndWorkerForecastID", FinancialAndWorkerForecastID));
                com.Parameters.Add(new SqlParameter("@Year", Year));
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@LocalKW", SyncHelper.ReturnNull(LocalKW)));
                com.Parameters.Add(new SqlParameter("@ForeignKW", SyncHelper.ReturnNull(ForeignKW)));
                com.Parameters.Add(new SqlParameter("@LocalWorker", SyncHelper.ReturnNull(LocalWorker)));
                com.Parameters.Add(new SqlParameter("@ForeignWorker", SyncHelper.ReturnNull(ForeignWorker)));
                com.Parameters.Add(new SqlParameter("@Investment", SyncHelper.ReturnNull(Investment)));
                com.Parameters.Add(new SqlParameter("@RnDExpenditure", SyncHelper.ReturnNull(RnDExpenditure)));
                com.Parameters.Add(new SqlParameter("@LocalSales", SyncHelper.ReturnNull(LocalSales)));
                com.Parameters.Add(new SqlParameter("@ExportSales", SyncHelper.ReturnNull(ExportSales)));
                com.Parameters.Add(new SqlParameter("@NetProfit", SyncHelper.ReturnNull(NetProfit)));
                com.Parameters.Add(new SqlParameter("@CashFlow", SyncHelper.ReturnNull(CashFlow)));
                com.Parameters.Add(new SqlParameter("@Asset", SyncHelper.ReturnNull(Asset)));
                com.Parameters.Add(new SqlParameter("@Equity", SyncHelper.ReturnNull(Equity)));
                com.Parameters.Add(new SqlParameter("@Liabilities", SyncHelper.ReturnNull(Liabilities)));
                com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();
                    Guid guidFinancialAndWorkerForecastID = new Guid(FinancialAndWorkerForecastID.ToString());
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    //Company Change Log - Financial & Worker Forecast
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newLogData = alMgr.SelectAccountForLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, guidFinancialAndWorkerForecastID);
                    alMgr.CreateAccountLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, guidFinancialAndWorkerForecastID, guidAccountID, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }
            }
        }

        public DataRow SelectAccountForLog_FinancialAndWorkerForecast_Wizard(SqlConnection Connection, SqlTransaction Transaction, Guid FinancialAndWorkerForecastID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT fwf.[Year], ");
                    sql.AppendLine("fwf.Investment, fwf.RnDExpenditure AS [R&D Expenditure],");
                    sql.AppendLine("fwf.LocalSales AS [Local Sales], fwf.ExportSales AS [Export Sales],");
                    sql.AppendLine("fwf.Revenue, fwf.NetProfit AS [Net Profit], fwf.CashFlow AS [Cash Flow],");
                    sql.AppendLine("fwf.Asset, fwf.Equity, fwf.Liabilities, fwf.LocalKW AS [Local KW],");
                    sql.AppendLine("fwf.ForeignKW AS [Foreign KW], fwf.LocalWorker AS [Local Worker], ");
                    sql.AppendLine("fwf.ForeignWorker AS [Foreign Worker]");
                    sql.AppendLine("FROM FinancialAndWorkerForecast fwf");
                    sql.AppendLine("WHERE fwf.FinancialAndWorkerForecastID = @FinancialAndWorkerForecastID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@FinancialAndWorkerForecastID", FinancialAndWorkerForecastID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        private static Nullable<Guid> GetFinancialAndWorkerForecastID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, int? Year)
        {

            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT FinancialAndWorkerForecastID");
            sql.AppendLine("FROM FinancialAndWorkerForecast");
            sql.AppendLine("WHERE AccountID = @AccountID");
            sql.AppendLine("AND [Year] = @Year");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@Year", Year));
                DataTable dt = new DataTable();
                ad.Fill(dt);
                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }
        private static bool IsUpdated(SqlConnection Connection, SqlTransaction Transaction, int MeetingNo, string MSCFileID, string SubmitType)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT count(1)");
            sql.AppendLine("FROM ACApprovedAccountHistory");
            sql.AppendLine("WHERE MeetingNo = @MeetingNo");
            sql.AppendLine("AND MSCFileID = @MSCFileID");
            sql.AppendLine("AND SubmitType = @SubmitType");


            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            try
            {
                com.Parameters.Add(new SqlParameter("@MeetingNo", MeetingNo));
                com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));
                com.Parameters.Add(new SqlParameter("@SubmitType", SubmitType));

                DataTable dt = new DataTable();
                ad.Fill(dt);
                if (Convert.ToInt32(dt.Rows[0][0]) > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }

        private static void ACApproved_CreateAccount(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, string AccountName, string CompanyRegNo, string MSCFileID, int OperationalStatus)
        {
            //Dim con As SqlClient.SqlConnection = SyncHelper.NewCRMConnection
            SqlCommand com = new SqlCommand();

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO Account (AccountID, AccountName, MSCFileID, CompanyRegNo, OperationStatus, CreatedBy, CreatedByName, ModifiedBy, ModifiedByName, CreatedDate, ModifiedDate, DataSource, BursaMalaysiaCID ) ");
            //Aryo20120109 Set default value "No" for BursaMalaysia
            sql.AppendLine("VALUES (@AccountID, @AccountName, @MSCFileID, @CompanyRegNo, @OperationStatus, @CreatedBy, @CreatedByName, @ModifiedBy, @ModifiedByName, @CreatedDate, @ModifiedDate, @DataSource, @BursaMalaysiaCID) ");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
            com.Parameters.Add(new SqlParameter("@AccountName", AccountName));
            com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
            com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));
            com.Parameters.Add(new SqlParameter("@CompanyRegNo", CompanyRegNo));
            com.Parameters.Add(new SqlParameter("@OperationStatus", OperationalStatus));
            com.Parameters.Add(new SqlParameter("@CreatedBy", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@CreatedByName", SyncHelper.AdminName));
            com.Parameters.Add(new SqlParameter("@ModifiedBy", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@ModifiedByName", SyncHelper.AdminName));
            com.Parameters.Add(new SqlParameter("@CreatedDate", DateTime.Now));
            com.Parameters.Add(new SqlParameter("@ModifiedDate", DateTime.Now));
            com.Parameters.Add(new SqlParameter("@BursaMalaysiaCID", BOL.Common.Modules.Parameter.DEFAULT_STCK_EXCHNGE));

            try
            {
                //con.Open()
                com.ExecuteNonQuery();

                //Company Change Log - General
                BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                Guid guidAccountID = new Guid(AccountID.ToString());
                DataRow newGeneralLogData = alMgr.SelectAccountForLog_General_Wizard(Connection, Transaction, guidAccountID);
                alMgr.CreateAccountLog_General_Wizard(Connection, Transaction, guidAccountID, null, newGeneralLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                //Company Change Log - Portfolio
                DataRow newPortfolioLogData = alMgr.SelectAccountForLog_Portfolio_Wizard(Connection, Transaction, guidAccountID);
                alMgr.CreateAccountLog_Portfolio_Wizard(Connection, Transaction, guidAccountID, null, newPortfolioLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                //Company Change Log - Relocation Plan
                DataRow newRelocationPlanLogData = alMgr.SelectAccountForLog_RelocationPlan_Wizard(Connection, Transaction, guidAccountID);
                alMgr.CreateAccountLog_RelocationPlan_Wizard(Connection, Transaction, guidAccountID, null, newRelocationPlanLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //	con.Close()
            }
        }
        public static Guid? GetParamValue(string ParamCode)
        {
            Guid? output = null;
            using (SqlConnection con = SQLHelper.GetConnection())
            {
                SqlCommand com = new SqlCommand();
                SqlDataAdapter ad = new SqlDataAdapter();

                try
                {
                    ad.SelectCommand = com;

                    StringBuilder sql = new StringBuilder();
                    sql.AppendLine("SELECT p.ParamValue");
                    sql.AppendLine("FROM Parameter p");
                    sql.AppendLine("WHERE p.ParamCode = @ParamCode");

                    com.CommandText = sql.ToString();
                    com.Connection = con;

                    com.Parameters.Add(new SqlParameter("@ParamCode", ParamCode));

                    DataTable dt = new DataTable();
                    ad.Fill(dt);
                    output = new Guid(dt.Rows[0][0].ToString());
                }
                catch (Exception ex)
                {
                    //Throw
                }
                finally
                {
                    con.Close();
                }
            }
            return output;
        }
        private static DataTable SelectACApprovedAccountList(out string SyncedDate)
        {
            SqlConnection con = SQLHelper.GetConnection();
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            #region "AUTO-SYNC"
            //string LastSync = BOL.Common.Modules.Parameter.WIZARD_TMS;
            //SyncedDate = LastSync;
            //sql.AppendLine(ConfigurationSettings.AppSettings["WizardStoredProc"].ToString()).Append(" '").Append(LastSync).Append("'");
            #endregion
            #region "IF WANT TO RE-SYNC"
            sql.AppendLine(ConfigurationSettings.AppSettings["WizardStoredProc"].ToString()).Append(" '").Append("2017-07-18 00:00:00 AM").Append("'");
            //sql.AppendLine("EXEC [MDCAZ-WIZARD2].production.dbo.spbigfileeir 727");
            SyncedDate = "2017-07-18 00:00:00 AM";
            #endregion
            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = con;
            com.CommandTimeout = int.MaxValue;

            try
            {
                DataTable dt = new DataTable();

                ad.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                con.Close();
            }

        }


        public static Nullable<Guid> GetAccountIDByFileID(SqlConnection Connection, SqlTransaction Transaction, string FileID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT AccountID FROM Account WHERE MSCFileID = @FileID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@FileID", FileID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }


        private static DataTable GetShareHolder(SqlConnection Connection, SqlTransaction Transaction, string MSCFileID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT OwnershipSHName, OwnershipPer, OwnershipBumi, OwnershipCName ");
            sql.AppendLine("FROM IntegrationDB.dbo.EIR_PMSCOwnerShipDtls ");
            sql.AppendLine("WHERE FileID = @MSCFileID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                return dt;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }

        //For delete all the shareholder prevent duplication
        private static void ACApproved_DeleteShareholder(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID)
        {

            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
            Guid ShareHolderID = Guid.NewGuid();
            DataRow currentLogData = alMgr.SelectAccountForLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID);

            sql.AppendLine("DELETE FROM ShareHolder ");
            sql.AppendLine("WHERE AccountID = @AccountID ");

            com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            try
            {
                //con.Open()
                com.ExecuteNonQuery();

                //Company Change Log - Shareholder
                DataRow newLogData = alMgr.SelectAccountForLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID);
                if (currentLogData != null && newLogData != null)
                {
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID, guidAccountID, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //	con.Close()
            }
        }

        private static void ACApproved_CreateUpdateShareholder(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, string ShareholderName, Nullable<decimal> Percentage, bool BumiShare, Nullable<Guid> CountryRegionID, DateTime SyncDate)
        {
            if (!string.IsNullOrEmpty(ShareholderName))
            {
                SqlCommand com = new SqlCommand();
                System.Text.StringBuilder sql = new System.Text.StringBuilder();

                BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                Guid ShareHolderID = Guid.NewGuid();
                DataRow currentLogData = alMgr.SelectAccountForLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID);

                sql.AppendLine("DELETE FROM ShareHolder");
                sql.AppendLine("WHERE AccountID = @AccountID ");
                sql.AppendLine("AND ModifiedDate <> @SyncDate ");
                sql.AppendLine("INSERT INTO ShareHolder ");
                sql.AppendLine("(");
                sql.AppendLine("ShareHolderID, AccountID, ShareholderName, Percentage, BumiShare, Status, CountryRegionID, ");
                sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate ");
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@ShareHolderID, @AccountID, @ShareholderName, @Percentage, @BumiShare, @Status, @CountryRegionID, ");
                sql.AppendLine("@UserID, @UserName, @SyncDate, @UserID, @UserName, @SyncDate");
                sql.AppendLine(")");

                com.Parameters.Add(new SqlParameter("@ShareHolderID", ShareHolderID));
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@ShareholderName", ShareholderName));
                com.Parameters.Add(new SqlParameter("@Percentage", SyncHelper.ReturnNull(Percentage)));
                com.Parameters.Add(new SqlParameter("@BumiShare", BumiShare));
                com.Parameters.Add(new SqlParameter("@Status", EnumSync.Status.Active));
                com.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));
                com.Parameters.Add(new SqlParameter("@SyncDate", SyncHelper.ReturnNull(SyncDate)));
                com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Company Change Log - Shareholder
                    DataRow newLogData = alMgr.SelectAccountForLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID);
                    if (currentLogData != null && newLogData != null)
                    {
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        alMgr.CreateAccountLog_Shareholder_Wizard(Connection, Transaction, ShareHolderID, guidAccountID, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                }
                catch (Exception ex)
                {

                }

            }
        }

        private static void ACApproved_CreateAccountCluster(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? ClusterID)
        {
            if (!GetAccountClusterID(Connection, Transaction, AccountID, ClusterID).HasValue)
            {
                SqlCommand com = new SqlCommand();
                System.Text.StringBuilder sql = new System.Text.StringBuilder();
                sql.AppendLine("INSERT INTO AccountCluster ");
                sql.AppendLine("(");
                sql.AppendLine("AccountClusterID, AccountID, ClusterID, ");
                sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@AccountClusterID, @AccountID, @ClusterID, ");
                sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate()");
                sql.AppendLine(")");

                Guid AccountClusterID = Guid.NewGuid();
                com.Parameters.Add(new SqlParameter("@AccountClusterID", AccountClusterID));
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@ClusterID", ClusterID));
                com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Company Change Log - Cluster
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newLogData = alMgr.SelectAccountForLog_Cluster_Wizard(Connection, Transaction, AccountClusterID);
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Cluster_Wizard(Connection, Transaction, AccountClusterID, guidAccountID, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }
            }
        }

        private static Nullable<Guid> GetAccountClusterID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? ClusterID)
        {

            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT ClusterID");
            sql.AppendLine("FROM AccountCluster");
            sql.AppendLine("WHERE AccountID = @AccountID");
            sql.AppendLine("AND ClusterID = @ClusterID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@ClusterID", ClusterID));
                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //	con.Close()
            }
        }

        private static void ACApproved_CreateUpdateBusinessAnalystAssignment(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, string EEManagerName, Nullable<DateTime> AssignmentDate)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            Guid? BusinessAnalystTypeCID = SyncHelper.GetCodeMasterID(Connection, Transaction, BOL.AppConst.AccountManagerType.BusinessAnalyst, BOL.AppConst.CodeType.AccountManagerType, true);
            Nullable<Guid> AccountManagerAssignmentID = GetBusinessAnalystID(Connection, Transaction, AccountID, EEManagerName);
            try
            {
                if (!AccountManagerAssignmentID.HasValue)
                {
                    sql.AppendLine("INSERT INTO AccountManagerAssignment ");
                    sql.AppendLine("(");
                    sql.AppendLine("AccountManagerAssignmentID, AccountID, EEManagerName, AccountManagerTypeCID, DataSource, AssignmentDate,");
                    sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");
                    sql.AppendLine(")");
                    sql.AppendLine("VALUES(");
                    sql.AppendLine("@AccountManagerAssignmentID, @AccountID, @EEManagerName, @AccountManagerTypeCID, @DataSource, @AssignmentDate,");
                    sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate()");
                    sql.AppendLine(")");

                    AccountManagerAssignmentID = Guid.NewGuid();
                    if (!AssignmentDate.HasValue)
                        AssignmentDate = DateTime.Now;
                    com.Parameters.Add(new SqlParameter("@AccountManagerAssignmentID", AccountManagerAssignmentID));
                    com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                    com.Parameters.Add(new SqlParameter("@EEManagerName", EEManagerName));
                    com.Parameters.Add(new SqlParameter("@AccountManagerTypeCID", BusinessAnalystTypeCID));
                    com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));
                    com.Parameters.Add(new SqlParameter("@AssignmentDate", AssignmentDate));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    try
                    {
                        com.ExecuteNonQuery();

                        //Company Change Log 
                        BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                        Guid guidAccountManagerAssignmentID = new Guid(AccountManagerAssignmentID.ToString());
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        DataRow newLogData = alMgr.SelectAccountForLog_EEManager_Wizard(Connection, Transaction, guidAccountManagerAssignmentID);
                        alMgr.CreateAccountLog_BusinessAnalyst_Wizard(Connection, Transaction, guidAccountManagerAssignmentID, guidAccountID, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                    catch (Exception ex)
                    {
                        throw;
                        //Finally
                        //	con.Close()
                    }
                }
                else
                {
                    Guid guidAccountManagerAssignmentID = new Guid(AccountManagerAssignmentID.ToString());
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow currentLogData = alMgr.SelectAccountForLog_EEManager(guidAccountManagerAssignmentID);

                    sql.AppendLine("UPDATE AccountManagerAssignment ");
                    sql.AppendLine("SET AssignmentDate = @AssignmentDate, ");
                    sql.AppendLine("ModifiedBy = @UserID,");
                    sql.AppendLine("ModifiedByName = @UserName,");
                    sql.AppendLine("ModifiedDate = getdate()");
                    sql.AppendLine("WHERE AccountManagerAssignmentID = @AccountManagerAssignmentID");

                    if (!AssignmentDate.HasValue)
                        AssignmentDate = DateTime.Now;
                    com.Parameters.Add(new SqlParameter("@AccountManagerAssignmentID", AccountManagerAssignmentID));
                    com.Parameters.Add(new SqlParameter("@AssignmentDate", AssignmentDate));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    try
                    {
                        //con.Open()
                        com.ExecuteNonQuery();

                        //Company Change Log 
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        DataRow newLogData = alMgr.SelectAccountForLog_EEManager_Wizard(Connection, Transaction, guidAccountManagerAssignmentID);
                        alMgr.CreateAccountLog_BusinessAnalyst_Wizard(Connection, Transaction, guidAccountManagerAssignmentID, guidAccountID, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                    catch (Exception ex)
                    {
                        throw;
                        //Finally
                        //	con.Close()
                    }
                }
            }
            catch (Exception ex)
            {

                throw;
            }

        }

        private static Nullable<Guid> GetBusinessAnalystID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, string EEManagerName)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);
            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            Guid? PreMSCCID = SyncHelper.GetCodeMasterID(BOL.AppConst.AccountManagerType.PreMSC, BOL.AppConst.CodeType.AccountManagerType, true);
            Guid? BusinessAnalystCID = SyncHelper.GetCodeMasterID(BOL.AppConst.AccountManagerType.BusinessAnalyst, BOL.AppConst.CodeType.AccountManagerType, true);
            sql.AppendLine("SELECT AccountManagerAssignmentID");
            sql.AppendLine("FROM AccountManagerAssignment");
            sql.AppendLine("WHERE AccountID = @AccountID");
            sql.AppendLine("AND EEManagerName = @EEManagerName");
            sql.AppendLine("AND (AccountManagerTypeCID = @BusinessAnalystCID");
            sql.AppendLine("OR AccountManagerTypeCID = @PreMSCCID)");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@EEManagerName", EEManagerName));
                com.Parameters.Add(new SqlParameter("@BusinessAnalystCID", BusinessAnalystCID));
                com.Parameters.Add(new SqlParameter("@PreMSCCID", PreMSCCID));
                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }

        private static void ACApproved_CreateUpdateAccountAddress(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? AddressTypeID, string Address1, string Address2, string Address3, string City, string PostCode, string State,
Nullable<Guid> CountryRegionID, string BusinessPhoneCountryCode, string BusinessPhoneSC, string BusinessPhoneAC, string BusinessPhoneCC, string BusinessPhone, string BusinessPhoneExt, string FaxCountryCode, string FaxSC, string FaxCC,
string Fax)
        {

            Nullable<Guid> AddressID = GetAccountAddressID(Connection, Transaction, AccountID, AddressTypeID);

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            string OffCallingCode = string.Empty;
            string FaxCallingCode = string.Empty;

            if (!string.IsNullOrEmpty(BusinessPhoneCC) && !string.IsNullOrEmpty(BusinessPhoneSC))
            {
                OffCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, BusinessPhoneSC, BusinessPhoneCC);
                BusinessPhoneCC = OffCallingCode;
            }

            if (!string.IsNullOrEmpty(FaxCC) && !string.IsNullOrEmpty(FaxSC))
            {
                FaxCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, FaxSC, FaxCC);
                FaxCC = FaxCallingCode;
            }


            if (AddressID.HasValue)
            {
                Console.WriteLine("13");
                BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                Guid guidAddressID = new Guid(AddressID.ToString());
                DataRow currentLogData = alMgr.SelectAccountForLog_Address_Wizard(Connection, Transaction, guidAddressID);

                try
                {
                    SqlCommand com = new SqlCommand();
                    System.Text.StringBuilder sql = new System.Text.StringBuilder();
                    sql.Length = 0;
                    sql.AppendLine("UPDATE Address SET ");
                    sql.AppendLine("Address1 = @Address1,");
                    sql.AppendLine("Address2 = @Address2,");
                    sql.AppendLine("Address3 = @Address3,");
                    sql.AppendLine("City = @City,");
                    sql.AppendLine("PostCode = @PostCode,");
                    sql.AppendLine("State = @State,");
                    sql.AppendLine("CountryRegionID = @CountryRegionID,");
                    sql.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode, ");
                    sql.AppendLine("BusinessPhoneStt = @BusinessPhoneSC, ");
                    sql.AppendLine("BusinessPhoneCC = @BusinessPhoneCC, ");
                    sql.AppendLine("BusinessPhone = @BusinessPhone, ");
                    sql.AppendLine("BusinessPhoneExt = @BusinessPhoneExt, ");
                    sql.AppendLine("FaxCtry = @FaxCountryCode, ");
                    sql.AppendLine("FaxStt = @FaxSC, ");
                    sql.AppendLine("FaxCC = @FaxCC, ");
                    sql.AppendLine("Fax = @Fax, ");
                    sql.AppendLine("ModifiedBy = @UserID,");
                    sql.AppendLine("ModifiedByName = @UserName,");
                    sql.AppendLine("ModifiedDate = getdate(),");
                    sql.AppendLine("AddressTypeID = @AddressTypeID, ");
                    //Aryo20120109 set default value Address type to ‘Headquarters’ 
                    sql.AppendLine("Master = 1 ");
                    //Aryo20120109 set Master to address
                    sql.AppendLine("WHERE AddressID = @AddressID");
                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;
                    com.Parameters.Add(new SqlParameter("@AddressID", AddressID));
                    com.Parameters.Add(new SqlParameter("@Address1", SyncHelper.ReturnNull(Address1)));
                    com.Parameters.Add(new SqlParameter("@Address2", SyncHelper.ReturnNull(Address2)));
                    com.Parameters.Add(new SqlParameter("@Address3", SyncHelper.ReturnNull(Address3)));
                    com.Parameters.Add(new SqlParameter("@City", SyncHelper.ReturnNull(City)));
                    com.Parameters.Add(new SqlParameter("@PostCode", SyncHelper.ReturnNull(PostCode)));
                    com.Parameters.Add(new SqlParameter("@State", SyncHelper.ReturnNull(State)));
                    com.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));
                    com.Parameters.Add(new SqlParameter("@AddressTypeID", BOL.Common.Modules.Parameter.DEFAULT_ADDRESS_TYPE));

                    com.ExecuteNonQuery();
                    #region "NO NEED TO SYNC TO AQIR"
                    //AccountContact odsAccount = new AccountContact();
                    //SqlCommand comSTG = new SqlCommand();
                    //StringBuilder sqlSTG = new StringBuilder();
                    //sqlSTG.Length = 0;
                    //sqlSTG.AppendLine("INSERT INTO AddressSTG");
                    //sqlSTG.AppendLine("SELECT * FROM Address WHERE AddressID = @AddressID");
                    //sqlSTG.AppendLine("UPDATE AddressSTG SET ");
                    //sqlSTG.AppendLine("Address1 = @Address1,");
                    //sqlSTG.AppendLine("Address2 = @Address2,");
                    //sqlSTG.AppendLine("Address3 = @Address3,");
                    //sqlSTG.AppendLine("City = @City,");
                    //sqlSTG.AppendLine("PostCode = @PostCode,");
                    //sqlSTG.AppendLine("State = @State,");
                    //sqlSTG.AppendLine("CountryRegionID = @CountryRegionID,");
                    //sqlSTG.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode, ");
                    //sqlSTG.AppendLine("BusinessPhoneStt = @BusinessPhoneSC, ");
                    //sqlSTG.AppendLine("BusinessPhoneCC = @BusinessPhoneCC, ");
                    //sqlSTG.AppendLine("BusinessPhone = @BusinessPhone, ");
                    //sqlSTG.AppendLine("BusinessPhoneExt = @BusinessPhoneExt, ");
                    //sqlSTG.AppendLine("FaxCtry = @FaxCountryCode, ");
                    //sqlSTG.AppendLine("FaxStt = @FaxSC, ");
                    //sqlSTG.AppendLine("FaxCC = @FaxCC, ");
                    //sqlSTG.AppendLine("Fax = @Fax, ");
                    //sqlSTG.AppendLine("ModifiedBy = @UserID,");
                    //sqlSTG.AppendLine("ModifiedByName = @UserName,");
                    //sqlSTG.AppendLine("ModifiedDate = getdate(),");
                    //sqlSTG.AppendLine("AddressTypeID = @AddressTypeID, ");
                    ////Aryo20120109 set default value Address type to ‘Headquarters’ 
                    //sqlSTG.AppendLine("Master = 1 ");
                    ////Aryo20120109 set Master to address
                    //sqlSTG.AppendLine("WHERE AddressID = @AddressID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.Add(new SqlParameter("@AddressID", AddressID));
                    //comSTG.Parameters.Add(new SqlParameter("@Address1", SyncHelper.ReturnNull(Address1)));
                    //comSTG.Parameters.Add(new SqlParameter("@Address2", SyncHelper.ReturnNull(Address2)));
                    //comSTG.Parameters.Add(new SqlParameter("@Address3", SyncHelper.ReturnNull(Address3)));
                    //comSTG.Parameters.Add(new SqlParameter("@City", SyncHelper.ReturnNull(City)));
                    //comSTG.Parameters.Add(new SqlParameter("@PostCode", SyncHelper.ReturnNull(PostCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@State", SyncHelper.ReturnNull(State)));
                    //comSTG.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    //comSTG.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    //comSTG.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    //comSTG.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));
                    //comSTG.Parameters.Add(new SqlParameter("@AddressTypeID", GetParamValue(Parameter.DEFAULT_ADDRESS_TYPE)));

                    //comSTG.ExecuteNonQuery();

                    ////To Update address in AQIR via WebService
                    //string Result = odsAccount.UpdateAQIRAddress_Wizard(Connection, Transaction, AccountID, AddressID, new Guid(SyncHelper.AQIRAdminID));

                    //XmlDocument Doc = new XmlDocument();
                    //string XMLForAddress = odsAccount.GetXMLForAddress_Wizard(Connection, Transaction, AccountID, AddressID, new Guid(SyncHelper.AQIRAdminID));
                    //XmlDocument XMLDoc = new XmlDocument();
                    //Console.WriteLine(XMLForAddress);
                    //Doc.LoadXml(Result);
                    //XMLDoc.LoadXml(XMLForAddress);
                    //Console.WriteLine(Doc.ToString());
                    //bool Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);

                    //if (Success)
                    //{
                    //    odsAccount.MoveAddressSTGToActualAddressUpdate_Wizard(Connection, Transaction, AddressID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateAddress", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //}
                    //else {
                    //    odsAccount.DeleteAddressSTG_Wizard(Connection, Transaction, AddressID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateAddress", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    throw new Exception("Updating new address failed in AQIR.");
                    //}
                    #endregion
                    //Company Change Log - Address
                    DataRow newLogData = alMgr.SelectAccountForLog_Address_Wizard(Connection, Transaction, guidAddressID);
                    if (currentLogData != null && newLogData != null)
                    {
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        alMgr.CreateAccountLog_Address_Wizard(Connection, Transaction, guidAddressID, guidAccountID, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                }
                catch (Exception ex)
                {
                    throw;
                }
                finally
                {
                    //con.Close()
                }
            }
            else
            {
                Console.WriteLine("14");
                try
                {
                    //con.Open()

                    AddressID = Guid.NewGuid();

                    SqlCommand com = new SqlCommand();
                    System.Text.StringBuilder sql = new System.Text.StringBuilder();
                    sql.Length = 0;
                    sql.AppendLine("INSERT INTO Address ");
                    sql.AppendLine("(");
                    sql.AppendLine("AddressID, OwnerName, OwnerID, Address1, Address2, Address3, City, PostCode, State, CountryRegionID, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, FaxCtry, FaxStt, FaxCC, Fax,");
                    sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate, AddressTypeID, Master, DataSource");
                    //Aryo20120109 set default value Address type to ‘Headquarters’ and set Master to address
                    sql.AppendLine(")");
                    sql.AppendLine("VALUES(");
                    sql.AppendLine("@AddressID, @OwnerName, @OwnerID, @Address1, @Address2, @Address3, @City, @PostCode, @State, @CountryRegionID, @BusinessPhoneCountryCode, @BusinessPhoneSC, @BusinessPhoneCC, @BusinessPhone, @BusinessPhoneExt, @FaxCountryCode, @FaxSC, @FaxCC, @Fax,");
                    sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate(), @AddressTypeID, 1, 'WIZ'");
                    sql.AppendLine(")");
                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;
                    com.Parameters.Add(new SqlParameter("@AddressID", AddressID));
                    com.Parameters.Add(new SqlParameter("@OwnerName", "Account"));
                    com.Parameters.Add(new SqlParameter("@OwnerID", AccountID));
                    com.Parameters.Add(new SqlParameter("@Address1", SyncHelper.ReturnNull(Address1)));
                    com.Parameters.Add(new SqlParameter("@Address2", SyncHelper.ReturnNull(Address2)));
                    com.Parameters.Add(new SqlParameter("@Address3", SyncHelper.ReturnNull(Address3)));
                    com.Parameters.Add(new SqlParameter("@City", SyncHelper.ReturnNull(City)));
                    com.Parameters.Add(new SqlParameter("@PostCode", SyncHelper.ReturnNull(PostCode)));
                    com.Parameters.Add(new SqlParameter("@State", SyncHelper.ReturnNull(State)));
                    com.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));
                    com.Parameters.Add(new SqlParameter("@AddressTypeID", SyncHelper.ReturnNull(BOL.Common.Modules.Parameter.DEFAULT_ADDRESS_TYPE)));

                    com.ExecuteNonQuery();

                    #region "NO MORE AQIR SYNC"
                    //AccountContact odsAccount = new AccountContact();
                    //SqlCommand comSTG = new SqlCommand();
                    //StringBuilder sqlSTG = new StringBuilder();
                    //sqlSTG.Length = 0;
                    //sqlSTG.AppendLine("INSERT INTO AddressSTG ");
                    //sqlSTG.AppendLine("(");
                    //sqlSTG.AppendLine("AddressID, OwnerName, OwnerID, Address1, Address2, Address3, City, PostCode, State, CountryRegionID, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, FaxCtry, FaxStt, FaxCC, Fax,");
                    //sqlSTG.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate, AddressTypeID, Master, DataSource");
                    ////Aryo20120109 set default value Address type to ‘Headquarters’ and set Master to address
                    //sqlSTG.AppendLine(")");
                    //sqlSTG.AppendLine("VALUES(");
                    //sqlSTG.AppendLine("@AddressID, @OwnerName, @OwnerID, @Address1, @Address2, @Address3, @City, @PostCode, @State, @CountryRegionID, @BusinessPhoneCountryCode, @BusinessPhoneSC, @BusinessPhoneCC, @BusinessPhone, @BusinessPhoneExt, @FaxCountryCode, @FaxSC, @FaxCC, @Fax,");
                    //sqlSTG.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate(), @AddressTypeID, 1, 'WIZ'");
                    //sqlSTG.AppendLine(")");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.Add(new SqlParameter("@AddressID", AddressID));
                    //comSTG.Parameters.Add(new SqlParameter("@OwnerName", "Account"));
                    //comSTG.Parameters.Add(new SqlParameter("@OwnerID", AccountID));
                    //comSTG.Parameters.Add(new SqlParameter("@Address1", SyncHelper.ReturnNull(Address1)));
                    //comSTG.Parameters.Add(new SqlParameter("@Address2", SyncHelper.ReturnNull(Address2)));
                    //comSTG.Parameters.Add(new SqlParameter("@Address3", SyncHelper.ReturnNull(Address3)));
                    //comSTG.Parameters.Add(new SqlParameter("@City", SyncHelper.ReturnNull(City)));
                    //comSTG.Parameters.Add(new SqlParameter("@PostCode", SyncHelper.ReturnNull(PostCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@State", SyncHelper.ReturnNull(State)));
                    //comSTG.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    //comSTG.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    //comSTG.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    //comSTG.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    //comSTG.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    //comSTG.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));
                    //comSTG.Parameters.Add(new SqlParameter("@AddressTypeID", SyncHelper.ReturnNull(GetParamValue(Parameter.DEFAULT_ADDRESS_TYPE))));

                    //comSTG.ExecuteNonQuery();
                    //Console.WriteLine("15");
                    ////To Insert address in AQIR via WebService
                    //string Result = odsAccount.UpdateAQIRAddress_Wizard(Connection, Transaction, AccountID, AddressID, new Guid(SyncHelper.AQIRAdminID));
                    //string XMLForAddress = odsAccount.GetXMLForAddress_Wizard(Connection, Transaction, AccountID, AddressID, new Guid(SyncHelper.AQIRAdminID));
                    //XmlDocument XMLDoc = new XmlDocument();
                    //XmlDocument Doc = new XmlDocument();
                    //Console.WriteLine("15");
                    //Console.WriteLine(XMLForAddress);
                    //XMLDoc.LoadXml(XMLForAddress);
                    //Doc.LoadXml(Result);
                    //Console.WriteLine(Result);
                    //bool Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //Console.WriteLine(Success.ToString());
                    //string AQIRID = string.Empty;
                    //Console.WriteLine(AQIRID);
                    ////End Update
                    //if (Success)
                    //{
                    //    AQIRID = Doc.SelectSingleNode("//MSCFile/AddressDetails/AQIRID").InnerText;
                    //    Console.WriteLine("16");
                    //    odsAccount.UpdateAddressAQIRID_Wizard(Connection, Transaction, AQIRID, AddressID);
                    //    odsAccount.MoveAddressSTGToActualAddressInsert_Wizard(Connection, Transaction, AddressID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertAddress", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //}
                    //else {
                    //    Console.WriteLine("17");
                    //    odsAccount.DeleteAddressSTG_Wizard(Connection, Transaction, AddressID);
                    //    Console.WriteLine("Inserting new address failed in AQIR.");
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertAddress", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    throw new Exception("Inserting new address failed in AQIR.");
                    //}
                    #endregion
                    //Company Change Log - Address
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    Guid guidAddressID = new Guid(AddressID.ToString());
                    DataRow newLogData = alMgr.SelectAccountForLog_Address_Wizard(Connection, Transaction, guidAddressID);
                    alMgr.CreateAccountLog_Address_Wizard(Connection, Transaction, guidAddressID, guidAccountID, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }
            }
        }


        private static Nullable<Guid> GetAccountAddressID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? AddressTypeID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            StringBuilder sql = new StringBuilder();
            sql.AppendLine("SELECT AddressID");
            sql.AppendLine("FROM Address");
            sql.AppendLine("WHERE OwnerID = @OwnerID");
            sql.AppendLine("AND OwnerName = 'Account'");
            sql.AppendLine("AND AddressTypeID = @AddressTypeID");


            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.CommandTimeout = int.MaxValue;
            com.Transaction = Transaction;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@OwnerID", AccountID));
                com.Parameters.Add(new SqlParameter("@AddressTypeID", AddressTypeID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }

        private static void ACApproved_CreateUpdateAccountContact(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? DesignationCID, string DesignationName, string Name, string Email, string BusinessPhoneCountryCode, string BusinessPhoneSC, string BusinessPhoneAC,

string BusinessPhoneCC, string BusinessPhone, string BusinessPhoneExt, string MobilePhoneCountryCode, string MobilePhoneCC, string MobilePhone, string FaxCountryCode, string FaxSC, string FaxCC, string Fax)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            Nullable<Guid> ContactID = GetAccountContactID(Connection, Transaction, AccountID, DesignationCID);

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            BOL.AccountContact.odsAccount mgrAccount = new BOL.AccountContact.odsAccount();
            string OffCallingCode = string.Empty;
            string FaxCallingCode = string.Empty;

            if (!string.IsNullOrEmpty(BusinessPhoneCC) && !string.IsNullOrEmpty(BusinessPhoneSC))
            {
                OffCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, BusinessPhoneSC, BusinessPhoneCC);
                BusinessPhoneCC = OffCallingCode;
            }

            if (!string.IsNullOrEmpty(FaxCC) && !string.IsNullOrEmpty(FaxSC))
            {
                FaxCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, FaxSC, FaxCC);
                FaxCC = FaxCallingCode;
            }

            if (ContactID.HasValue && DesignationName != "Others")
            {
                try
                {
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow currentData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);

                    sql.AppendLine("UPDATE Contact SET ");
                    sql.AppendLine("Name = @Name,");
                    sql.AppendLine("Email = @Email,");
                    sql.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode,");
                    sql.AppendLine("BusinessPhoneStt = @BusinessPhoneSC,");
                    sql.AppendLine("BusinessPhoneCC = @BusinessPhoneCC,");
                    sql.AppendLine("BusinessPhone = @BusinessPhone,");
                    sql.AppendLine("BusinessPhoneExt = @BusinessPhoneExt,");
                    sql.AppendLine("MobilePhoneCtry = @MobilePhoneCountryCode,");
                    sql.AppendLine("MobilePhoneCC = @MobilePhoneCC,");
                    sql.AppendLine("MobilePhone = @MobilePhone,");
                    sql.AppendLine("FaxCtry = @FaxCountryCode,");
                    sql.AppendLine("FaxStt = @FaxSC,");
                    sql.AppendLine("FaxCC = @FaxCC,");
                    sql.AppendLine("Fax = @Fax,");
                    sql.AppendLine("ModifiedBy = @UserID,");
                    sql.AppendLine("ModifiedByName = @UserName,");
                    sql.AppendLine("ModifiedDate = getdate(),");
                    sql.AppendLine("DataSource = '" + SyncHelper.DataSource + "',");
                    sql.AppendLine("MSCKeyContact = 1");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine("WHERE ContactID = @ContactID");

                    com.Parameters.Add(new SqlParameter("@ContactID", ContactID));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC)));
                    com.Parameters.Add(new SqlParameter("@MobilePhone", SyncHelper.ReturnNull(MobilePhone)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;



                    //con.Open()
                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    //NO3
                    //SqlCommand comSTG = new SqlCommand();
                    //StringBuilder sqlSTG = new StringBuilder();
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactID, AccountID, Name, SalutationCID, DesignationCID, DesignationName, Department, ReportToContactID, Role, ContactStatus, Email, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone,");
                    //sqlSTG.AppendLine("BusinessPhoneExt, MobilePhoneCtry, MobilePhoneCC, MobilePhone, FaxCtry, FaxStt, FaxCC, Fax, IMAddress, SkypeName, ContactCategoryCID, Gender, DateOfBirth, RaceCID, SpouseName, Anniversary, AccessMode, KeyContact,");
                    //sqlSTG.AppendLine(" MSCKeyContact,  CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,Published,OtherEmail, OtherMobilePhone, OtherBusinessPhone,OtherFax, DataSource,  Deleted,  CREATEDDATEMIGRATE,ContactTypeCID,");
                    //sqlSTG.AppendLine("CEOEmailFlag, AQIRContact, MGSContact,AQIRID,ContactClassificationID,PAName, MDeCPrimaryContact,MDeCBackupContact, AQIRSyncTimestamp");
                    //sqlSTG.AppendLine("FROM Contact WHERE ContactID = @ContactID");
                    //sqlSTG.AppendLine("UPDATE ContactSTG");
                    //sqlSTG.AppendLine("  SET Name=@Name");
                    //sqlSTG.AppendLine("  , Email=@Email");
                    //sqlSTG.AppendLine("  , BusinessPhoneCtry=@BusinessPhoneCountryCode");
                    //sqlSTG.AppendLine("  , BusinessPhoneStt=@BusinessPhoneStateCode");
                    //sqlSTG.AppendLine("  , BusinessPhoneCC=@BusinessPhoneCC");
                    //sqlSTG.AppendLine("  , BusinessPhone=@BusinessPhone");
                    //sqlSTG.AppendLine("  , BusinessPhoneExt=@BusinessPhoneExt");
                    //sqlSTG.AppendLine("  , MobilePhoneCtry=@MobileCountryCode");
                    //sqlSTG.AppendLine("  , MobilePhoneCC=@MobilePhoneCC");
                    //sqlSTG.AppendLine("  , MobilePhone=@MobilePhone");
                    //sqlSTG.AppendLine("  , FaxCtry=@FaxCountryCode");
                    //sqlSTG.AppendLine("  , FaxStt=@FaxStateCode");
                    //sqlSTG.AppendLine("  , FaxCC=@FaxCC");
                    //sqlSTG.AppendLine("  , Fax=@Fax");
                    //sqlSTG.AppendLine("  , ModifiedDate=@ActionDate");
                    //sqlSTG.AppendLine("  , ModifiedBy=@ActionBy");
                    //sqlSTG.AppendLine("  , ModifiedByName=@ActionByName");
                    //sqlSTG.AppendLine("WHERE ContactID=@ContactID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.Parameters.AddWithValue("@ContactID", SyncHelper.ReturnNull(ContactID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneStateCode", SyncHelper.ReturnNull(BusinessPhoneSC));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC));
                    //comSTG.Parameters.AddWithValue("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt));
                    //comSTG.Parameters.AddWithValue("@MobileCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode));
                    //comSTG.Parameters.AddWithValue("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC));
                    //comSTG.Parameters.AddWithValue("@MobilePhone", SyncHelper.ReturnNull(MobilePhone));
                    //comSTG.Parameters.AddWithValue("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode));
                    //comSTG.Parameters.AddWithValue("@FaxStateCode", SyncHelper.ReturnNull(FaxSC));
                    //comSTG.Parameters.AddWithValue("@FaxCC", SyncHelper.ReturnNull(FaxCC));
                    //comSTG.Parameters.AddWithValue("@Fax", SyncHelper.ReturnNull(Fax));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);

                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.ExecuteNonQuery();

                    ////To update contact in AQIR via WebService
                    //string Result = string.Empty;
                    //bool Success = false;

                    //if (mgrContact.IsAQIRContactSTG_Wizard(Connection, Transaction, ContactID))
                    //{
                    //    Result = mgrContact.UpdateAQIRContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID));
                    //    string AQIRID = mgrContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactID);
                    //    string XMLForContact = mgrContact.GetXMLForContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //    XmlDocument XMLDoc = new XmlDocument();

                    //    XmlDocument Doc = new XmlDocument();
                    //    XMLDoc.LoadXml(XMLForContact);

                    //    Doc.LoadXml(Result);
                    //    Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //    if (Success)
                    //    {
                    //        mgrContact.MoveContactSTGToActualContactUpdate_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContact", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    }
                    //    else {
                    //        mgrContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactID);
                    //        mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContact", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //        throw new Exception("Updating new contact failed in AQIR.");
                    //    }
                    //}
                    ////End update
                    #endregion
                    //Log
                    DataRow newData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);
                    if (currentData != null && newData != null)
                    {
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactID.Value, guidAccountID, currentData, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }

                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }

            }
            else
            {
                CodeMaster mgr = new CodeMaster();
                Guid ContactTypeCID = mgr.GetCodeMasterID_Wizard(Connection, Transaction, BOL.AppConst.CodeType.ContactType, "Key Contact");

                try
                {

                    //con.Open()
                    //NO4
                    sql.AppendLine("INSERT INTO Contact");
                    sql.AppendLine("(");
                    sql.AppendLine("ContactID, AccountID, DesignationCID, DesignationName, Name, Email, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, MobilePhoneCtry, MobilePhoneCC, MobilePhone, FaxCtry, FaxStt, FaxCC, Fax, ContactTypeCID, Published, ContactStatus,");
                    sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate, DataSource, MSCKeyContact, Deleted");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine(")");
                    sql.AppendLine("VALUES(");
                    sql.AppendLine("@ContactID, @AccountID, @DesignationCID, @DesignationName, @Name, @Email, @BusinessPhoneCountryCode, @BusinessPhoneSC, @BusinessPhoneCC, @BusinessPhone, @BusinessPhoneExt, @MobilePhoneCountryCode, @MobilePhoneCC, @MobilePhone, @FaxCountryCode, @FaxSC, @FaxCC, @Fax, @ContactTypeCID, @Published, @ContactStatus,");
                    sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate(), @DataSource, 1, 'N'");
                    sql.AppendLine(")");

                    ContactID = Guid.NewGuid();
                    com.Parameters.Add(new SqlParameter("@ContactID", ContactID.Value));
                    com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                    com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));
                    com.Parameters.Add(new SqlParameter("@DesignationName", DesignationName));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC)));
                    com.Parameters.Add(new SqlParameter("@MobilePhone", SyncHelper.ReturnNull(MobilePhone)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@ContactTypeCID", ContactTypeCID));
                    com.Parameters.Add(new SqlParameter("@Published", true));
                    com.Parameters.Add(new SqlParameter("@ContactStatus", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    ////To update contact in AQIR via WebService
                    //string Result = mgrContact.UpdateAQIRContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID));
                    //string AQIRID = mgrContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactID);
                    //XmlDocument Doc = new XmlDocument();
                    //XmlDocument XMLDoc = new XmlDocument();

                    //string XMLForContact = mgrContact.GetXMLForContact_Wizard(Connection, Transaction, AccountID, ContactID, new Guid(SyncHelper.AQIRAdminID), AQIRID);

                    //XMLDoc.LoadXml(XMLForContact);
                    //Doc.LoadXml(Result);
                    //bool Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //AQIRID = Doc.SelectSingleNode("//MSCFile/MainAqirID").InnerText;

                    //if (Success)
                    //{
                    //    mgrContact.UpdateContactAQIRID_Wizard(Connection, Transaction, AQIRID, ContactID);
                    //    mgrContact.MoveContactSTGToActualContactInsert_Wizard(Connection, Transaction, ContactID);
                    //    mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContact", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //}
                    //else {
                    //    mgrContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactID);
                    //    mgrAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContact", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    throw new Exception("Inserting new contact failed in AQIR.");
                    //}
                    //End Update
                    #endregion
                    //Log
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newData = alMgr.SelectAccountForLog_Contact_Wizard(Connection, Transaction, ContactID.Value);
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactID.Value, guidAccountID, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {

                }
            }
        }

        private static Nullable<Guid> GetAccountContactID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? DesignationCID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT ContactID");
            sql.AppendLine("FROM Contact");
            sql.AppendLine("WHERE AccountID = @AccountID");
            sql.AppendLine("AND DesignationCID = @DesignationCID");
            sql.AppendLine("AND Deleted = 'N'");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }

        private static void CreateACApprovedAccountHistory(SqlConnection Connection, SqlTransaction Transaction, int? MeetingNo, string MSCFileID, string WizardXMLData)
        {
            //WizardXMLData = ReplaceInvalidChar(WizardXMLData);
            //WizardXMLData= System.Security.SecurityElement.Escape(WizardXMLData);
            WizardXMLData = WizardXMLData.Replace("&#xB", "");
            SqlCommand com = new SqlCommand();
            StringBuilder sql = new StringBuilder();

            sql.AppendLine("INSERT INTO ACApprovedAccountHistory ");
            sql.AppendLine("(");
            sql.AppendLine("MeetingNo, MSCFileID, WizardXMLData, ");
            sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate, SubmitType");
            sql.AppendLine(")");
            sql.AppendLine("VALUES(");
            sql.AppendLine("@MeetingNo, @MSCFileID, @WizardXMLData,");
            sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate(), 'S' ");
            sql.AppendLine(")");

            com.Parameters.Add(new SqlParameter("@MeetingNo", MeetingNo));
            com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));
            com.Parameters.Add(new SqlParameter("@WizardXMLData", WizardXMLData));
            com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            try
            {
                //con.Open()
                com.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //	con.Close()
            }
        }

        private static string ReplaceInvalidChar(string wizardXMLData)
        {
            string FilteredXML = "";
            FilteredXML = wizardXMLData.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;").Replace("'", "&apos;");
            return FilteredXML;
        }

        private static void CreateMSCChangesHistory(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? MSCChangesCID)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            sql.AppendLine("INSERT INTO MSCChangesHistory ");
            sql.AppendLine("(");
            sql.AppendLine("MSCChangesHistoryID, AccountID, MSCChangesCID, MSCChangesDate, ");
            sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");
            sql.AppendLine(")");
            sql.AppendLine("VALUES(");
            sql.AppendLine("NEWID(), @AccountID, @MSCChangesCID, GETDATE(),");
            sql.AppendLine("@UserID, @UserName, GETDATE(), @UserID, @UserName, GETDATE()");
            sql.AppendLine(")");

            com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
            com.Parameters.Add(new SqlParameter("@MSCChangesCID", MSCChangesCID));
            com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            try
            {
                //con.Open()
                com.ExecuteNonQuery();
            }
            catch (Exception ex)
            {

            }
        }

        private static void ACApproved_CreateUpdateAccountRelocation(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID)
        {
            SqlCommand com = new SqlCommand();
            StringBuilder sql = new StringBuilder();

            CodeMaster mgr = new CodeMaster();
            Guid? RelocationStatus = mgr.GetCodeMasterID_Wizard(Connection, Transaction, BOL.AppConst.CodeType.RelocationStatus, "Under 6 Months Grace Period");

            Nullable<Guid> RelocationID = GetAccountRelocationID(Connection, Transaction, AccountID, RelocationStatus);

            if (RelocationID.HasValue)
            {
                BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                DataRow currentData = alMgr.SelectAccountForLog_Relocation_Wizard(Connection, Transaction, RelocationID.Value);

                sql.AppendLine("UPDATE Relocation SET ");
                sql.AppendLine("RelocationStatus = @RelocationStatus,");
                sql.AppendLine("ModifiedBy = @ModifiedBy,");
                sql.AppendLine("ModifiedByName = @ModifiedByName");
                sql.AppendLine("WHERE RelocationID = @RelocationID");
                //com.Parameters.Add(New SqlClient.SqlParameter("@RelocationID", RelocationID))

                com.Parameters.Add(new SqlParameter("@RelocationStatus", SyncHelper.ReturnNull(RelocationStatus)));
                com.Parameters.Add(new SqlParameter("@RelocationID", SyncHelper.ReturnNull(RelocationID)));
                com.Parameters.Add(new SqlParameter("@ModifiedBy", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@ModifiedByName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Log
                    DataRow newData = alMgr.SelectAccountForLog_Relocation_Wizard(Connection, Transaction, RelocationID.Value);
                    if (currentData != null && newData != null)
                    {
                        Guid guidAccountID = new Guid(AccountID.ToString());
                        alMgr.CreateAccountLog_Relocation_Wizard(Connection, Transaction, RelocationID.Value, guidAccountID, currentData, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }

                }
                catch (Exception ex)
                {
                }


            }
            else
            {
                sql.AppendLine("INSERT INTO Relocation ");
                sql.AppendLine("(");
                sql.AppendLine("RelocationID, AccountID, RelocationStatus, CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@ContactID, @AccountID, @RelocationStatus, @UserID, @UserName, GETDATE(), @UserID, @UserName, GETDATE()");
                sql.AppendLine(")");

                RelocationID = Guid.NewGuid();
                com.Parameters.Add(new SqlParameter("@ContactID", RelocationID.Value));
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@RelocationStatus", RelocationStatus));
                com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Log
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newData = alMgr.SelectAccountForLog_Relocation_Wizard(Connection, Transaction, RelocationID.Value);
                    Guid guidAccountID = new Guid(AccountID.ToString());
                    alMgr.CreateAccountLog_Relocation_Wizard(Connection, Transaction, RelocationID.Value, guidAccountID, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //	con.Close()
                }
            }
        }
        private static Nullable<Guid> GetAccountRelocationID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? RelocationStatus)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT RelocationID");
            sql.AppendLine("FROM Relocation");
            sql.AppendLine("WHERE AccountID = @AccountID");
            sql.AppendLine("AND RelocationStatus = @RelocationStatus");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
                com.Parameters.Add(new SqlParameter("@RelocationStatus", RelocationStatus));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
            }
        }

        private static void ACApproved_UpdateAccount(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? AccountTypeCID, string AccountName, string CompanyRegNo, Nullable<DateTime> DateOfIncorporation, int OperationStatus, Nullable<int> RequirementSpace, string PlanMoveTo,
Nullable<Guid> FinancialIncentiveCID, Nullable<decimal> Acc5YearsTax, string CoreActivities, string LeadGenerator, Nullable<DateTime> LeadSubmitDate, string BusinessPhoneCountryCode, string BusinessPhoneSC, string BusinessPhoneAC, string BusinessPhoneCC, string BusinessPhone,
string BusinessPhoneExt, string FaxCountryCode, string FaxSC, string FaxCC, string Fax, string WebSiteUrl, Nullable<Guid> AccountCategoryCID, Nullable<DateTime> MSCApprovedDate, string InstitutionName, string InstitutionType,
string InstituteURL, string SubmitType)
        {
            Guid guidAccountID = new Guid(AccountID.ToString());
            BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
            DataRow currentGeneralLogData = alMgr.SelectAccountForLog_General_Wizard(Connection, Transaction, guidAccountID);
            DataRow currentPortfolioLogData = alMgr.SelectAccountForLog_Portfolio_Wizard(Connection, Transaction, guidAccountID);
            DataRow currentRelocationPlanLogData = alMgr.SelectAccountForLog_RelocationPlan_Wizard(Connection, Transaction, guidAccountID);

            Console.WriteLine("3751");
            SqlCommand com = new SqlCommand();

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            string OffCallingCode = string.Empty;
            string FaxCallingCode = string.Empty;

            if (!string.IsNullOrEmpty(BusinessPhoneCC) && !string.IsNullOrEmpty(BusinessPhoneSC))
            {
                OffCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, BusinessPhoneSC, BusinessPhoneCC);
                BusinessPhoneCC = OffCallingCode;
            }

            Console.WriteLine("3752");
            if (!string.IsNullOrEmpty(FaxCC) && !string.IsNullOrEmpty(FaxSC))
            {
                FaxCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, FaxSC, FaxCC);
                FaxCC = FaxCallingCode;
            }

            Console.WriteLine("3771");
            //Fadly 20130905 --> combine BusinessPhoneCC,BusinessPhone,and BusinessPhoneExt and also FaxCC and Fax
            //Begin
            if (!string.IsNullOrEmpty(BusinessPhone) & !string.IsNullOrEmpty(BusinessPhoneCC))
            {
                BusinessPhoneCC = BusinessPhoneCC.Replace(";", "");
                BusinessPhone = BusinessPhone.Replace(";", "");
                BusinessPhoneExt = BusinessPhoneExt.Replace(";", "");

                if (!string.IsNullOrEmpty(BusinessPhoneExt))
                {
                    BusinessPhone = BusinessPhoneCC + "" + BusinessPhone + "x" + BusinessPhoneExt;
                }
                else
                {
                    BusinessPhone = BusinessPhoneCC.ToString() + "" + BusinessPhone.ToString();
                }
            }
            if (!string.IsNullOrEmpty(Fax) & !string.IsNullOrEmpty(FaxCC))
            {
                FaxCC = FaxCC.Replace(";", "");
                Fax = Fax.Replace(";", "");

                Fax = FaxCC + "" + Fax;
            }
            //End

            Console.WriteLine("3790");
            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("UPDATE Account SET ");
            sql.AppendLine("AccountTypeCID = @AccountTypeCID,");
            sql.AppendLine("AccountName = @AccountName,");
            sql.AppendLine("CompanyRegNo = @CompanyRegNo,");
            sql.AppendLine("DateOfIncorporation = @DateOfIncorporation,");
            //sql.AppendLine("OperationStatus = @OperationStatus,")
            //sql.AppendLine("JVCategoryCID = @JVCategoryCID,")
            sql.AppendLine("RequirementSpace = @RequirementSpace,");
            sql.AppendLine("PlanMoveTo = @PlanMoveTo,");
            sql.AppendLine("FinancialIncentiveCID = @FinancialIncentiveCID,");
            sql.AppendLine("Acc5YearsTax = @Acc5YearsTax,");
            if (SubmitType == "A")
            {
                sql.AppendLine("CoreActivities = ISNULL(CoreActivities,'')  + CHAR(13) + CHAR(10) + @CoreActivities,");
            }
            else
            {
                sql.AppendLine("CoreActivities = @CoreActivities,");
            }
            sql.AppendLine("LeadGenerator = @LeadGenerator,");
            sql.AppendLine("LeadSubmitDate = @LeadSubmitDate,");
            //sql.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode,")
            //sql.AppendLine("BusinessPhoneStt = @BusinessPhoneSC,")
            //sql.AppendLine("BusinessPhoneCC = @BusinessPhoneCC,")
            sql.AppendLine("BusinessPhone = @BusinessPhone,");
            //sql.AppendLine("BusinessPhoneExt = @BusinessPhoneExt,")
            //sql.AppendLine("FaxCtry = @FaxCountryCode,")
            //sql.AppendLine("FaxStt = @FaxSC,")
            //sql.AppendLine("FaxCC = @FaxCC,")
            sql.AppendLine("Fax = @Fax,");
            sql.AppendLine("WebSiteUrl = @WebSiteUrl,");
            sql.AppendLine("InstitutionName = @InstitutionName,");
            sql.AppendLine("InstitutionType = @InstitutionType,");
            sql.AppendLine("InstitutionURL = @InstituteURL,");
            //sql.AppendLine("AccountCategoryCID = @AccountCategoryCID,")
            sql.AppendLine("MSCApprovedDate = @MSCApprovedDate,");
            sql.AppendLine("ModifiedBy = @ModifiedBy,");
            sql.AppendLine("ModifiedByName = @ModifiedByName,");
            sql.AppendLine("ModifiedDate = @ModifiedDate,");
            sql.AppendLine("BursaMalaysiaCID = @BursaMalaysiaCID");
            //Aryo20120109 Set default value "No" for BursaMalaysia
            sql.AppendLine("WHERE AccountID = @AccountID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            com.Parameters.Add(new SqlParameter("@AccountID", AccountID));
            com.Parameters.Add(new SqlParameter("@AccountTypeCID", AccountTypeCID));
            com.Parameters.Add(new SqlParameter("@AccountName", SyncHelper.ReturnNull(AccountName)));
            com.Parameters.Add(new SqlParameter("@CompanyRegNo", SyncHelper.ReturnNull(CompanyRegNo)));
            com.Parameters.Add(new SqlParameter("@DateOfIncorporation", SyncHelper.ReturnNull(DateOfIncorporation)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@OperationStatus", OperationStatus))
            //com.Parameters.Add(New SqlClient.SqlParameter("@JVCategoryCID", SyncHelper.ReturnNull(JVCategoryCID)))
            com.Parameters.Add(new SqlParameter("@RequirementSpace", SyncHelper.ReturnNull(RequirementSpace)));
            com.Parameters.Add(new SqlParameter("@PlanMoveTo", SyncHelper.ReturnNull(PlanMoveTo)));
            com.Parameters.Add(new SqlParameter("@FinancialIncentiveCID", SyncHelper.ReturnNull(FinancialIncentiveCID)));
            com.Parameters.Add(new SqlParameter("@Acc5YearsTax", SyncHelper.ReturnNull(Acc5YearsTax)));
            com.Parameters.Add(new SqlParameter("@CoreActivities", SyncHelper.ReturnNull(CoreActivities)));
            com.Parameters.Add(new SqlParameter("@LeadGenerator", SyncHelper.ReturnNull(LeadGenerator)));
            com.Parameters.Add(new SqlParameter("@LeadSubmitDate", SyncHelper.ReturnNull(LeadSubmitDate)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneAC", SyncHelper.ReturnNull(BusinessPhoneAC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)))
            com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)))
            com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
            com.Parameters.Add(new SqlParameter("@WebSiteUrl", SyncHelper.ReturnNull(WebSiteUrl)));
            com.Parameters.Add(new SqlParameter("@InstitutionName", SyncHelper.ReturnNull(InstitutionName)));
            com.Parameters.Add(new SqlParameter("@InstitutionType", SyncHelper.ReturnNull(InstitutionType)));
            com.Parameters.Add(new SqlParameter("@InstituteURL", SyncHelper.ReturnNull(InstituteURL)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@AccountCategoryCID", SyncHelper.ReturnNull(AccountCategoryCID)))
            com.Parameters.Add(new SqlParameter("@MSCApprovedDate", SyncHelper.ReturnNull(MSCApprovedDate)));
            com.Parameters.Add(new SqlParameter("@ModifiedBy", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@ModifiedByName", SyncHelper.AdminName));
            com.Parameters.Add(new SqlParameter("@ModifiedDate", DateTime.Now));
            com.Parameters.Add(new SqlParameter("@BursaMalaysiaCID", BOL.Common.Modules.Parameter.DEFAULT_STCK_EXCHNGE));

            try
            {
                //con.Open()
                com.ExecuteNonQuery();
                BOL.AccountContact.odsAccount mgr = new BOL.AccountContact.odsAccount();
                //Update JV Category
                mgr.UpdateAccountJVCategory_Wizard(Connection, Transaction, guidAccountID, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                //Update BumiClassification
                mgr.UpdateAccountBumiClassification_Wizard(Connection, Transaction, guidAccountID, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                //Update Classification
                mgr.UpdateAccountClassification_Wizard(Connection, Transaction, guidAccountID, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                //Account Category
                if (AccountCategoryCID.HasValue)
                {
                    mgr.LogAccountCategory_Wizard(Connection, Transaction, guidAccountID, AccountCategoryCID.Value, DateTime.Now, new Guid(SyncHelper.AdminID), SyncHelper.AdminName, false, false);
                }

                //OperationalStatus
                mgr.LogOperationalStatusWZ_Wizard(Connection, Transaction, guidAccountID, OperationStatus, DateTime.Now, new Guid(SyncHelper.AdminID), SyncHelper.AdminName, false, false);

                //Log
                DataRow newGeneralLogData = alMgr.SelectAccountForLog_General_Wizard(Connection, Transaction, guidAccountID);
                DataRow newPortfolioLogData = alMgr.SelectAccountForLog_Portfolio_Wizard(Connection, Transaction, guidAccountID);
                DataRow newRelocationPlanLogData = alMgr.SelectAccountForLog_RelocationPlan_Wizard(Connection, Transaction, guidAccountID);

                if (currentGeneralLogData != null && newGeneralLogData != null)
                {
                    alMgr.CreateAccountLog_General_Wizard(Connection, Transaction, guidAccountID, currentGeneralLogData, newGeneralLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }

                if (currentPortfolioLogData != null && newPortfolioLogData != null)
                {
                    alMgr.CreateAccountLog_Portfolio_Wizard(Connection, Transaction, guidAccountID, currentPortfolioLogData, newPortfolioLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }

                if (currentRelocationPlanLogData != null && newRelocationPlanLogData != null)
                {
                    alMgr.CreateAccountLog_RelocationPlan_Wizard(Connection, Transaction, guidAccountID, currentRelocationPlanLogData, newRelocationPlanLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
            }
            catch (Exception ex)
            {

            }
        }

        public static DataRow GetAccountDetailsNameByAccountID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT AccountName, CoreActivities FROM Account WHERE AccountID = @AccountID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountID", AccountID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return dt.Rows[0];
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //	con.Close()
            }
        }

        private static void ACApproved_CreateUpdateAccountContactDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, Guid? DesignationCID, string DesignationName, string Name, string Email, string BusinessPhoneCountryCode, string BusinessPhoneSC, string BusinessPhoneAC,

string BusinessPhoneCC, string BusinessPhone, string BusinessPhoneExt, string MobilePhoneCountryCode, string MobilePhoneCC, string MobilePhone, string FaxCountryCode, string FaxSC, string FaxCC, string Fax)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            Guid? ContactDVID = GetAccountContactDVID(Connection, Transaction, AccountDVID, DesignationCID);

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            string OffCallingCode = string.Empty;
            string FaxCallingCode = string.Empty;

            if (!string.IsNullOrEmpty(BusinessPhoneCC) && !string.IsNullOrEmpty(BusinessPhoneSC))
            {
                OffCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, BusinessPhoneSC, BusinessPhoneCC);
                BusinessPhoneCC = OffCallingCode;
            }

            if (!string.IsNullOrEmpty(FaxCC) && !string.IsNullOrEmpty(FaxSC))
            {
                FaxCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, FaxSC, FaxCC);
                FaxCC = FaxCallingCode;
            }
            ///CEO - CFO - CTO - MD
            if (ContactDVID.HasValue && DesignationName != "Others")
            {
                try
                {
                    DataRow currentData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID.Value);

                    sql.AppendLine("UPDATE ContactDV SET ");
                    sql.AppendLine("Name = @Name,");
                    sql.AppendLine("Email = @Email,");
                    sql.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode,");
                    sql.AppendLine("BusinessPhoneStt = @BusinessPhoneSC,");
                    sql.AppendLine("BusinessPhoneCC = @BusinessPhoneCC,");
                    sql.AppendLine("BusinessPhone = @BusinessPhone,");
                    sql.AppendLine("BusinessPhoneExt = @BusinessPhoneExt,");
                    sql.AppendLine("MobilePhoneCtry = @MobilePhoneCountryCode,");
                    sql.AppendLine("MobilePhoneCC = @MobilePhoneCC,");
                    sql.AppendLine("MobilePhone = @MobilePhone,");
                    sql.AppendLine("FaxCtry = @FaxCountryCode,");
                    sql.AppendLine("FaxStt = @FaxSC,");
                    sql.AppendLine("FaxCC = @FaxCC,");
                    sql.AppendLine("Fax = @Fax,");
                    sql.AppendLine("DataSource = '" + SyncHelper.DataSource + "'");
                    //sql.AppendLine("MSCKeyContact = 1");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine("WHERE ContactDVID = @ContactDVID");

                    com.Parameters.Add(new SqlParameter("@ContactDVID", ContactDVID));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC)));
                    com.Parameters.Add(new SqlParameter("@MobilePhone", SyncHelper.ReturnNull(MobilePhone)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;


                    //con.Open()
                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    //Contact odsContact = new Contact();
                    //AccountContact odsAccount = new AccountContact();
                    ////NO5
                    //SqlCommand comSTG = new SqlCommand();
                    //System.Text.StringBuilder sqlSTG = new System.Text.StringBuilder();
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactDVID, ContactID, AccountDVID,AccountID,Name,SalutationCID,DesignationCID,DesignationName,Department,");
                    //sqlSTG.AppendLine("ReportToContactID,Role,ContactStatus,Email,BusinessPhoneCtry,BusinessPhoneStt,BusinessPhoneCC,BusinessPhone, BusinessPhoneExt,");
                    //sqlSTG.AppendLine("MobilePhoneCtry,MobilePhoneCC,MobilePhone,FaxCtry,FaxStt,FaxCC,Fax,IMAddress,SkypeName,ContactCategoryCID,Gender,DateOfBirth,RaceCID,");
                    //sqlSTG.AppendLine("SpouseName,Anniversary,AccessMode,KeyContact,MSCKeyContact,CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,");
                    //sqlSTG.AppendLine("Published,OtherEmail,OtherMobilePhone,OtherBusinessPhone,OtherFax, DataSource,Deleted,CREATEDDATEMIGRATE,ContactTypeCID,CEOEmailFlag,AQIRContact,");
                    //sqlSTG.AppendLine("MGSContact,AQIRID,ContactClassificationID,PAName,MDeCPrimaryContact,MDeCBackupContact,AQIRSyncTimestamp,AppSource,AppDateTime,HighestAuthority");
                    //sqlSTG.AppendLine("FROM ContactDV WHERE ContactDVID = @ContactDVID");
                    //sqlSTG.AppendLine("UPDATE ContactSTG");
                    //sqlSTG.AppendLine("  SET Name=@Name");
                    //sqlSTG.AppendLine("  , Email=@Email");
                    //sqlSTG.AppendLine("  , BusinessPhoneCtry=@BusinessPhoneCountryCode");
                    //sqlSTG.AppendLine("  , BusinessPhoneStt=@BusinessPhoneStateCode");
                    //sqlSTG.AppendLine("  , BusinessPhoneCC=@BusinessPhoneCC");
                    //sqlSTG.AppendLine("  , BusinessPhone=@BusinessPhone");
                    //sqlSTG.AppendLine("  , BusinessPhoneExt=@BusinessPhoneExt");
                    //sqlSTG.AppendLine("  , MobilePhoneCtry=@MobileCountryCode");
                    //sqlSTG.AppendLine("  , MobilePhoneCC=@MobilePhoneCC");
                    //sqlSTG.AppendLine("  , MobilePhone=@MobilePhone");
                    //sqlSTG.AppendLine("  , FaxCtry=@FaxCountryCode");
                    //sqlSTG.AppendLine("  , FaxStt=@FaxStateCode");
                    //sqlSTG.AppendLine("  , FaxCC=@FaxCC");
                    //sqlSTG.AppendLine("  , Fax=@Fax");
                    //sqlSTG.AppendLine("  , ModifiedDate=@ActionDate");
                    //sqlSTG.AppendLine("  , ModifiedBy=@ActionBy");
                    //sqlSTG.AppendLine("  , ModifiedByName=@ActionByName");
                    //sqlSTG.AppendLine("WHERE ContactID=@ContactDVID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.AddWithValue("@ContactDVID", SyncHelper.ReturnNull(ContactDVID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneStateCode", SyncHelper.ReturnNull(BusinessPhoneSC));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC));
                    //comSTG.Parameters.AddWithValue("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone));
                    //comSTG.Parameters.AddWithValue("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt));
                    //comSTG.Parameters.AddWithValue("@MobileCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode));
                    //comSTG.Parameters.AddWithValue("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC));
                    //comSTG.Parameters.AddWithValue("@MobilePhone", SyncHelper.ReturnNull(MobilePhone));
                    //comSTG.Parameters.AddWithValue("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode));
                    //comSTG.Parameters.AddWithValue("@FaxStateCode", SyncHelper.ReturnNull(FaxSC));
                    //comSTG.Parameters.AddWithValue("@FaxCC", SyncHelper.ReturnNull(FaxCC));
                    //comSTG.Parameters.AddWithValue("@Fax", SyncHelper.ReturnNull(Fax));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);

                    //comSTG.ExecuteNonQuery();
                    //string Result = string.Empty;
                    //bool Success = false;
                    ////To Update Contact in AQIR via WebService

                    //if (odsContact.IsAQIRContactSTG_Wizard(Connection, Transaction, ContactDVID))
                    //{
                    //    Result = odsContact.UpdateAQIRContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID));
                    //    string AQIRID = odsContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //    string XMLForContact = odsContact.GetXMLForContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //    XmlDocument XMLDoc = new XmlDocument();
                    //    Console.WriteLine(AQIRID);
                    //    Console.WriteLine(XMLForContact);
                    //    XmlDocument Doc = new XmlDocument();
                    //    XMLDoc.LoadXml(XMLForContact);

                    //    Doc.LoadXml(Result);
                    //    Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);

                    //    if (Success)
                    //    {
                    //        odsContact.MoveContactSTGToActualContactDVUpdate_Wizard(Connection, Transaction, ContactDVID);
                    //        odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContactDV", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    }
                    //    else {
                    //        odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //        odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContactDV", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //        throw new Exception("Updating new contactDV failed in AQIR.");
                    //    }

                    //}
                    //else {
                    //    odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //}
                    #endregion
                    //Log
                    DataRow newData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID.Value);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    if (currentData != null && newData != null)
                    {
                        Guid guidAccountDVID = new Guid(AccountDVID.ToString());
                        alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactDVID.Value, guidAccountDVID, currentData, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //con.Close()
                }
            }
            //OTHERS
            else
            {
                try
                {
                    CodeMaster mgr = new CodeMaster();
                    Guid ContactTypeCID = mgr.GetCodeMasterID_Wizard(Connection, Transaction, BOL.AppConst.CodeType.ContactType, "Key Contact");

                    sql.AppendLine("INSERT INTO ContactDV ");
                    sql.AppendLine("(");
                    sql.AppendLine("ContactDVID, AccountDVID, ContactID, DesignationCID, DesignationName, Name, Email, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, MobilePhoneCtry, MobilePhoneCC, MobilePhone, FaxCtry, FaxStt, FaxCC, Fax, ContactTypeCID, Published, ContactStatus, DataSource, MSCKeyContact, Deleted");
                    //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                    sql.AppendLine(")");
                    sql.AppendLine("VALUES(");
                    sql.AppendLine("@ContactDVID, @AccountDVID, NULL, @DesignationCID, @DesignationName, @Name, @Email, @BusinessPhoneCountryCode, @BusinessPhoneSC, @BusinessPhoneCC, @BusinessPhone, @BusinessPhoneExt, @MobilePhoneCountryCode, @MobilePhoneCC, @MobilePhone, @FaxCountryCode, @FaxCC, @FaxSC, @Fax, @ContactTypeCID, @Published, @ContactStatus, @DataSource, 1, 'N'");
                    sql.AppendLine(")");

                    ContactDVID = Guid.NewGuid();
                    com.Parameters.Add(new SqlParameter("@ContactDVID", ContactDVID));
                    com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                    com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));
                    com.Parameters.Add(new SqlParameter("@DesignationName", DesignationName));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
                    com.Parameters.Add(new SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCountryCode", SyncHelper.ReturnNull(MobilePhoneCountryCode)));
                    com.Parameters.Add(new SqlParameter("@MobilePhoneCC", SyncHelper.ReturnNull(MobilePhoneCC)));
                    com.Parameters.Add(new SqlParameter("@MobilePhone", SyncHelper.ReturnNull(MobilePhone)));
                    com.Parameters.Add(new SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)));
                    com.Parameters.Add(new SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)));
                    com.Parameters.Add(new SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)));
                    com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
                    com.Parameters.Add(new SqlParameter("@ContactTypeCID", ContactTypeCID));
                    com.Parameters.Add(new SqlParameter("@Published", true));
                    com.Parameters.Add(new SqlParameter("@ContactStatus", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
                    com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
                    com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;


                    //con.Open()
                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    ////NO6
                    //Contact odsContact = new Contact();
                    //SqlCommand comSTG = new SqlCommand();
                    //System.Text.StringBuilder sqlSTG = new System.Text.StringBuilder();
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactDVID, ContactID, AccountDVID,AccountID,Name,SalutationCID,DesignationCID,DesignationName,Department,");
                    //sqlSTG.AppendLine("ReportToContactID,Role,ContactStatus,Email,BusinessPhoneCtry,BusinessPhoneStt,BusinessPhoneCC,BusinessPhone, BusinessPhoneExt,");
                    //sqlSTG.AppendLine("MobilePhoneCtry,MobilePhoneCC,MobilePhone,FaxCtry,FaxStt,FaxCC,Fax,IMAddress,SkypeName,ContactCategoryCID,Gender,DateOfBirth,RaceCID,");
                    //sqlSTG.AppendLine("SpouseName,Anniversary,AccessMode,KeyContact,MSCKeyContact,CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,");
                    //sqlSTG.AppendLine("Published,OtherEmail,OtherMobilePhone,OtherBusinessPhone,OtherFax, DataSource,Deleted,CREATEDDATEMIGRATE,ContactTypeCID,CEOEmailFlag,AQIRContact,");
                    //sqlSTG.AppendLine("MGSContact,AQIRID,ContactClassificationID,PAName,MDeCPrimaryContact,MDeCBackupContact,AQIRSyncTimestamp");
                    //sqlSTG.AppendLine("FROM ContactDV WHERE ContactDVID = @ContactDVID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.AddWithValue("@ContactDVID", SyncHelper.ReturnNull(ContactDVID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);
                    //Console.WriteLine(com.CommandText);
                    //Console.WriteLine(comSTG.CommandText);
                    //comSTG.ExecuteNonQuery();

                    ////To update contact in AQIR via WebService
                    //string AQIRID = string.Empty;
                    //string Result = odsContact.UpdateAQIRContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID));
                    //AQIRID = odsContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactDVID);

                    //string XMLForContact = odsContact.GetXMLForContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //XmlDocument Doc = new XmlDocument();
                    //XmlDocument XMLDoc = new XmlDocument();

                    //Doc.LoadXml(Result);
                    //XMLDoc.LoadXml(XMLForContact);
                    //bool Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //AQIRID = Doc.SelectSingleNode("//MSCFile/MainAqirID").InnerText;

                    //AccountContact odsAccount = new AccountContact();

                    //if (Success)
                    //{
                    //    odsContact.DeleteContactDV_Wizard(Connection, Transaction, ContactDVID, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    odsContact.UpdateContactAQIRID_Wizard(Connection, Transaction, AQIRID, ContactDVID);
                    //    odsContact.MoveContactSTGToActualContactDVInsert_Wizard(Connection, Transaction, ContactDVID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContactDV", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AQIRAdminID), SyncHelper.AdminName);
                    //}
                    //else {
                    //    odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContactDV", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AQIRAdminID), SyncHelper.AdminName);
                    //    throw new Exception("Inserting new contactDV failed in AQIR.");
                    //}
                    #endregion
                    //Log
                    DataRow newData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID.Value);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactDVID.Value, AccountDVID.Value, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                }
            }
        }
        private static Nullable<Guid> GetAccountContactDVID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, Guid? DesignationCID)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            StringBuilder sql = new StringBuilder();
            sql.AppendLine("SELECT ContactDVID");
            sql.AppendLine("FROM ContactDV");
            sql.AppendLine("WHERE AccountDVID = @AccountDVID");
            sql.AppendLine("AND DesignationCID = @DesignationCID");
            sql.AppendLine("AND Deleted = 'N'");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
            }
        }
        private static void ACApproved_CreateUpdateAccountContactDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, Guid? DesignationCID, string DesignationName, string Name, string Email)
        {
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            Nullable<Guid> ContactDVID = GetAccountContactDVID(Connection, Transaction, AccountDVID, DesignationCID);
            //CEO - CFO - CTO - MD
            if (ContactDVID.HasValue && DesignationName != "Others")
            {
                try
                {
                    DataRow currentData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID.Value);
                    sql.AppendLine("UPDATE ContactDV SET ");
                    sql.AppendLine("Name = @Name,");
                    sql.AppendLine("Email = @Email,");
                    sql.AppendLine("DataSource = '" + SyncHelper.DataSource + "',");
                    //AQIR PIC Comment out on 11/3/2016 
                    //sql.AppendLine("MSCKeyContact = 1");
                    sql.AppendLine("WHERE ContactDVID = @ContactDVID");

                    com.Parameters.Add(new SqlParameter("@ContactDVID", ContactDVID));
                    com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                    com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;


                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    //SqlCommand comSTG = new SqlCommand();
                    //System.Text.StringBuilder sqlSTG = new System.Text.StringBuilder();
                    ////NO7
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactDVID, ContactID, AccountDVID,AccountID,Name,SalutationCID,DesignationCID,DesignationName,Department,");
                    //sqlSTG.AppendLine("ReportToContactID,Role,ContactStatus,Email,BusinessPhoneCtry,BusinessPhoneStt,BusinessPhoneCC,BusinessPhone, BusinessPhoneExt,");
                    //sqlSTG.AppendLine("MobilePhoneCtry,MobilePhoneCC,MobilePhone,FaxCtry,FaxStt,FaxCC,Fax,IMAddress,SkypeName,ContactCategoryCID,Gender,DateOfBirth,RaceCID,");
                    //sqlSTG.AppendLine("SpouseName,Anniversary,AccessMode,KeyContact,MSCKeyContact,CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,");
                    //sqlSTG.AppendLine("Published,OtherEmail,OtherMobilePhone,OtherBusinessPhone,OtherFax, DataSource,Deleted,CREATEDDATEMIGRATE,ContactTypeCID,CEOEmailFlag,AQIRContact,");
                    //sqlSTG.AppendLine("MGSContact,AQIRID,ContactClassificationID,PAName,MDeCPrimaryContact,MDeCBackupContact,AQIRSyncTimestamp");
                    //sqlSTG.AppendLine("FROM ContactDV WHERE ContactDVID = @ContactDVID");
                    //sqlSTG.AppendLine("UPDATE ContactSTG");
                    //sqlSTG.AppendLine("  SET Name=@Name");
                    //sqlSTG.AppendLine("  , Email=@Email");
                    //sqlSTG.AppendLine("  , ModifiedDate=@ActionDate");
                    //sqlSTG.AppendLine("  , ModifiedBy=@ActionBy");
                    //sqlSTG.AppendLine("  , ModifiedByName=@ActionByName");
                    //sqlSTG.AppendLine("WHERE ContactID=@ContactDVID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.AddWithValue("@ContactDVID", SyncHelper.ReturnNull(ContactDVID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);

                    //comSTG.ExecuteNonQuery();

                    //Contact odsContact = new Contact();
                    //AccountContact odsAccount = new AccountContact();
                    //string Result = string.Empty;
                    //bool Success = false;
                    ////To Update Contact in AQIR via WebService

                    //if (odsContact.IsAQIRContactSTG_Wizard(Connection, Transaction, ContactDVID))
                    //{
                    //    Result = odsContact.UpdateAQIRContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID));
                    //    string AQIRID = odsContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //    string XMLForContact = odsContact.GetXMLForContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //    XmlDocument XMLDoc = new XmlDocument();

                    //    XmlDocument Doc = new XmlDocument();
                    //    XMLDoc.LoadXml(XMLForContact);

                    //    Doc.LoadXml(Result);
                    //    Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);

                    //    if (Success)
                    //    {
                    //        odsContact.MoveContactSTGToActualContactDVUpdate_Wizard(Connection, Transaction, ContactDVID);
                    //        odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContactDV", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    }
                    //    else {
                    //        odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //        odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "UpdateContactDV", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //        throw new Exception("Updating new contactDV failed in AQIR.");
                    //    }
                    //}
                    //else {
                    //    odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //}
                    #endregion
                    //Log
                    DataRow newData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID.Value);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    if (currentData != null && newData != null)
                    {
                        alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactDVID.Value, AccountDVID.Value, currentData, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }
                }
                catch (Exception ex)
                {

                }
            }
            else
            {
                CodeMaster mgr = new CodeMaster();
                Guid ContactTypeCID = mgr.GetCodeMasterID_Wizard(Connection, Transaction, BOL.AppConst.CodeType.ContactType, "Key Contact");

                sql.AppendLine("INSERT INTO ContactDV ");
                sql.AppendLine("(");
                sql.AppendLine("ContactDVID, AccountDVID, DesignationCID, DesignationName, Name, Email, ContactTypeCID, Published, ContactStatus, DataSource, MSCKeyContact, Deleted");
                //Aryo 20120109 all contact from Wizard is treated as Key Contact for AQIR
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@ContactDVID, @AccountDVID, @DesignationCID, @DesignationName, @Name, @Email, @ContactTypeCID, @Published, @ContactStatus, @DataSource, 1, 'N'");
                sql.AppendLine(")");
                ContactDVID = Guid.NewGuid();
                com.Parameters.Add(new SqlParameter("@ContactDVID", ContactDVID));
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@DesignationCID", DesignationCID));
                com.Parameters.Add(new SqlParameter("@DesignationName", DesignationName));
                com.Parameters.Add(new SqlParameter("@Name", SyncHelper.ReturnNull(Name)));
                com.Parameters.Add(new SqlParameter("@Email", SyncHelper.ReturnNull(Email)));
                com.Parameters.Add(new SqlParameter("@ContactTypeCID", ContactTypeCID));
                com.Parameters.Add(new SqlParameter("@Published", true));
                com.Parameters.Add(new SqlParameter("@ContactStatus", EnumSync.Status.Active));
                com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;
                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();
                    #region "NO MORE AQIR SYNC"
                    //NO8
                    //Contact odsContact = new Contact();
                    //SqlCommand comSTG = new SqlCommand();
                    //System.Text.StringBuilder sqlSTG = new System.Text.StringBuilder();
                    //sqlSTG.AppendLine("INSERT INTO ContactSTG");
                    //sqlSTG.AppendLine("SELECT ContactDVID, ContactID, AccountDVID,AccountID,Name,SalutationCID,DesignationCID,DesignationName,Department,");
                    //sqlSTG.AppendLine("ReportToContactID,Role,ContactStatus,Email,BusinessPhoneCtry,BusinessPhoneStt,BusinessPhoneCC,BusinessPhone, BusinessPhoneExt,");
                    //sqlSTG.AppendLine("MobilePhoneCtry,MobilePhoneCC,MobilePhone,FaxCtry,FaxStt,FaxCC,Fax,IMAddress,SkypeName,ContactCategoryCID,Gender,DateOfBirth,RaceCID,");
                    //sqlSTG.AppendLine("SpouseName,Anniversary,AccessMode,KeyContact,MSCKeyContact,CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,");
                    //sqlSTG.AppendLine("Published,OtherEmail,OtherMobilePhone,OtherBusinessPhone,OtherFax, DataSource,Deleted,CREATEDDATEMIGRATE,ContactTypeCID,CEOEmailFlag,AQIRContact,");
                    //sqlSTG.AppendLine("MGSContact,AQIRID,ContactClassificationID,PAName,MDeCPrimaryContact,MDeCBackupContact,AQIRSyncTimestamp");
                    //sqlSTG.AppendLine("FROM ContactDV WHERE ContactDVID = @ContactDVID");
                    //comSTG.CommandText = sqlSTG.ToString();
                    //comSTG.CommandType = CommandType.Text;
                    //comSTG.Connection = Connection;
                    //comSTG.Transaction = Transaction;
                    //comSTG.CommandTimeout = int.MaxValue;
                    //comSTG.Parameters.AddWithValue("@ContactDVID", SyncHelper.ReturnNull(ContactDVID));
                    //comSTG.Parameters.AddWithValue("@Name", SyncHelper.ReturnNull(Name));
                    //comSTG.Parameters.AddWithValue("@Email", SyncHelper.ReturnNull(Email));
                    //comSTG.Parameters.AddWithValue("@ActionDate", SyncHelper.ReturnNull(System.DateTime.Now));
                    //comSTG.Parameters.AddWithValue("@ActionBy", SyncHelper.AdminID);
                    //comSTG.Parameters.AddWithValue("@ActionByName", SyncHelper.AdminName);
                    //Console.WriteLine(com.CommandText);
                    //Console.WriteLine(comSTG.CommandText);
                    //comSTG.ExecuteNonQuery();

                    ////To update contact in AQIR via WebService
                    //string AQIRID = string.Empty;
                    //string Result = odsContact.UpdateAQIRContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID));
                    //AQIRID = odsContact.GetAQIRIDContactSTG_Wizard(Connection, Transaction, ContactDVID);

                    //string XMLForContact = odsContact.GetXMLForContactDV_Wizard(Connection, Transaction, AccountDVID, ContactDVID, new Guid(SyncHelper.AQIRAdminID), AQIRID);
                    //XmlDocument Doc = new XmlDocument();
                    //XmlDocument XMLDoc = new XmlDocument();

                    //Doc.LoadXml(Result);
                    //XMLDoc.LoadXml(XMLForContact);
                    //bool Success = Convert.ToBoolean(Doc.SelectSingleNode("//MSCFile/Status").InnerText);
                    //AQIRID = Doc.SelectSingleNode("//MSCFile/MainAqirID").InnerText;

                    //AccountContact odsAccount = new AccountContact();

                    //if (Success)
                    //{
                    //    odsContact.DeleteContactDV_Wizard(Connection, Transaction, ContactDVID, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    //    odsContact.UpdateContactAQIRID_Wizard(Connection, Transaction, AQIRID, ContactDVID);
                    //    odsContact.MoveContactSTGToActualContactDVInsert_Wizard(Connection, Transaction, ContactDVID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContactDV", XMLDoc.OuterXml, "Y", new Guid(SyncHelper.AQIRAdminID), SyncHelper.AdminName);
                    //}
                    //else {
                    //    odsContact.DeleteContactSTG_Wizard(Connection, Transaction, ContactDVID);
                    //    odsAccount.CreateAQIRWSLog_Wizard(Connection, Transaction, "WizardSync", "InsertContactDV", XMLDoc.OuterXml, "N", new Guid(SyncHelper.AQIRAdminID), SyncHelper.AdminName);
                    //    throw new Exception("Inserting new contactDV failed in AQIR.");
                    //}
                    #endregion
                    //Log
                    DataRow newData = SelectAccountForLog_Contact(Connection, Transaction, ContactDVID);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    alMgr.CreateAccountLog_Contact_Wizard(Connection, Transaction, ContactDVID.Value, AccountDVID.Value, null, newData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                }
            }
        }
        private static void ACApproved_CreateUpdateBusinessAnalystAssignmentDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, string EEManagerName, Nullable<DateTime> AssignmentDate)
        {
            SqlCommand com = new SqlCommand();
            StringBuilder sql = new StringBuilder();
            Guid? BusinessAnalystTypeCID = SyncHelper.GetCodeMasterID(Connection, Transaction, AccountManagerType.BusinessAnalyst, BOL.AppConst.CodeType.AccountManagerType, true);

            Nullable<Guid> AccountManagerAssignmentDVID = GetBusinessAnalystDVID(Connection, Transaction, AccountDVID, EEManagerName);

            if (!AccountManagerAssignmentDVID.HasValue)
            {
                sql.AppendLine("INSERT INTO AccountManagerAssignmentDV ");
                sql.AppendLine("(");
                sql.AppendLine("AccountManagerAssignmentDVID, AccountDVID, AccountManagerAssignmentID, EEManagerName, AccountManagerTypeCID, DataSource, AssignmentDate");
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@AccountManagerAssignmentDVID, @AccountDVID, NULL, @EEManagerName, @AccountManagerTypeCID, @DataSource, @AssignmentDate");
                sql.AppendLine(")");

                AccountManagerAssignmentDVID = Guid.NewGuid();
                if (!AssignmentDate.HasValue)
                    AssignmentDate = DateTime.Now;
                com.Parameters.Add(new SqlParameter("@AccountManagerAssignmentDVID", AccountManagerAssignmentDVID));
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@EEManagerName", EEManagerName));
                com.Parameters.Add(new SqlParameter("@AccountManagerTypeCID", BusinessAnalystTypeCID));
                com.Parameters.Add(new SqlParameter("@DataSource", SyncHelper.DataSource));
                com.Parameters.Add(new SqlParameter("@AssignmentDate", AssignmentDate));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Company Change Log 
                    DataRow newLogData = SelectAccountForLog_EEManager(Connection, Transaction, AccountManagerAssignmentDVID);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    alMgr.CreateAccountLog_BusinessAnalyst_Wizard(Connection, Transaction, AccountManagerAssignmentDVID.Value, AccountDVID.Value, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                }
            }
            else
            {
                DataRow currentLogData = SelectAccountForLog_EEManager(Connection, Transaction, AccountManagerAssignmentDVID);

                sql.AppendLine("UPDATE AccountManagerAssignmentDV ");
                sql.AppendLine("SET AssignmentDate = @AssignmentDate, ");
                sql.AppendLine("    Active = 1 ");
                sql.AppendLine("WHERE AccountManagerAssignmentDVID = @AccountManagerAssignmentDVID");

                if (!AssignmentDate.HasValue)
                    AssignmentDate = DateTime.Now;
                com.Parameters.Add(new SqlParameter("@AccountManagerAssignmentDVID", AccountManagerAssignmentDVID));
                com.Parameters.Add(new SqlParameter("@AssignmentDate", AssignmentDate));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();

                    //Company Change Log 
                    DataRow newLogData = SelectAccountForLog_EEManager(Connection, Transaction, AccountManagerAssignmentDVID);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    alMgr.CreateAccountLog_BusinessAnalyst_Wizard(Connection, Transaction, AccountManagerAssignmentDVID.Value, AccountDVID.Value, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }
                catch (Exception ex)
                {
                }
            }
        }
        private static Nullable<Guid> GetBusinessAnalystDVID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, string EEManagerName)
        {

            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);
            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            Guid? PreMSCCID = SyncHelper.GetCodeMasterID(AccountManagerType.PreMSC, BOL.AppConst.CodeType.AccountManagerType, true);
            Guid? BusinessAnalystCID = SyncHelper.GetCodeMasterID(AccountManagerType.BusinessAnalyst, BOL.AppConst.CodeType.AccountManagerType, true);

            sql.AppendLine("SELECT AccountManagerAssignmentDVID");
            sql.AppendLine("FROM AccountManagerAssignmentDV");
            sql.AppendLine("WHERE AccountDVID = @AccountDVID");
            sql.AppendLine("AND EEManagerName = @EEManagerName");
            sql.AppendLine("AND (AccountManagerTypeCID = @BusinessAnalystCID");
            sql.AppendLine("OR AccountManagerTypeCID = @PreMSCCID)");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@EEManagerName", EEManagerName));
                com.Parameters.Add(new SqlParameter("@BusinessAnalystCID", BusinessAnalystCID));
                com.Parameters.Add(new SqlParameter("@PreMSCCID", PreMSCCID));

                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }
        private static void ACApproved_CreateUpdateShareholderDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, string ShareholderName, Nullable<decimal> Percentage, bool BumiShare, Nullable<Guid> CountryRegionID)
        {
            if (!string.IsNullOrEmpty(ShareholderName))
            {
                SqlCommand com = new SqlCommand();
                StringBuilder sql = new StringBuilder();
                Guid? ShareHolderDVID = GetShareHolderDVID(Connection, Transaction, AccountDVID, ShareholderName);

                if (ShareHolderDVID.HasValue)
                {
                    DataRow currentLogData = SelectAccountForLog_Shareholder(Connection, Transaction, ShareHolderDVID);

                    sql.AppendLine("UPDATE ShareHolderDV SET ");
                    sql.AppendLine("Percentage = @Percentage,");
                    sql.AppendLine("BumiShare = @BumiShare,");
                    sql.AppendLine("Status = @Status,");
                    sql.AppendLine("CountryRegionID = @CountryRegionID");
                    sql.AppendLine("WHERE ShareHolderDVID = @ShareHolderDVID");

                    com.Parameters.Add(new SqlParameter("@ShareHolderDVID", ShareHolderDVID));
                    com.Parameters.Add(new SqlParameter("@Percentage", SyncHelper.ReturnNull(Percentage)));
                    com.Parameters.Add(new SqlParameter("@BumiShare", BumiShare));
                    com.Parameters.Add(new SqlParameter("@Status", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    try
                    {
                        //con.Open()
                        com.ExecuteNonQuery();

                        //Company Change Log - Shareholder
                        DataRow newLogData = SelectAccountForLog_Shareholder(Connection, Transaction, ShareHolderDVID);
                        BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                        if (currentLogData != null && newLogData != null)
                        {
                            alMgr.CreateAccountLog_Shareholder_Wizard(Connection, Transaction, ShareHolderDVID.Value, AccountDVID.Value, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                }
                else
                {
                    sql.AppendLine("INSERT INTO ShareHolderDV ");
                    sql.AppendLine("(ShareHolderDVID, AccountDVID, ShareHolderID, ShareholderName, Percentage, BumiShare, Status, CountryRegionID)");
                    sql.AppendLine("VALUES");
                    sql.AppendLine("(@ShareHolderDVID, @AccountDVID, NULL, @ShareholderName, @Percentage, @BumiShare, @Status, @CountryRegionID)");

                    ShareHolderDVID = Guid.NewGuid();
                    com.Parameters.Add(new SqlParameter("@ShareHolderDVID", ShareHolderDVID));
                    com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                    com.Parameters.Add(new SqlParameter("@ShareholderName", ShareholderName));
                    com.Parameters.Add(new SqlParameter("@Percentage", SyncHelper.ReturnNull(Percentage)));
                    com.Parameters.Add(new SqlParameter("@BumiShare", BumiShare));
                    com.Parameters.Add(new SqlParameter("@Status", EnumSync.Status.Active));
                    com.Parameters.Add(new SqlParameter("@CountryRegionID", SyncHelper.ReturnNull(CountryRegionID)));

                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.Transaction = Transaction;
                    com.CommandTimeout = int.MaxValue;

                    try
                    {
                        //con.Open()
                        com.ExecuteNonQuery();

                        //Company Change Log - Shareholder
                        DataRow currentLogData = SelectAccountForLog_Shareholder(Connection, Transaction, ShareHolderDVID);
                        BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                        alMgr.CreateAccountLog_Shareholder_Wizard(Connection, Transaction, ShareHolderDVID.Value, AccountDVID.Value, null, currentLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                    }
                    catch (Exception ex)
                    {
                        throw;
                        //Finally
                        //	con.Close()
                    }
                }
            }
        }
        public static DataRow SelectAccountForLog_EEManager(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountManagerAssignmentDVID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT ama.EEManagerName AS [Account Manager], ");
                    sql.AppendLine("convert(varchar, ama.AssignmentDate, 106) AS [Created Date]");
                    sql.AppendLine("FROM AccountManagerAssignmentDV ama");
                    sql.AppendLine("WHERE ama.AccountManagerAssignmentDVID = @AccountManagerAssignmentDVID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@AccountManagerAssignmentDVID", AccountManagerAssignmentDVID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        public static DataRow SelectAccountForLog_Shareholder(SqlConnection Connection, SqlTransaction Transaction, Guid? ShareholderDVID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT s.ShareholderName AS [Shareholder Name], s.Percentage AS [Percentage Hold],");
                    sql.AppendLine("CASE s.BumiShare WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE '' END AS [Bumi Status],");
                    sql.AppendLine("r.RegionName AS [Country], CASE s.Status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' ELSE '' END AS [Status]");
                    sql.AppendLine("FROM ShareholderDV s");
                    sql.AppendLine("LEFT JOIN Region r ON r.RegionID = s.CountryRegionID");
                    sql.AppendLine("WHERE s.ShareholderDVID = @ShareholderDVID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@ShareholderDVID", ShareholderDVID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        private static Nullable<Guid> GetShareHolderDVID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, string ShareholderName)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT ShareHolderDVID");
            sql.AppendLine("FROM ShareHolderDV");
            sql.AppendLine("WHERE AccountDVID = @AccountDVID");
            sql.AppendLine("AND ShareholderName = @ShareholderName");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@ShareholderName", ShareholderName));
                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }
        public static DataRow SelectAccountForLog_Contact(SqlConnection Connection, SqlTransaction Transaction, Guid? ContactDVID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT c.Name, cmd.CodeName AS [Position], c.Email, c.BusinessPhoneCC AS [Business Phone CC], c.BusinessPhone AS [Business Phone], c.BusinessPhoneExt AS [Business Phone Ext], c.FaxCC AS [Fax CC], c.Fax AS [Fax]");
                    sql.AppendLine("FROM ContactDV c");
                    sql.AppendLine("LEFT JOIN CodeMaster cmd ON cmd.CodeMasterID = c.DesignationCID");
                    sql.AppendLine("WHERE c.ContactDVID = @ContactDVID");
                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@ContactDVID", ContactDVID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        private static void CreateAccountDV(SqlConnection Connection, SqlTransaction Transaction, Guid? SubmitTypeID, Guid? AccountDVID, Guid? AccountID)
        {
            SqlCommand com = new SqlCommand();

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO AccountDV");
            sql.AppendLine("SELECT @AccountDVID, @SubmitTypeID,  AccountID,AccountCode,AccountName,AccountTypeCID,CompanyTypeCID,CompanyRegNo,AccountCategoryCID,IndustryCID,ParentAccountID,CompanyLocationID,BusinessPhoneCtry,BusinessPhoneStt,BusinessPhoneCC,BusinessPhone,BusinessPhoneExt ");
            sql.AppendLine("\t\t,FaxCtry,FaxCC,FaxStt,Fax,WebSiteUrl,DateOfIncorporation,CoreActivities,MSCFileID,OldMSCFileID,MSCFileIDTesting,CustomerRankingCID,BursaMalaysiaCID,CounterName,EquityOwnershipCID,BumiClassificationCID,WomanOwnCompany,JVCategoryCID,OperationStatus");
            sql.AppendLine("\t\t,Acc5YearsTax,LeadGenerator,PDG,EXPat,Remarks,CreatedDate,ModifiedDate,CreatedBy,ModifiedBy,CreatedByName,ModifiedByName,FinancialIncentiveCID,OtherBusinessPhone,OtherFax,BumiStatusCID,ClassificationCID,CompanyEmail,WriteUp,Logo,RequirementSpace");
            sql.AppendLine("\t\t,PlanMoveTo,LeadSubmitDate,PercentageBumiParticipation,YearofApproval,MSCApprovedDate,DataSource,MSCCertificateNo,CompanyLogo,MSCApprovedCourses,InstitutionName,InstitutionType,InstitutionURL,Under6MonthsGracePeriodEmail");
            sql.AppendLine("FROM Account");
            sql.AppendLine("WHERE AccountID = @AccountID");

            //sql.AppendLine("INSERT INTO ShareHolderDV")
            //sql.AppendLine("SELECT NEWID(), @AccountDVID, ShareHolderID, ShareHolderName, Percentage, BumiPercentage, BumiShare, Status, CountryRegionID")
            //sql.AppendLine("FROM ShareHolder")
            //sql.AppendLine("WHERE AccountID = @AccountID")

            sql.AppendLine("INSERT INTO AccountManagerAssignmentDV ");
            sql.AppendLine("SELECT NEWID(), @AccountDVID, ama.AccountManagerAssignmentID, ama.UserID, ama.FinancialAnalystID, ama.StartDate, ama.EndDate, ama.AccountManagerTypeCID, ama.EEManagerName, ama.AssignmentDate, '0' AS 'Active', ama.DataSource");
            //sql.AppendLine("SELECT NEWID(), @AccountDVID, ama.AccountManagerAssignmentID, ama.UserID, ama.FinancialAnalystID, ama.StartDate, ama.EndDate, ama.AccountManagerTypeCID, ama.EEManagerName, ama.AssignmentDate, ama.Active, ama.DataSource")
            sql.AppendLine("FROM AccountManagerAssignment ama");
            sql.AppendLine("LEFT JOIN CodeMaster cm ON cm.CodeMasterID = ama.AccountManagerTypeCID");
            sql.AppendLine("WHERE ama.AccountID = @AccountID");
            //sql.AppendLine("AND cm.CodeName IN ('").Append(BOL.AppConst.AccountManagerType.BusinessAnalyst).Append("', '").Append(BOL.AppConst.AccountManagerType.PreMSC).Append("')")
            sql.AppendLine("AND cm.CodeName IN ('Business Analyst','PreMSC (Application)')");

            sql.AppendLine("INSERT INTO FinancialAndWorkerForecastDV");
            sql.AppendLine("(FinancialAndWorkerForecastDVID, AccountDVID, FinancialAndWorkerForecastID, Year, Investment, RnDExpenditure, LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities, LocalKW, ForeignKW, LocalWorker, ForeignWorker)");
            sql.AppendLine("SELECT NEWID(), @AccountDVID, FinancialAndWorkerForecastID, Year, Investment, RnDExpenditure, LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities, LocalKW, ForeignKW, LocalWorker, ForeignWorker");
            sql.AppendLine("FROM FinancialAndWorkerForecast");
            sql.AppendLine("WHERE AccountID = @AccountID");

            sql.AppendLine("INSERT INTO ContactDV ");
            sql.AppendLine("SELECT NEWID(), @AccountDVID, ContactID, Name, SalutationCID, DesignationCID, DesignationName, Department, ReportToContactID, Role, ContactStatus, Email, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, MobilePhoneCtry, MobilePhoneCC, MobilePhone, FaxCtry, FaxStt, FaxCC, Fax, IMAddress, SkypeName, ContactCategoryCID, Gender, DateOfBirth, RaceCID, SpouseName, Anniversary, AccessMode, KeyContact, MSCKeyContact, Published, OtherEmail, OtherMobilePhone, OtherBusinessPhone, OtherFax, DataSource, Deleted, ContactTypeCID, CEOEmailFlag, AQIRContact, MGSContact, AQIRID, ContactClassificationID");
            sql.AppendLine("FROM Contact");
            sql.AppendLine("WHERE AccountID = @AccountID");

            //sql.AppendLine("INSERT INTO AddressDV ")
            //sql.AppendLine("SELECT NEWID(), 'Account', @AccountDVID, AddressID, AddressTypeID, Address1, Address2, Address3, Address4, City, CountryRegionID, State, PostCode, Region, CyberCenterCID, BusinessPhoneCtry, BusinessPhoneStt, BusinessPhoneCC, BusinessPhone, BusinessPhoneExt, FaxCtry, FaxCC, FaxStt, Fax, OtherBusinessPhone, OtherFax, Master, Email, StateOthers, DataSource, AQIRID ")
            //sql.AppendLine("FROM Address")
            //sql.AppendLine("WHERE OwnerName = 'Account' AND OwnerID = @AccountID")

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
            com.Parameters.Add(new SqlParameter("@SubmitTypeID", SubmitTypeID));
            com.Parameters.Add(new SqlParameter("@AccountID", AccountID));

            try
            {
                com.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
        }
        private static void ACApproved_CreateUpdateFinancialAndWorkerForecastDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, int? Year, Nullable<int> LocalKW, Nullable<int> ForeignKW, Nullable<int> LocalWorker, Nullable<int> ForeignWorker, Nullable<decimal> Investment, Nullable<decimal> RnDExpenditure,
 Nullable<decimal> LocalSales, Nullable<decimal> ExportSales, Nullable<decimal> NetProfit, Nullable<decimal> CashFlow, Nullable<decimal> Asset, Nullable<decimal> Equity, Nullable<decimal> Liabilities)
        {
            SqlCommand com = new SqlCommand();
            StringBuilder sql = new StringBuilder();
            Nullable<Guid> FinancialAndWorkerForecastDVID = GetFinancialAndWorkerForecastDVID(Connection, Transaction, AccountDVID, Year);

            if (FinancialAndWorkerForecastDVID.HasValue)
            {
                DataRow currentLogData = SelectAccountForLog_FinancialAndWorkerForecast(Connection, Transaction, FinancialAndWorkerForecastDVID);
                sql.AppendLine("UPDATE FinancialAndWorkerForecastDV SET ");
                sql.AppendLine("LocalKW = @LocalKW,");
                sql.AppendLine("ForeignKW = @ForeignKW,");
                sql.AppendLine("LocalWorker = @LocalWorker,");
                sql.AppendLine("ForeignWorker = @ForeignWorker,");
                sql.AppendLine("Investment = @Investment,");
                sql.AppendLine("RnDExpenditure = @RnDExpenditure,");
                sql.AppendLine("LocalSales = @LocalSales,");
                sql.AppendLine("ExportSales = @ExportSales,");
                sql.AppendLine("NetProfit = @NetProfit,");
                sql.AppendLine("CashFlow = @CashFlow,");
                sql.AppendLine("Asset = @Asset,");
                sql.AppendLine("Equity = @Equity,");
                sql.AppendLine("Liabilities = @Liabilities");
                sql.AppendLine("WHERE FinancialAndWorkerForecastDVID = @FinancialAndWorkerForecastDVID");

                com.Parameters.Add(new SqlParameter("@FinancialAndWorkerForecastDVID", FinancialAndWorkerForecastDVID));
                com.Parameters.Add(new SqlParameter("@LocalKW", SyncHelper.ReturnNull(LocalKW)));
                com.Parameters.Add(new SqlParameter("@ForeignKW", SyncHelper.ReturnNull(ForeignKW)));
                com.Parameters.Add(new SqlParameter("@LocalWorker", SyncHelper.ReturnNull(LocalWorker)));
                com.Parameters.Add(new SqlParameter("@ForeignWorker", SyncHelper.ReturnNull(ForeignWorker)));
                com.Parameters.Add(new SqlParameter("@Investment", SyncHelper.ReturnNull(Investment)));
                com.Parameters.Add(new SqlParameter("@RnDExpenditure", SyncHelper.ReturnNull(RnDExpenditure)));
                com.Parameters.Add(new SqlParameter("@LocalSales", SyncHelper.ReturnNull(LocalSales)));
                com.Parameters.Add(new SqlParameter("@ExportSales", SyncHelper.ReturnNull(ExportSales)));
                com.Parameters.Add(new SqlParameter("@NetProfit", SyncHelper.ReturnNull(NetProfit)));
                com.Parameters.Add(new SqlParameter("@CashFlow", SyncHelper.ReturnNull(CashFlow)));
                com.Parameters.Add(new SqlParameter("@Asset", SyncHelper.ReturnNull(Asset)));
                com.Parameters.Add(new SqlParameter("@Equity", SyncHelper.ReturnNull(Equity)));
                com.Parameters.Add(new SqlParameter("@Liabilities", SyncHelper.ReturnNull(Liabilities)));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;
                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();
                    //Company Change Log - Financial & Worker Forecast
                    DataRow newLogData = SelectAccountForLog_FinancialAndWorkerForecast(Connection, Transaction, FinancialAndWorkerForecastDVID);
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    if (currentLogData != null && newLogData != null)
                    {
                        alMgr.CreateAccountLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, FinancialAndWorkerForecastDVID.Value, AccountDVID.Value, currentLogData, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                    }

                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //con.Close()
                }
            }
            else
            {
                sql.AppendLine("INSERT INTO FinancialAndWorkerForecastDV ");
                sql.AppendLine("(");
                sql.AppendLine("FinancialAndWorkerForecastDVID, AccountDVID, FinancialAndWorkerForecastID, Year, LocalKW, ForeignKW, LocalWorker, ForeignWorker, Investment, ");
                sql.AppendLine("RnDExpenditure, LocalSales, ExportSales, NetProfit, CashFlow, Asset, Equity, Liabilities");
                sql.AppendLine(")");
                sql.AppendLine("VALUES(");
                sql.AppendLine("@FinancialAndWorkerForecastDVID, @AccountDVID, NULL, @Year, @LocalKW, @ForeignKW, @LocalWorker, @ForeignWorker, @Investment, ");
                sql.AppendLine("@RnDExpenditure, @LocalSales, @ExportSales, @NetProfit, @CashFlow, @Asset, @Equity, @Liabilities");
                sql.AppendLine(")");

                FinancialAndWorkerForecastDVID = Guid.NewGuid();
                com.Parameters.Add(new SqlParameter("@FinancialAndWorkerForecastDVID", FinancialAndWorkerForecastDVID));
                com.Parameters.Add(new SqlParameter("@Year", Year));
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@LocalKW", SyncHelper.ReturnNull(LocalKW)));
                com.Parameters.Add(new SqlParameter("@ForeignKW", SyncHelper.ReturnNull(ForeignKW)));
                com.Parameters.Add(new SqlParameter("@LocalWorker", SyncHelper.ReturnNull(LocalWorker)));
                com.Parameters.Add(new SqlParameter("@ForeignWorker", SyncHelper.ReturnNull(ForeignWorker)));
                com.Parameters.Add(new SqlParameter("@Investment", SyncHelper.ReturnNull(Investment)));
                com.Parameters.Add(new SqlParameter("@RnDExpenditure", SyncHelper.ReturnNull(RnDExpenditure)));
                com.Parameters.Add(new SqlParameter("@LocalSales", SyncHelper.ReturnNull(LocalSales)));
                com.Parameters.Add(new SqlParameter("@ExportSales", SyncHelper.ReturnNull(ExportSales)));
                com.Parameters.Add(new SqlParameter("@NetProfit", SyncHelper.ReturnNull(NetProfit)));
                com.Parameters.Add(new SqlParameter("@CashFlow", SyncHelper.ReturnNull(CashFlow)));
                com.Parameters.Add(new SqlParameter("@Asset", SyncHelper.ReturnNull(Asset)));
                com.Parameters.Add(new SqlParameter("@Equity", SyncHelper.ReturnNull(Equity)));
                com.Parameters.Add(new SqlParameter("@Liabilities", SyncHelper.ReturnNull(Liabilities)));

                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.Transaction = Transaction;
                com.CommandTimeout = int.MaxValue;

                try
                {
                    //con.Open()
                    com.ExecuteNonQuery();
                    //Company Change Log - Financial & Worker Forecast
                    BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
                    DataRow newLogData = SelectAccountForLog_FinancialAndWorkerForecast(Connection, Transaction, FinancialAndWorkerForecastDVID);
                    alMgr.CreateAccountLog_FinancialAndWorkerForecast_Wizard(Connection, Transaction, FinancialAndWorkerForecastDVID.Value, AccountDVID.Value, null, newLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);

                }
                catch (Exception ex)
                {
                    throw;
                    //Finally
                    //con.Close()
                }
            }


        }

        private static Nullable<Guid> GetFinancialAndWorkerForecastDVID(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID, int? Year)
        {
            SqlCommand com = new SqlCommand();
            SqlDataAdapter ad = new SqlDataAdapter(com);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("SELECT FinancialAndWorkerForecastDVID");
            sql.AppendLine("FROM FinancialAndWorkerForecastDV");
            sql.AppendLine("WHERE AccountDVID = @AccountDVID");
            sql.AppendLine("AND [Year] = @Year");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            //con.Open()
            try
            {
                com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
                com.Parameters.Add(new SqlParameter("@Year", Year));
                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return new Guid(dt.Rows[0][0].ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //con.Close()
            }
        }
        private static void ACApproved_UpdateAccountDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID, Guid? AccountDVID, string AccountName, int OperationStatus, string CoreActivities, string BusinessPhoneCountryCode, string BusinessPhoneSC, string BusinessPhoneAC,
string BusinessPhoneCC, string BusinessPhone, string BusinessPhoneExt, string FaxCountryCode, string FaxSC, string FaxCC, string Fax, string WebSiteUrl, string InstitutionName, string InstitutionType,
string InstituteURL, string SubmitType)
        {
            BOL.AuditLog.Modules.AccountLog alMgr = new BOL.AuditLog.Modules.AccountLog();
            DataRow currentGeneralLogData = SelectAccountForLog_Account(Connection, Transaction, AccountID);
            DataRow currentPortfolioLogData = SelectAccountForLog_Portfolio(Connection, Transaction, AccountID);

            BOL.AccountContact.odsAccount acMgr = new BOL.AccountContact.odsAccount();
            Nullable<Guid> BumiClassificationCID = acMgr.CalculateBumiClassification_Wizard(Connection, Transaction, AccountID.Value);
            Nullable<Guid> ClassificationCID = acMgr.CalculateClassification_Wizard(Connection, Transaction, AccountID.Value);
            Nullable<Guid> JVCategoryCID = acMgr.CalculateJVCategory_Wizard(Connection, Transaction, AccountID.Value);

            BOL.AccountContact.odsContact mgrContact = new BOL.AccountContact.odsContact();
            string OffCallingCode = string.Empty;
            string FaxCallingCode = string.Empty;

            if (!string.IsNullOrEmpty(BusinessPhoneCC) && !string.IsNullOrEmpty(BusinessPhoneSC))
            {
                OffCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, BusinessPhoneSC, BusinessPhoneCC);
                BusinessPhoneCC = OffCallingCode;
            }

            if (!string.IsNullOrEmpty(FaxCC) && !string.IsNullOrEmpty(FaxSC))
            {
                FaxCallingCode = mgrContact.GetStateCodeWithStateCC_Wizard(Connection, Transaction, FaxSC, FaxCC);
                FaxCC = FaxCallingCode;
            }

            //Fadly 20130905 --> combine BusinessPhoneCC,BusinessPhone,and BusinessPhoneExt and also FaxCC and Fax
            //Begin
            if (!string.IsNullOrEmpty(BusinessPhone) & !string.IsNullOrEmpty(BusinessPhoneCC))
            {
                BusinessPhoneCC = BusinessPhoneCC.Replace(";", "");
                BusinessPhone = BusinessPhone.Replace(";", "");
                BusinessPhoneExt = BusinessPhoneExt.Replace(";", "");

                if (!string.IsNullOrEmpty(BusinessPhoneExt))
                {
                    BusinessPhone = BusinessPhoneCC + BusinessPhone + "x" + BusinessPhoneExt;
                }
                else
                {
                    BusinessPhone = BusinessPhoneCC + BusinessPhone;
                }
            }
            if (!string.IsNullOrEmpty(Fax) & !string.IsNullOrEmpty(FaxCC))
            {
                FaxCC = FaxCC.Replace(";", "");
                Fax = Fax.Replace(";", "");

                Fax = FaxCC + Fax;
            }
            //End

            SqlCommand com = new SqlCommand();

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.AppendLine("UPDATE AccountDV SET");

            if (BumiClassificationCID.HasValue)
                sql.AppendLine("BumiClassificationCID = @BumiClassificationCID,");
            if (ClassificationCID.HasValue)
                sql.AppendLine("ClassificationCID = @ClassificationCID,");
            if (JVCategoryCID.HasValue)
                sql.AppendLine("JVCategoryCID = @JVCategoryCID,");

            sql.AppendLine("AccountName = @AccountName,");
            if (SubmitType == "A")
            {
                sql.AppendLine("CoreActivities = ISNULL(CoreActivities,'') + CHAR(13) + CHAR(10) + @CoreActivities,");
            }
            else
            {
                sql.AppendLine("CoreActivities = @CoreActivities,");
            }
            //sql.AppendLine("BusinessPhoneCtry = @BusinessPhoneCountryCode,")
            //sql.AppendLine("BusinessPhoneStt = @BusinessPhoneSC,")
            //sql.AppendLine("BusinessPhoneCC = @BusinessPhoneCC,")
            sql.AppendLine("BusinessPhone = @BusinessPhone,");
            //sql.AppendLine("BusinessPhoneExt = @BusinessPhoneExt,")
            //sql.AppendLine("FaxCtry = @FaxCountryCode,")
            //sql.AppendLine("FaxStt = @FaxSC,")
            //sql.AppendLine("FaxCC = @FaxCC,")
            sql.AppendLine("Fax = @Fax,");
            sql.AppendLine("WebSiteUrl = @WebSiteUrl,");
            sql.AppendLine("InstitutionName = @InstitutionName,");
            sql.AppendLine("InstitutionType = @InstitutionType,");
            sql.AppendLine("InstitutionURL = @InstituteURL,");
            sql.AppendLine("OperationStatus = @OperationStatus,");
            sql.AppendLine("ModifiedBy = @ModifiedBy,");
            sql.AppendLine("ModifiedByName = @ModifiedByName,");
            sql.AppendLine("ModifiedDate = @ModifiedDate,");
            sql.AppendLine("BursaMalaysiaCID = @BursaMalaysiaCID");
            //Aryo20120109 Set default value "No" for BursaMalaysia
            sql.AppendLine("WHERE AccountDVID = @AccountDVID");

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            if (BumiClassificationCID.HasValue)
                com.Parameters.Add(new SqlParameter("@BumiClassificationCID", BumiClassificationCID));
            if (ClassificationCID.HasValue)
                com.Parameters.Add(new SqlParameter("@ClassificationCID", ClassificationCID));
            if (JVCategoryCID.HasValue)
                com.Parameters.Add(new SqlParameter("@JVCategoryCID", JVCategoryCID));

            com.Parameters.Add(new SqlParameter("@AccountName", SyncHelper.ReturnNull(AccountName)));
            com.Parameters.Add(new SqlParameter("@CoreActivities", SyncHelper.ReturnNull(CoreActivities)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneCountryCode", SyncHelper.ReturnNull(BusinessPhoneCountryCode)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneSC", SyncHelper.ReturnNull(BusinessPhoneSC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneAC", SyncHelper.ReturnNull(BusinessPhoneAC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneCC", SyncHelper.ReturnNull(BusinessPhoneCC)))
            com.Parameters.Add(new SqlParameter("@BusinessPhone", SyncHelper.ReturnNull(BusinessPhone)));
            //com.Parameters.Add(New SqlClient.SqlParameter("@BusinessPhoneExt", SyncHelper.ReturnNull(BusinessPhoneExt)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxCountryCode", SyncHelper.ReturnNull(FaxCountryCode)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxSC", SyncHelper.ReturnNull(FaxSC)))
            //com.Parameters.Add(New SqlClient.SqlParameter("@FaxCC", SyncHelper.ReturnNull(FaxCC)))
            com.Parameters.Add(new SqlParameter("@Fax", SyncHelper.ReturnNull(Fax)));
            com.Parameters.Add(new SqlParameter("@OperationStatus", OperationStatus));
            com.Parameters.Add(new SqlParameter("@WebSiteUrl", SyncHelper.ReturnNull(WebSiteUrl)));
            com.Parameters.Add(new SqlParameter("@InstitutionName", SyncHelper.ReturnNull(InstitutionName)));
            com.Parameters.Add(new SqlParameter("@InstitutionType", SyncHelper.ReturnNull(InstitutionType)));
            com.Parameters.Add(new SqlParameter("@InstituteURL", SyncHelper.ReturnNull(InstituteURL)));
            com.Parameters.Add(new SqlParameter("@ModifiedBy", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@ModifiedByName", SyncHelper.AdminName));
            com.Parameters.Add(new SqlParameter("@ModifiedDate", DateTime.Now));
            com.Parameters.Add(new SqlParameter("@AccountDVID", AccountDVID));
            com.Parameters.Add(new SqlParameter("@BursaMalaysiaCID", BOL.Common.Modules.Parameter.DEFAULT_STCK_EXCHNGE));

            try
            {
                //con.Open()
                com.ExecuteNonQuery();

                //Log
                DataRow newGeneralLogData = SelectAccountForLog_AccountDV(Connection, Transaction, AccountDVID);
                DataRow newPortfolioLogData = SelectAccountForLog_Portfolio(Connection, Transaction, AccountDVID);
                Guid guidAccountDVID = new Guid(AccountDVID.ToString());
                if (currentGeneralLogData != null && newGeneralLogData != null)
                {
                    alMgr.CreateAccountLog_General_Wizard(Connection, Transaction, guidAccountDVID, currentGeneralLogData, newGeneralLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }

                if (currentPortfolioLogData != null && newPortfolioLogData != null)
                {
                    alMgr.CreateAccountLog_Portfolio_Wizard(Connection, Transaction, guidAccountDVID, currentPortfolioLogData, newPortfolioLogData, new Guid(SyncHelper.AdminID), SyncHelper.AdminName);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        private static DataRow SelectAccountForLog_AccountDV(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountDVID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT a.AccountName AS [Company Name], a.BusinessPhoneCC AS [Business Phone CC], a.BusinessPhone AS [Business Phone], a.BusinessPhoneExt AS [Business Phone Ext], a.FaxCC AS [Fax CC], a.Fax AS [Fax], a.WebSiteUrl AS [WebSite]");
                    sql.AppendLine("FROM AccountDV a");
                    sql.AppendLine("WHERE AccountDVID = @AccountDVID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;

                    cmd.Parameters.AddWithValue("@AccountDVID", AccountDVID);

                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }

        private static DataRow SelectAccountForLog_Portfolio(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT CAST(a.OperationStatus AS varchar) AS [Operational Status], a.CoreActivities AS [Core Activities], cmc.CodeName AS [Classification], cmbc.CodeName AS [Bumi Classification], cmjc.CodeName AS [JV Category]");
                    sql.AppendLine("FROM Account a");
                    sql.AppendLine("JOIN CodeMaster cmc ON a.ClassificationCID = cmc.CodeMasterID");
                    sql.AppendLine("JOIN CodeMaster cmbc ON a.BumiClassificationCID = cmbc.CodeMasterID");
                    sql.AppendLine("JOIN CodeMaster cmjc ON a.JVCategoryCID = cmjc.CodeMasterID");
                    sql.AppendLine("WHERE AccountID = @AccountID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@AccountID", AccountID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        private static DataRow SelectAccountForLog_Account(SqlConnection Connection, SqlTransaction Transaction, Guid? AccountID)
        {
            //Using conn As New SqlClient.SqlConnection(ConfigurationManager.ConnectionStrings("CRM").ConnectionString)
            //	conn.Open()
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT a.AccountName AS [Company Name], a.BusinessPhoneCC AS [Business Phone CC], a.BusinessPhone AS [Business Phone], a.BusinessPhoneExt AS [Business Phone Ext], a.FaxCC AS [Fax CC], a.Fax AS [Fax], a.WebSiteUrl AS [WebSite]");
                    sql.AppendLine("FROM Account a");
                    sql.AppendLine("WHERE AccountID = @AccountID");

                    cmd.CommandText = sql.ToString();
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Transaction = Transaction;
                    cmd.Parameters.AddWithValue("@AccountID", AccountID);

                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        public static DataRow SelectAccountForLog_FinancialAndWorkerForecast(SqlConnection Connection, SqlTransaction Transaction, Guid? FinancialAndWorkerForecastDVID)
        {
            using (SqlCommand cmd = new SqlCommand("", Connection))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    StringBuilder sql = new StringBuilder();

                    sql.AppendLine("SELECT fwf.[Year], ");
                    sql.AppendLine("fwf.Investment, fwf.RnDExpenditure AS [R&D Expenditure],");
                    sql.AppendLine("fwf.LocalSales AS [Local Sales], fwf.ExportSales AS [Export Sales],");
                    sql.AppendLine("fwf.Revenue, fwf.NetProfit AS [Net Profit], fwf.CashFlow AS [Cash Flow],");
                    sql.AppendLine("fwf.Asset, fwf.Equity, fwf.Liabilities, fwf.LocalKW AS [Local KW],");
                    sql.AppendLine("fwf.ForeignKW AS [Foreign KW], fwf.LocalWorker AS [Local Worker], ");
                    sql.AppendLine("fwf.ForeignWorker AS [Foreign Worker]");
                    sql.AppendLine("FROM FinancialAndWorkerForecastDV fwf");
                    sql.AppendLine("WHERE fwf.FinancialAndWorkerForecastDVID = @FinancialAndWorkerForecastDVID");

                    cmd.CommandText = sql.ToString();
                    cmd.Transaction = Transaction;
                    cmd.CommandTimeout = int.MaxValue;
                    cmd.Parameters.AddWithValue("@FinancialAndWorkerForecastDVID", FinancialAndWorkerForecastDVID);
                    DataTable dataTable = new DataTable();
                    adapter.Fill(dataTable);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            //End Using
        }
        private static void CreateACApprovedUpdatedHistoryWithXML(SqlConnection Connection, SqlTransaction Transaction, int? MeetingNo, string MSCFileID, string SubmitType, string WizardXMLData)
        {
            WizardXMLData = Regex.Replace(WizardXMLData, @"[#{}&()]+(?=[^<>]*>)", "");
            SqlCommand com = new SqlCommand();
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            sql.AppendLine("INSERT INTO ACApprovedAccountHistory ");
            sql.AppendLine("(");
            sql.AppendLine("MeetingNo, MSCFileID, WizardXMLData, SubmitType, ");
            sql.AppendLine("CreatedBy, CreatedByName, CreatedDate, ModifiedBy, ModifiedByName, ModifiedDate");
            sql.AppendLine(")");
            sql.AppendLine("VALUES(");
            sql.AppendLine("@MeetingNo, @MSCFileID, @WizardXMLData, @SubmitType,");
            sql.AppendLine("@UserID, @UserName, getdate(), @UserID, @UserName, getdate()");
            sql.AppendLine(")");

            com.Parameters.Add(new SqlParameter("@MeetingNo", MeetingNo));
            com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));
            com.Parameters.Add(new SqlParameter("@WizardXMLData", WizardXMLData));
            com.Parameters.Add(new SqlParameter("@SubmitType", SubmitType));
            com.Parameters.Add(new SqlParameter("@UserID", SyncHelper.AdminID));
            com.Parameters.Add(new SqlParameter("@UserName", SyncHelper.AdminName));

            com.CommandText = sql.ToString();
            com.CommandType = CommandType.Text;
            com.Connection = Connection;
            com.Transaction = Transaction;
            com.CommandTimeout = int.MaxValue;

            try
            {
                //con.Open()
                com.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw;
                //Finally
                //con.Close()
            }
        }

    }
}
