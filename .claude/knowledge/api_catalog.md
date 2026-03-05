# External API Operations Catalog

Reference index of all documented external API operations.
Docs location: `Project Documentation/Documentation/Endpoints/`
Each operation folder contains: README.md, YAML spec, test script, response sample.

---

## CallCenterWs (SOAP — StorageMaker/SMD)
Client: `backend/python/common/soap_client.py`
Namespace: `http://tempuri.org/`

### Charges (9 operations)
- ApplyCredit
- ChargeAddToLedger
- ChargeDateUpdate
- ChargeDescriptionsRetrieve
- ChargePriceUpdate
- ChargesAllByLedgerID
- CustomBillingDateCharges
- CustomerAccountsMakeFutureCharges
- RecurringChargeAddToLedger_v1

### Discounts & Promotions (7)
- DiscountPlanUnitTypesList
- DiscountPlanUpdate
- DiscountPlanUpdateSimple
- DiscountPlansRetrieve
- DiscountPlansRetrieveIncludingDisabled
- PromotionsRetrieve
- ProrationInformationRetrieve

### Employees & Users (10)
- CorpUserDisable, CorpUserList, CorpUserPasswordReset, CorpUserSecurityUnlock
- EmployeeDisable, EmployeeList, EmployeeLogin, EmployeeLoginCCC, EmployeePasswordReset, EmployeeSecurityUnlock

### Forms & Documents (3)
- BulletinBoardInsert
- FormsRetrieve, FormsRetrieve_v2

### Gate Access (3)
- GateAccessData, KeypadZonesRetrieve, UpdateLockCode

### Insurance (6)
- CancelInsurancePolicy
- InsuranceCoverageMinimumsRetrieve
- InsuranceCoverageRetrieve, _V2, _V3
- InsuranceRateUpdate

### Ledger (13)
- CustomerAccountsBalanceDetails, _WithDiscount, _v2
- InsuranceCoverageAddToLedger
- InsuranceLedgerStatusByLedgerID, ByLedgerIDs
- LedgerBillingDayUpdate
- LedgerImageUpload, LedgerInvoiceUpdate, LedgerProofOfInsuranceUpdate, LedgerPurchaseOrderUpdate
- LedgerStatementByLedgerID, LedgerVehicleUpdate
- PaidThroughDateByLedgerID
- RemoveDiscountFromLedger

### Marketing (12)
- CallTrackingCampaigns: Add, Delete, Retrieve, Update
- Competitor: Add, Update
- CompetitorTracking: Add, List, Update
- LeadGeneration
- MarketingSources: Add, Delete, Retrieve, Update

### Move In/Out (35 — largest category)
- MoveIn, MoveInWithDiscount (_v2-v7, _28DayBilling, _SCA variants)
- MoveInCostRetrieve, MoveInCostRetrieveWithDiscount (many variants: _28DayBilling, _Reservation, _PushRate)
- MoveInReservation (_v2-v6, _28DayBilling, _SCA variants)
- MoveInOutList
- MoveOut, ScheduleMoveOut

### Payments (28)
- CalculateSurchargeAmounts, SurchargeApplicable
- ChargesAndPaymentsByLedgerID, PaymentsByLedgerID
- ConvenienceFee: Add, Remove, Retrieve
- CustomerAccountsBalanceDetailsWithPrepayment (_v2), CustomerAccountsChargesWithPrepayment
- DeliveryFeeRetrieve
- PaymentMultipleWithSource (_v2, _v3, _SCA)
- PaymentSettings, PaymentTypesRetrieve
- PaymentSimple (+ ACH, Cash, Check, WithSource, WithPrepaidDiscount — many variants)
- Refund: Cash, Check, CreditCard (_v2), ApplyToDifferentUnit
- SendPaymentConfirmationEmail

### Reservations (18)
- ReservationFeeAdd, ReservationFeeAddWithSource (_v2, _SCA, _ForMobileStorage)
- ReservationFeeRetrieve
- ReservationList (_v2, _v3)
- ReservationNew, ReservationNewWithSource (_v2-v6)
- ReservationNoteInsert, ReservationNotesRetrieve
- ReservationUpdate (_v2-v4)
- SendReservationConfirmationEmail

### Settings & Configuration (4)
- JavascriptEncryptionPublicKeyRetrieve, JavascriptEncryptionTest
- PhoneIntegrationPushCallInformation
- RentTaxRatesRetrieve

### Site Management (19)
- ACHProcessorSiteCurrentType, CCProcessorSiteCurrentType
- MapShapesRetrieve
- POSItem: AddToLedger, Payment, PaymentWithDiscount (_SCA), UpdateInStockQuantity, Retrieve
- PostalCodeOwnerMarketsList
- SiteInformation, SiteInformationUpdate
- SiteLinkeSign: Create/Preview Document/Lease URL (multiple versions), Retrieve, GenerateDownloadUrl
- SiteMapCreateURL, SiteSearchByPostalCode, TimeZonesRetrieve

### System Utilities (5)
- CallStoredProcedure (_v2, _v3, _v4)
- SignMessage_SCA_v1

### Tenants (42 — second largest)
- BillingInfoByTenantIDForMobile (_v2)
- LedgerTransferToNewTenant
- LedgersByTenantID (_v2, _v3)
- NationalMasterAccountsRetrieve
- ReservationBillingInfoByTenantID (_v2), ReservationBillingInfoUpdate (_SCA)
- ReservationListByTenantID
- ScheduleTenantRateChange (_v2)
- TenantBillingInfoByTenantID (_v2, _v3), ByQRIDGlobalNumMasked
- TenantBillingInfoUpdate (_v2, _SCA), ByQRIDGlobalNum (_v2)
- TenantConnectSettings: Retrieve, Update
- TenantEmailOptInUpdate, TenantExitSurveyUpdate
- TenantIDByUnitNameOrAccessCode
- TenantImagePathRetrieve, TenantImagePathUpdate, TenantImageUpload
- TenantInfoByTenantID, TenantInvoicesByTenantID
- TenantList, TenantListDetailed (_v2, _v3, MovedInTenantsOnly)
- TenantLogin, TenantLoginAndSecurityUpdate
- TenantMarketingUpdate
- TenantNew, TenantNewDetailed (_v2, _v3)
- TenantNoteInsert (_v2), TenantNotesRetrieve
- TenantSMSOptInUpdate, TenantSearchDetailed, TenantSettingsRetrieve
- TenantUpdate (_v2, _v3, _AdditionalContact, _Military, _NationalAccount)

### Uncategorized (4)
- PurchaseOrderNumberRetrieve, PurchaseOrderNumberUpdate
- SendLeaseInformationEmail
- SurchargingConfigurationRetrieve

### Units (17)
- UnitAdd, UnitDelete
- UnitContentsRetrieve, UnitContentsUpdate
- UnitPushRateUpdate (_v2)
- UnitStandardRateUpdate (_v2, _v3, _v4)
- UnitStatusUpdate
- UnitTypePriceList (_v2)
- UnitWebRateUpdate
- UnitsInformation (_v2, _v3), AvailableUnitsOnly (_v2), ByUnitID, ByUnitName, Internal

---

## Reporting (SOAP — SMD Reporting Service)

### Activity Reports (5)
- BadDebtWrittenOff, BadDebts, Exceptions, MoveInsAndMoveOuts, RentalActivity

### Custom Reports (2)
- CustomReportByReportID, CustomReportListByCorp

### Data Exchange (3)
- DEGetIndex, DEGetJobStatus, DEStartJob

### Data Export (8)
- DashboardTablesRefresh, DataCarve, DataMineSP
- ERPTransactionsByDateAndLedgerID, FacturaRetrieve
- RMSDataCarve (_v2, _v3, _v4)

### Financial (8)
- AccountsReceivable, DailyDeposits, DailyDepositsPreClose
- FinancialSummary, GeneralJournalEntries, IncomeAnalysis
- PastDueBalances, Receipts

### Insurance (8)
- InsuranceActivity, InsuranceActivityForAPI, InsuranceCustomActivityForAPI, InsuranceCustomReport
- InsuranceOverview, InsurancePolicyNumUpdate, InsuranceRoll, InsuranceStatement, InsuranceSummary

### Management Summary (4)
- ConsolidatedManagementSummary, ManagementHistory, ManagementSummary, OccupancyStatistics

### Marketing (3)
- InquiryTracking, MarketingRoll, MarketingSummary

### Merchandise (2)
- MerchandiseActivity, MerchandiseSummary

### System Utilities (6)
- Discounts, MiscReportingTables (_v2), MobDispatchSched, ScheduledAuctions, ScheduledMoveOuts, SiteListGlobal

### Tenant Lists (7)
- ChargesAndPaymentsComplete, PastDueCustom, RentRoll
- TenantListComplete (_v2, MovedInTenantsOnly)
- TenantRentChangeHistory

### Units & Pricing (3)
- SiteRates, UnitPayments, UnitPriceList

---

## EmbedSocial (REST)
Client: custom fetch in pipelines
- GetListings
- GetListingById
- GetListingMetrics
- GetListingItemMetrics
- GetItems
- UpdateListing
- CreateContentPublishingMedia

---

## SugarCRM (REST)
Client: `backend/python/common/sugarcrm_client.py`

### Authentication
- oauth2_token

### Modules (CRUD pattern — GET/POST/PUT/DELETE on /{module}/{id})
- Accounts, Contacts, Leads, Opportunities (+ _generic pattern)

### Other
- global_search
- bulk_request
- get_metadata, get_enum_values, get_language_labels
- get_dashboard
- get_installed_packages, get_staged_packages
