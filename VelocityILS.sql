--  Age of the account
--  Avg Monthly Deposit for last 90 days
--  Avg Monthly Deposit Count for last 90 days 
--  Avg Monthly Debit Declines for last 90 days
--  Average Days Between Deposits for last 90 days
--  # Days Since Last Deposit
--  Days Currently Overdrawn
--  Average Days Overdrawn
--  Total Deposit Amount for the past 33 days
--  Normalized Deposit Amount Prior 90 Days

--DECLARE @DL_Load_Date datetime = '2023-05-07';

--SELECT x.member_nbr
--, x.share_nbr
--, occurrence_date = min(ah.entry_date)
--, calendar_days_neg = datediff(d,min(ah.entry_date), @DL_Load_Date) + 1
--FROM 
--(
--	select sh.member_nbr
--	, sh.share_nbr
--	, max(account_history_id) as last_positive_tran_id
--	FROM datamart.dbo.share sh
--	INNER JOIN datamart.dbo.accounthistory ah on sh.member_nbr = ah.member_nbr and sh.share_nbr = ah.acct_nbr and ah.entry_date >= dateadd(d,-180, @DL_Load_Date) and ah.before_bal + ah.principal_amt >= 0 and ah.dl_load_ts <= sh.dl_load_ts
--	WHERE sh.DL_LOAD_DATE = @DL_Load_Date AND sh.balance < 0 and sh.class = 1 and sh.CLOSED = 0
--	AND SH.SHARE_TYPE IN (23,44,45,55,57,68,69,74,75,17,19,21,73,34)
--	GROUP by sh.member_nbr, sh.share_nbr
--) x
--INNER JOIN ACCOUNTHISTORY ah ON ah.MEMBER_NBR = x.MEMBER_NBR AND x.SHARE_NBR = ah.ACCT_NBR AND ah.ACCOUNT_HISTORY_ID > x.last_positive_tran_id AND ah.ENTRY_DATE >= DATEADD(day, -180, @DL_Load_Date)
--GROUP BY x.MEMBER_NBR, x.SHARE_NBR
--ORDER BY x.MEMBER_NBR, x.SHARE_NBR


--SELECT Class = CASE WHEN BEFORE_BAL + PRINCIPAL_AMT >=0 THEN 1 ELSE 0 END , *
--FROM ACCOUNTHISTORY
--WHERE ENTRY_DATE > '2023-04-15' AND MEMBER_NBR = 4131130 AND ACCT_NBR = 2;


DECLARE @DL_Load_Date date = '2023-05-21';

WITH Checking_Types AS
(
   SELECT product_code
   , product_name
   , Share_Type = fxp_type_nbr
   , dtodnegl as OD_Limit
   FROM DataMart.dbo.Product  P
   INNER JOIN datamart.dbo.base_dtrcd dt
      ON P.fxp_type_nbr = dt.dt_
   WHERE P.product_category_code = 1 AND fxp_type_nbr in (23,45,55,57,68,69,74,75,44)
)

SELECT Top(20) MEMBER_NBR
, SHARE_NBR
, Age = DATEDIFF(year,S.OPEN_DATE, GETDATE())
, AvgMonthlyDeposit = COALESCE(AvgMonthlyDeposit,0)
, AvgMonthlyDepositCount = COALESCE(AvgMonthlyDepositCount,0)
, AvgDaysBetweenDeposits = COALESCE(AvgDaysBetweenDeposits,0)
, DaysSinceLastDeposit = CASE WHEN LastDepositDate IS NULL THEN 0 ELSE DATEDIFF(DAY, LastDepositDate, GETDATE()) END
, DaysCurrentlyOverDrawn = CASE WHEN S.BALANCE < 0 THEN 
								CASE WHEN Deposits.LastPositiveBalanceDate IS NOT NULL THEN 
									DATEDIFF(DAY, Deposits.LastPositiveBalanceDate, GETDATE()) 
								ELSE 180 
								END 
						  ELSE 0 
						  END
, AvgDaysOverDrawn = COALESCE(AvgDaysOverDrawn,0)
, Last33DepositAmt = COALESCE(Last33DepositAmt,0)
, NormalizedDepositPrior90 = COALESCE(NormalizedDepositPrior90,0)
, TotalDepositAmount = COALESCE(TotalDepositAmount,0)
, TotalDepositCount = COALESCE(TotalDepositCount,0)
, OverDraftCount = COALESCE(OverDraftCount,0)
, DepositTrendNumber = CASE WHEN LastDepositDate IS NULL THEN 0 ELSE DATEDIFF(DAY, LastDepositDate, GETDATE()) END/NULLIF(COALESCE(CAST(AvgDaysBetweenDeposits AS decimal),0),0)
, WindfallTrendNumber = COALESCE(Last33DepositAmt,0)/NULLIF(COALESCE(NormalizedDepositPrior90,0),0)
, ODPBooster = CASE WHEN Booster.Cnt > 0 THEN 1 ELSE 0 END
, NSF.Tiered_Nsf_Option_Set_Nbr
, Account_OD_Limit = OD_Limit
FROM Checking_Types CT
INNER JOIN SHARE S ON CT.Share_Type = S.Share_Type AND S.Class = 1 AND S.DL_Load_Date = @DL_Load_Date AND CLosed <> -1 --AND S.NEG_BAL_CNT_LAST_30_DAYS < 1
CROSS APPLY
(
   SELECT Last33DepositAmt = SUM(CASE WHEN Entry_Date>=DATEADD(DAY,-33,@DL_LOAD_DATE) THEN AH.PRINCIPAL_AMT ELSE 0 END)
   , NormalizedDepositPrior90 = AVG(CASE WHEN Entry_Date BETWEEN DATEADD(DAY,-123,@DL_LOAD_DATE) AND DATEADD(DAY,-34,@DL_LOAD_DATE) THEN AH.PRINCIPAL_AMT Else 0 END)
   , AvgMonthlyDeposit = ROUND(SUM(AH.PRINCIPAL_AMT)/6.0,2)
   , AvgMonthlyDepositCount = ROUND(COUNT(*)/6.0,2)
   , TotalDepositAmount = SUM(CASE WHEN AH.PRINCIPAL_AMT > 32 THEN AH.Principal_Amt ELSE 0 END)
   , TotalDepositCount = SUM(CASE WHEN AH.PRINCIPAL_AMT > 32 THEN 1 ELSE 0 END)
   , LastDepositDate = MAX(Entry_Date)     
   , AvgDaysBetweenDeposits = AVG(CASE WHEN AH.PREV_ENTRY_DATE IS NULL THEN 0 ELSE DATEDIFF(DAY, AH.PREV_ENTRY_DATE, AH.ENTRY_DATE) END)
   , LastPositiveBalanceDate = MAX(CASE WHEN AH.BEFORE_BAL + AH.PRINCIPAL_AMT >= 0 THEN ENTRY_DATE ELSE NULL END) 
   FROM 
   (
		SELECT ENTRY_DATE
		, PRINCIPAL_AMT
		, BEFORE_BAL
		, PREV_ENTRY_DATE = LAG(Entry_Date, 1, NULL) OVER(ORDER BY Entry_Date)		
		FROM ACCOUNTHISTORY AH
		INNER JOIN TransactionCode TC
			ON TC.TRAN_T_TYPE = AH.TRAN_T_TYPE
			AND TC.TRAN_NBR = AH.TRAN_NBR
			AND (TC.Description like '%DEPOSIT%' OR TC.Description like '%TRANSFER%')
			AND TC.Description NOT LIKE '%UNDO%'
			AND TC.Description <> 'DEPOSIT-OVERDRAFT'  
			WHERE AH.MEMBER_NBR = S.MEMBER_NBR AND AH.ACCT_NBR = S.SHARE_NBR AND AH.ENTRY_DATE >= DATEADD(DAY, -180, S.DL_Load_Date) AND AH.PRINCIPAL_AMT >= 1
	) AH
) Deposits
CROSS APPLY
(
   SELECT AvgDaysOverDrawn = AVG(CASE WHEN AH.PREV_ENTRY_DATE IS NULL THEN 0 ELSE DATEDIFF(DAY, AH.PREV_ENTRY_DATE, AH.ENTRY_DATE) END) 
   , OverDraftCount = COUNT(*)
   FROM 
   (
		SELECT ENTRY_DATE
		, PRINCIPAL_AMT
		, BEFORE_BAL
		, PREV_ENTRY_DATE = LAG(Entry_Date, 1, NULL) OVER(ORDER BY Entry_Date)		
		FROM ACCOUNTHISTORY AH
		INNER JOIN TransactionCode TC
			ON TC.TRAN_T_TYPE = AH.TRAN_T_TYPE
			AND TC.TRAN_NBR = AH.TRAN_NBR	
			AND AH.BEFORE_BAL + AH.PRINCIPAL_AMT <0
		WHERE AH.MEMBER_NBR = S.MEMBER_NBR AND AH.ACCT_NBR = S.SHARE_NBR AND AH.ENTRY_DATE >= DATEADD(DAY, -180, S.DL_Load_Date)
	) AH
) History
CROSS APPLY
(
    SELECT Cnt = SUM(CASE WHEN SHARE_ITEM_VALUE = 1 THEN 1 ELSE 0 END)
	FROM SHAREPROPERTY
	WHERE DL_LOAD_DATE = S.DL_LOAD_DATE AND MEMBER_NBR = S.MEMBER_NBR AND SHARE_NBR = S.SHARE_NBR AND ITEM_NAME ='OXBOOSTER_COVERME'
) Booster
CROSS APPLY
(
	SELECT Tiered_Nsf_Option_Set_Nbr = MAX(Tiered_Nsf_Option_Set_Nbr)
	FROM TIEREDNSFACCOUNT
	WHERE MEMBER_NBR = S.MEMBER_NBR AND SHARE_NBR = S.SHARE_NBR AND DL_LOAD_DATE = S.DL_LOAD_DATE
) NSF
--WHERE S.MEMBER_NBR = 1824290



--select *
--from TIEREDNSFACCOUNT
--WHERE DL_Load_Date = '2023-05-21' AND Member_Nbr = 4274560


 --   SELECT *
	--FROM SHAREPROPERTY
	--WHERE DL_LOAD_DATE = @DL_LOAD_DATE AND MEMBER_NBR = 30 AND SHARE_NBR = 2 AND ITEM_NAME ='OXBOOSTER_COVERME'

