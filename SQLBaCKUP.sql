SELECT a.MEMBER_NBR, Age_of_Account = DATEDIFF(month, S.OPEN_DATE, GETDATE())
,avg(a.PRINCIPAL_AMT) as 'AVG_90_DAY_DEP'
,COUNT(a.PRINCIPAL_AMT) as '90_DAY_DEP_CNT'
,SUM(PRINCIPAL_AMT)/3 as 'TOTAL_33DAYS_DEP'
,SUM(a.principal_amt)/COUNT(a.MEMBER_NBR) as 'NORM_90_DAY_DEP'
,(COUNT(c.OperationID)/90) as 'AVG_90_DEBIT_DECLINE'
FROM dbo.Share S
INNER JOIN dbo.TIEREDNSFACCOUNT T ON T.DL_LOAD_DATE = S.DL_LOAD_DATE AND T.MEMBER_NBR = S.MEMBER_NBR AND t.SHARE_NBR = s.SHARE_NBR
INNER JOIN ID.MonthEnd_DL_Load_Dates I ON S.DL_LOAD_DATE = I.dl_load_Date AND I.sequence = 1
INNER JOIN dbo.ACCOUNTHISTORY a ON s.MEMBER_NBR=a.MEMBER_NBR
INNER JOIN dbo.TRANSACTIONCODE tc ON tc.TRAN_NBR = a.TRAN_NBR and tc.TRAN_T_TYPE = a.TRAN_T_TYPE
left JOIN dbo.CATOperations c ON s.MEMBER_NBR=c.MemberNumber and BIN = 477748 and c.OperationDateTime >= DATEADD(d,-90,Getdate()) and OperationResponseType = 'DCL' and OperationClass in ('PUR','WTH','ADJCT','ATHZ','CASHADV')
where a.ENTRY_DATE >= DATEADD(d,-90,Getdate())
and a.PRINCIPAL_AMT > 0 and a.TRAN_NBR = 100
and a.TRAN_T_TYPE not in ('*DV!','*DV@','*DVT','*PA ','*PA!','*PA@','*PRA','*S' ,'*S !','*S @','PR*!','PR*@','PR**','QLS ','QLS!','QLS@')
and right(a.TRAN_T_TYPE,1) not in ('!','@')
and DESCRIPTION like '%deposit%' 
group by s.MEMBER_NBR,a.MEMBER_NBR,s.OPEN_DATE,c.MemberNumber
