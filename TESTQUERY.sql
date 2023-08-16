WITH temp2 (MEMBER_NBR, LAST_DEPOSIT_DATE) AS (
    -- Select from a subquery that joins the ACCOUNTHISTORY and TRANSACTIONCODE tables and filters the results
    SELECT MEMBER_NBR, ENTRY_DATE AS LAST_DEPOSIT_DATE 
    FROM (
        -- Select the MEMBER_NBR, PRINCIPAL_AMT, ENTRY_DATE from the ACCOUNTHISTORY table and assign a row number to each entry
        SELECT MEMBER_NBR, ROW_NUMBER() OVER(PARTITION BY A.MEMBER_NBR ORDER BY A.ENTRY_DATE DESC) AS ROW, PRINCIPAL_AMT, ENTRY_DATE 
        FROM dbo.ACCOUNTHISTORY A 
        JOIN TRANSACTIONCODE TC 
        ON TC.TRAN_NBR = A.TRAN_NBR 
        AND TC.TRAN_T_TYPE = A.TRAN_T_TYPE 
        -- Filter the results based on various conditions
        WHERE A.ENTRY_DATE >= DATEADD(D, -365, GETDATE()) 
        AND A.TRAN_NBR = 100 
        AND A.TRAN_T_TYPE NOT IN ('*DV!','*DV@','*DVT','*PA','*PA!','*PA@','*PRA','*S','*S!','*S@','PR*!','PR*@','PR**','QLS','QLS!','QLS@') 
        AND RIGHT(A.TRAN_T_TYPE, 1) NOT IN ('!','@') 
        AND DESCRIPTION LIKE '%deposit%'
    ) AS subq 
    -- Only select the rows with a row number of 1
    WHERE ROW = 1 
)
-- Select all rows from the "temp" CTE and return them
SELECT * FROM temp2

UNION ALL 

With temp (MEMBER_NBR,TOT_DEPOSIT ) as (
SELECT MEMBER_NBR
, sum(PRINCIPAL_AMT) as 'TOT_DEPOSIT'
FROM dbo.ACCOUNTHISTORY A
join
TRANSACTIONCODE TC
on tc.TRAN_NBR = a.TRAN_NBR
and tc.TRAN_T_TYPE = a.TRAN_T_TYPE
where
a.ENTRY_DATE >= DATEADD(d,-33,Getdate())
and a.TRAN_NBR = 100
and a.TRAN_T_TYPE not in ('*DV!','*DV@','*DVT','*PA ','*PA!','*PA@','*PRA','*S' ,'*S !','*S @','PR*!','PR*@','PR**','QLS ','QLS!','QLS@')
and right(a.TRAN_T_TYPE,1) not in ('!','@')
and DESCRIPTION like '%deposit%'

group by (MEMBER_NBR)
)
SELECT a.MEMBER_NBR, Age_of_Account = DATEDIFF(month, S.OPEN_DATE, GETDATE())
,avg(a.PRINCIPAL_AMT) as 'AVG_90_DAY_DEP'
,COUNT(a.PRINCIPAL_AMT) as '90_DAY_DEP_CNT'
,SUM(a.PRINCIPAL_AMT)/3 as 'TOTAL_33DAYS_DEP'
,SUM(a.principal_amt)/COUNT(a.MEMBER_NBR) as 'NORM_90_DAY_DEP'
,(COUNT(c.OperationID)/90) as 'AVG_90_DEBIT_DECLINE'
,t2.TOT_DEPOSIT
FROM dbo.Share S
INNER JOIN dbo.TIEREDNSFACCOUNT T ON T.DL_LOAD_DATE = S.DL_LOAD_DATE AND T.MEMBER_NBR = S.MEMBER_NBR AND t.SHARE_NBR = s.SHARE_NBR
INNER JOIN ID.MonthEnd_DL_Load_Dates I ON S.DL_LOAD_DATE = I.dl_load_Date AND I.sequence = 1
INNER JOIN dbo.ACCOUNTHISTORY a ON s.MEMBER_NBR=a.MEMBER_NBR
INNER JOIN dbo.TRANSACTIONCODE tc ON tc.TRAN_NBR = a.TRAN_NBR and tc.TRAN_T_TYPE = a.TRAN_T_TYPE AND TC.DL_LOAD_DATE = t.DL_LOAD_DATE
LEFT JOIN temp t2 on t2.MEMBER_NBR = s.MEMBER_NBR
--LEFT JOIN temp t3 on t3.MEMBER_NBR = s.MEMBER_NBR
LEFT JOIN dbo.CATOperations c ON s.MEMBER_NBR=c.MemberNumber and BIN = 477748 and c.OperationDateTime >= DATEADD(d,-90,Getdate()) and OperationResponseType = 'DCL' and OperationClass in ('PUR','WTH','ADJCT','ATHZ','CASHADV')
where a.ENTRY_DATE >= DATEADD(d,-90,Getdate())
and a.PRINCIPAL_AMT > 0 and a.TRAN_NBR = 100
and a.TRAN_T_TYPE not in ('*DV!','*DV@','*DVT','*PA ','*PA!','*PA@','*PRA','*S' ,'*S !','*S @','PR*!','PR*@','PR**','QLS ','QLS!','QLS@')
and right(a.TRAN_T_TYPE,1) not in ('!','@')
and DESCRIPTION like '%deposit%'
group by s.MEMBER_NBR,a.MEMBER_NBR,s.OPEN_DATE,c.MemberNumber,TOT_DEPOSIT

