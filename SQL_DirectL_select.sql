/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (3000) [DL_LOAD_TS]
      ,[DL_LOAD_DATE]
      ,[LOAN_NBR]
      ,[MEMBER_NBR]
      ,[MI_CODE1]
      ,[MI_CODE2]
      ,[LAST_CYCLE_DATE]
      ,[MI_CODE3]
      ,[MIN_PMT_LAST_CYCLE]
      ,[ADDON_FIRST_DATE]
      ,[ADDON_FOR_ZERO_BAL]
      ,[ADDON_LAST_DATE]
      ,[PMT_RECEIVED_CYCLE]
      ,[ALT_PMT_AMT]
      ,[ALT_PMT_BEGIN_DATE]
      ,[ALT_PMT_CODE]
      ,[ALT_PMT_END_DATE]
      ,[ALT_PMT_SWAP_DATE]
      ,[APR]
      ,[AMTZ_METHOD]
      ,[AMTZ_TERM]
      ,[ANNUAL_FEE_DATE]
      ,[BALANCE]
      ,[BILLING_NOTICE_WAIVE]
      ,[BRANCH_NBR]
      ,[CANCEL_AMT]
      ,[CANCEL_COST]
      ,[CANCEL_DATE]
      ,[CANCEL_INT]
      ,[CANCEL_REASON]
      ,[CASH_ADVANCE]
      ,[CHARGE_OVERLIMIT_FEE]
      ,[CLASS]
      ,[CLASSIFICATION_CODE]
      ,[CLOSE_DATE]
      ,[CLOSE_ON_PAYOFF]
      ,[CLOSED]
      ,[COLLATERAL_CODE]
      ,[CONVERTED_VAR_PCT_PMT]
      ,[COUPONS]
      ,[COUPONS_THRU_DATE]
      ,[CREDIT_SCORE]
      ,[CSSD_SERIAL_NBR]
      ,[CURTAILMENTS_YTD]
      ,[CURTAILMENTS_PYR]
      ,[CYCLE_DAY]
      ,[DELQ_FUTURE]
      ,[DELQ_LAST_NOTICE]
      ,[DELQ_NBR_PMTS]
      ,[DELQ_NBR_PMTS_13TH]
      ,[BASE_RATE]
      ,[DELQ_NEXT_DUE_DATE]
      ,[DELQ_NOTICE_BIAS]
      ,[DELQ_AMT]
      ,[DELQ_NOTICE_SENT]
      ,[DELQ_NOTICE_WAIVE]
      ,[DELQ_VAR_PMT_LOC]
      ,[DELQ_VIA_MTHD_A]
      ,[DRAW_DATE]
      ,[DELQ_AMT_CYCLE]
      ,[END_OF_CURRENT_YEAR]
      ,[FEES_YTD]
      ,[FINAL_PMT_AMT]
      ,[FEES_PYR]
      ,[FINAL_PMT_DATE]
      ,[FIRST_ADDON]
      ,[FIRST_PMT_DATE]
      ,[FIRST_PMT_NOT_MADE]
      ,[FIRST_PMT_SHORT]
      ,[FIRST_RATE_CHNG]
      ,[FIRST_VAR_ANALYSIS]
      ,[FLEX_LOAN_PMT_FREEZE]
      ,[FLEX_LOAN_RATE_FREEZE]
      ,[FREQ]
      ,[GENERATE_1098]
      ,[INT_ACCRUED_CYCLE]
      ,[HAS_COBORROWERS]
      ,[HAS_CUSTOM_TITLE]
      ,[INT_DUE_CYCLE]
      ,[HAS_INHIBIT_ALL_SET]
      ,[HAS_INSURANCE]
      ,[HAS_STOCK_COLLATERAL]
      ,[HAS_STOPS]
      ,[HISTORY_CNT]
      ,[LAST_STMT_BAL]
      ,[HISTORY_LAST_SEQ]
      ,[INACTIVE]
      ,[LATE_CHRG_ASSESSED_CYCLE]
      ,[INCOL]
      ,[INCOL_DATE]
      ,[IGNORE_PLEDGE_SELF_PMT]
      ,[INS_CODE]
      ,[LATE_CHRG_CYCLE]
      ,[INS_DIS_ACT_CODE]
      ,[INS_DIS_CODE]
      ,[INS_DIS_MI]
      ,[INS_DIS_RATE]
      ,[INS_EXP_DATE]
      ,[INS_LIFE_ACT_CODE]
      ,[MIN_PMT]
      ,[INS_LIFE_CODE]
      ,[INS_LIFE_MI]
      ,[INS_LIFE_RATE]
      ,[INS_MONTHLY_ADDON_DAY]
      ,[INS_PAY_MONTHLY]
      ,[INS_PAY_WITH_STMT]
      ,[INS_PRIMARY_BIRTH_DATE]
      ,[INS_PRIMARY_SSN]
      ,[INS_SECONDARY_BIRTH_DATE]
      ,[INS_SECONDARY_SSN]
      ,[INS_SHR_NBR]
      ,[INS_UNEMP_CODE]
      ,[INS_UNEMP_MI]
      ,[INS_UNEMP_RATE]
      ,[INT_ACCRUED]
      ,[PAYMENTS_LAST_CYCLE]
      ,[INT_ACCRUED_DATE]
      ,[INT_DUE]
      ,[INT_ESCROW_YTD]
      ,[INT_LTD]
      ,[INT_ONLY]
      ,[PRECALC_NEW_RATE]
      ,[INT_PAID_QTR1]
      ,[INT_PAID_QTR2]
      ,[INT_PAID_QTR3]
      ,[PRIOR_CYCLE_DATE]
      ,[INT_PAID_QTR4]
      ,[INT_PYR]
      ,[PROJECTED_PAYOFF]
      ,[INT_YTD]
      ,[INV_PAY_INT_ON_FIRST_PMT]
      ,[INV_REMIT_ESCROW]
      ,[INV_REMIT_UNAPPLIED_FUNDS]
      ,[INV_SUBMITTED_TO_SALE]
      ,[INVESTOR_BAL]
      ,[INVESTOR_CTL_NBR]
      ,[INVESTOR_ESCROW_BAL]
      ,[INVESTOR_FIRST_PMT_DATE]
      ,[INVESTOR_INT_AMT]
      ,[INVESTOR_INT_DUE]
      ,[INVESTOR_LN_NBR]
      ,[INVESTOR_UNAPPLIED_FUNDS_BAL]
      ,[IS_CREDIT_CARD]
      ,[IS_CREDIT_CARD_AMTZ]
      ,[IS_DELQ]
      ,[IS_FLEX_LOAN]
      ,[IS_TRAINING_BRANCH]
      ,[STMT_DAYS]
      ,[LAST_CUST_CONT_DATE]
      ,[LAST_UPDATE_DATE]
      ,[LAST_UPDATE_TLR]
      ,[LATE_CHRG_ALT_PMT]
      ,[LATE_CHRG_MEMOED]
      ,[LATE_CHRG_MEMOED_PYR]
      ,[LATE_CHRG_DATE]
      ,[LATE_CHRG_NOTICE_DATE]
      ,[LATE_CHRG_PYR]
      ,[LATE_CHRG_WAIVE]
      ,[LATE_CHRG_YTD]
      ,[LIMIT]
      ,[LOAN_ATTRIBUTE_DELTA]
      ,[LOAN_NOTE_NBR]
      ,[LOAN_TYPE]
      ,[LOAN_TYPE_CHNG_DATE]
      ,[MANUAL_END_OF_FREEZE]
      ,[MET_ASSOC_CODE]
      ,[MET_ASSOC_DATE]
      ,[MET_CMT_CODE]
      ,[MET_DATE]
      ,[MET_STATUS_CODE]
      ,[MET_SUB_CODE]
      ,[MI_AMT_DUE1]
      ,[MI_AMT_DUE2]
      ,[MI_AMT_DUE3]
      ,[MI_FEE_PYR]
      ,[NEW_RATE_WILL_APPLY]
      ,[NEXT_DUE_BASE]
      ,[NEXT_DUE_DATE]
      ,[NEXT_DUE_WEEK_DAY]
      ,[NEXT_LOAN_CHANGE_CYCLE]
      ,[NEXT_LOAN_CHANGE_DB]
      ,[NEXT_LOAN_CHANGE_PMT_DUE]
      ,[NEXT_PMT_UPDATE_DATE]
      ,[NEXT_SCHEDULED_UPDATE_DATE]
      ,[NO_ADDONS]
      ,[NO_INT_ON_ESC_BAL]
      ,[NOT_BOOKED]
      ,[OFFICER]
      ,[OPEN_DATE]
      ,[ORIG_AMT]
      ,[ORIG_APR]
      ,[ORIG_LN_TYPE]
      ,[ORIG_PMT_PERIOD]
      ,[ORIG_RATE]
      ,[ORIG_TERM]
      ,[OTHER_SECURITY]
      ,[PAID_BY_PGN]
      ,[PARTIAL_PMT]
      ,[PMT_BASE]
      ,[PMT_FIXED]
      ,[PMT_HISTORY]
      ,[PMT_HISTORY_UPDATE_DATE]
      ,[PMT_MANUAL]
      ,[PMT_PGN]
      ,[PMT_SHR_NBR]
      ,[PMT_SKIP]
      ,[PMT_SOURCE]
      ,[POINTS_PAID]
      ,[PREV_NEXT_DUE_DATE]
      ,[PRIN_AT_PAYOFF]
      ,[PRIN_REDUCE_PYR]
      ,[PRIN_REDUCE_YTD]
      ,[PURCHASE_ADV_BAL_LAST_CYCLE]
      ,[PURPOSE_CODE]
      ,[RATE]
      ,[RATE_ALT]
      ,[RATE_BASE]
      ,[RATE_DELTA]
      ,[RATE_MANUAL]
      ,[RBL_DELTA]
      ,[REOPEN_DATE]
      ,[REOPENED]
      ,[RESIDENCE_1098_CODE]
      ,[RPL_DELTA]
      ,[SCHEDULED_PMT]
      ,[SCHEDULED_UPDATE_AUTH_AMT]
      ,[SCHEDULED_UPDATE_AUTH_CNT]
      ,[SCHEDULED_UPDATE_OLDEST]
      ,[PMT_SKIP_BEGIN_DATE]
      ,[SET_RATE_AT_ADDON]
      ,[SPLIT_RATE]
      ,[SPLIT_RATE_LIMIT]
      ,[STMT_FREQ]
      ,[STMT_FREQ_CHANGED]
      ,[STMT_LAST_THRU_DATE]
      ,[STOCK_PLEDGE_ORIG_VALUE]
      ,[STOCK_TYPE]
      ,[STUDENT_INT_PYR]
      ,[STUDENT_INT_YTD]
      ,[TIN]
      ,[TITLE]
      ,[USE_ARM_ANALYSIS]
      ,[USE_OLD_RATE_CHNG_TBL]
      ,[VAR_RATE_CODE]
      ,[VR_CHANGE_DATE]
      ,[VR_PMT_CHNG_CDE]
      ,[VR_PRIOR_PMT]
      ,[VR_PRIOR_RATE]
      ,[WAIVE_BILLING_NOTICE]
      ,[WAIVE_LATE_CHRG]
      ,[IS_WRITTEN_OFF]
      ,[WAIVE_NEXT_FEE]
      ,[WAIVE_OVERLIMIT_FEE]
      ,[WOFF_DATE]
      ,[LOAN_SUBTYPE]
      ,[HAS_ALT_PMT_SET_BY_BATCH]
      ,[HAS_PMT_MTD_99_USED]
      ,[DUE_DATE_ADV_CNT_LIMIT]
      ,[EXCESS_OF_PMT]
      ,[INT_RATE_ALT]
      ,[INT_RATE]
      ,[PROCESSING_CLASS]
      ,[MAX_LN_BAL]
      ,[HAS_DRAW_DATE]
      ,[HAS_FINAL_PMT_DATE]
      ,[TIMES_DELQ_90_DAYS]
      ,[TIMES_DELQ_60_DAYS]
      ,[VIN]
      ,[TIMES_DELQ_30_DAYS]
      ,[LAST_PER_UPD_DATETIME]
      ,[SKIP_JANUARY_PMT]
      ,[SKIP_FEBRUARY_PMT]
      ,[SKIP_MARCH_PMT]
      ,[SKIP_APRIL_PMT]
      ,[SKIP_MAY_PMT]
      ,[SKIP_JUNE_PMT]
      ,[SKIP_JULY_PMT]
      ,[SKIP_AUGUST_PMT]
      ,[SKIP_SEPTEMBER_PMT]
      ,[SKIP_OCTOBER_PMT]
      ,[SKIP_NOVEMBER_PMT]
      ,[SKIP_DECEMBER_PMT]
      ,[SKIP_DELINQ_LOANS_PMT]
      ,[USE_LOAN_SKIPS]
      ,[NEEDS_COUPONS_PRINTED]
      ,[HAS_CB]
      ,[PLAN_NBR]
      ,[OPEN_END_CATEGORY]
      ,[INS_SP_LIFE_TERM]
      ,[INS_SP_DISB_TERM]
      ,[INS_SP_CUD_TERM]
      ,[MONTHLY_TERM]
      ,[REFINANCE_COUNT]
      ,[FIRST_PMT_NOT_MADE_CNV]
      ,[LAST_REFINANCE_DATE]
      ,[XP_WORKSTATION_ID]
      ,[XP_OPERATOR_ID]
      ,[XPTIMESTAMP]
      ,[MET2_CCC_CODE]
      ,[MET2_CCC_SET_DATE]
      ,[MET2_CCC_RPT_CODE]
      ,[MET2_CCC_RPT_DATE]
      ,[IS_PSBK]
      ,[PSBK_LAST_HISTORY_ID]
      ,[HIGHEST_BAL_DATE]
      ,[HIGHEST_BAL]
      ,[LAST_PAYMENT_DATE]
      ,[IS_BUSINESS_LOAN]
      ,[COMMITMENT_INDIVIDUAL_ID]
      ,[COMMITMENT_NBR]
      ,[HAS_PAYMENT_SCHEDULE]
      ,[HAS_ACTIVE_PMT_DETAIL]
      ,[MORTGAGE_INS_CURRENT_YR]
      ,[MORTGAGE_INS_PAID_YTD]
      ,[MORTGAGE_INS_PAID_PRIOR_YTD]
      ,[MORTGAGE_INS_PAID_LTD]
      ,[PREPAYMENT_INFO_TXT]
      ,[HAS_INVESTOR_PARTICIPATION]
      ,[HAS_AUTO_INVESTOR_SPREAD]
      ,[INTEREST_TYPE]
      ,[IS_OPEN_ENDED]
      ,[MAIL_CYCLE_STMT]
      ,[MORTGAGE_INS_DEPS_YTD]
      ,[MORTGAGE_INS_DEPS_PRIOR_YTD]
      ,[MORTGAGE_INS_DEPS_LTD]
      ,[WOFF_AMOUNT]
      ,[WOFF_CYR_RECOV_AMT]
      ,[WOFF_TOT_RECOV_AMT]
      ,[WOFF_OLD_LOAN_TYPE]
      ,[WOFF_OLD_PRODUCT_CODE]
      ,[WOFF_OLD_TYPE]
      ,[LAST_BILL_DATE]
      ,[ML_ORIGINATOR_NBR]
      ,[DEFERRED_START_DATE]
      ,[IS_BALLOON_PAYOFF]
      ,[BALLOON_TERM]
      ,[IS_NON_REVOLVING]
      ,[TOTAL_DISBURSED_AMT]
      ,[PLEDGE_CODE]
      ,[IS_ADJUSTABLE_PAYMENT]
      ,[PS_AMORTIZATION]
      ,[TRANSFER_DAY]
      ,[PMT_MEMBER_NBR]
      ,[IS_INTEREST_ACCRUAL_INELIGIBLE]
      ,[PORTFOLIO_TYPE]
      ,[IS_INT_CALC_DAYS_CUSTOM]
      ,[INT_CALC_DAYS]
      ,[PREPAID_INT_CALC_DAYS]
      ,[PAYOFF_INT_CALC_DAYS]
      ,[PAYMENT_SKIP_COUNT]
      ,[MET2_FIRST_DELINQUENCY_SET_DATE]
      ,[MET2_FIRST_DELINQUENCY_RPT_DATE]
      ,[PREPMT_PENALTY_AMT]
      ,[PREPMT_PENALTY_DATE]
      ,[INITIAL_ANALYSIS_ESTIMATE_DATE]
      ,[IS_ESTIMATE_ANALYSIS_PERFORMED]
      ,[STMT_TYPE]
      ,[STMT_DELQ_MSG1]
      ,[STMT_DELQ_MSG2]
      ,[STMT_DELQ_MSG3]
      ,[STMT_INCLUDE_HUD_INFO]
      ,[IS_FIRST_RATE_CHNG_PERFORMED_ENABLED]
      ,[ALTERNATE_LOAN_ID]
      ,[RATE_LOCK_DATE]
      ,[CYR_STARTING_BALANCE]
      ,[PMT_HISTORY_PROFILE]
      ,[IS_MLA_COVERED]
      ,[PYR_STARTING_BALANCE]
      ,[LATE_CHRG_LTD]
      ,[PRIN_REDUCE_LTD]
      ,[RECOMM_STATUS_CODE]
      ,[STOP_PAY_CNT]
      ,[NUMBER_OF_PROPERTIES]
      ,[ORIGINAL_TERM]
      ,[ORIGINAL_LTV]
      ,[ORIGINAL_CR_BUREAU_CD]
      ,[ORIGINAL_CB_RISK_MODEL_CD]
      ,[ORIGINAL_CREDIT_SCORE]
      ,[MET2_TERM]
      ,[MET2_CALC_ACCT_STATUS_CODE]
      ,[MET2_CALC_ASC_RPT_CODE]
      ,[MET2_CALC_ASC_RPT_DATE]
      ,[MET2_CALC_ASC_SET_DATE]
      ,[MET2_PAYMENT_RATING]
      ,[MET2_PR_RPT_CODE]
      ,[ACQUISITION_DATE]
      ,[DEFER_PROFILE_REPORTING]
      ,[MET2_PHP_UPDT_DATE]
      ,[MET2_AMOUNT_PAST_DUE]
      ,[MET2_ACCOMMODATION_END_DATE]
      ,[RESET_LOAN_AFTER_DEFERRAL]
      ,[ACCOMMODATION_END_DATE]
      ,[ACCOMMODATION_START_DATE]
      ,[ACCOMMODATION_LAST_PMT_DATE]
      ,[REPORT_CURRENT_IF_CURED]
      ,[MET2_ORIG_PAST_DUE_AMT]
      ,[MET2_COLLECTED_AMT]
      ,[MET2_REPORT_PREVIOUS_LIMIT]
      ,[MET2_PREVIOUS_LIMIT]
      ,[MET2_BALLOON_DATE]
      ,[MET2_BALLOON_AMT]
      ,[MET2_CLOSED_TO_CHARGES]
      ,[MET2_POSTPONE_DELINQUENCY]
      ,[MET2_SCC_RPT_CODE]
      ,[MET2_SCC_RPT_DATE]
  FROM [DataMart].[dbo].[LOAN]
  WHERE LOAN_TYPE in (34,35,36,37)