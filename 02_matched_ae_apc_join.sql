  
------------------------- COVID-19 Vaccination Effectiveness - Join AE / APC Data -----------------------------------

-- Author: NHS England and Improvement Performance Analysis Team - April 2021 
-- Email: england.NHSEI-advanced-analytics@nhs.net 
-- github: https://github.com/NHSEI-Analytics

-- Summary: This script joins matched data to AE / APC records to estimate the number of AE / APC attendances with COVID

-- Disclaimer: This is a simplified version of the script used for the final analysis.  
--             Record level data is used in the final analysis which has been suppressed in this script. 
--             File locations and database directories have been suppressed behind temp databases. 

/* get A&E attendances */

IF OBJECT_ID('tempdb..#temp1') IS NOT NULL					
    DROP TABLE #temp1	

SELECT *
into #temp1
FROM
(
SELECT x.[NCDR_ID]
, x.[Patient_ID]
, x.Attendance_Date
, x.Provider_Current
, Case  When (x.Dimention_8 like '%1240751000000100%' 
			OR x.Dimention_8 like '%1300721000000109%' 
			OR x.Dimention_8 like '%1300731000000106%' 
			OR x.Dimention_8 like '%1240761000000102%' 
			OR x.Dimention_8 like '%1240561000000108%' 
			OR x.Dimention_8 like '%1240571000000101%' 
			OR x.Dimention_8 like '%1240541000000107%' 
			OR x.Dimention_8 like '%1240531000000103%' 
			OR x.Dimention_8 like '%1240521000000100%' 
			OR x.Dimention_8 like '%1240551000000105%' 
			OR x.Dimention_8 like '%1325171000000109%'  
			OR x.Dimention_8 like '%1325181000000106%' 
			OR x.Dimention_8 like '%1325161000000102%') Then 'COVID-19 Diagnosis' 
        When (x.Clinical_Disease_Notification like '%1240751000000100%' 
			OR x.Clinical_Disease_Notification like '%1300721000000109%' 
			OR x.Clinical_Disease_Notification like '%1300731000000106%' 
			OR x.Clinical_Disease_Notification like '%1240761000000102%' 
			OR x.Clinical_Disease_Notification like '%1240561000000108%' 
			OR x.Clinical_Disease_Notification like '%1240571000000101%' 
			OR x.Clinical_Disease_Notification like '%1240541000000107%' 
			OR x.Clinical_Disease_Notification like '%1240531000000103%' 
			OR x.Clinical_Disease_Notification like '%1240521000000100%' 
			OR x.Clinical_Disease_Notification like '%1240551000000105%' 
			OR x.Clinical_Disease_Notification like '%1325171000000109%'  
			OR x.Clinical_Disease_Notification like '%1325181000000106%' 
			OR  x.Clinical_Disease_Notification like '%1325161000000102%') Then 'COVID-19 Notification' Else 'Other' 
		End As ECDS_COVID
, z.[Earliest_Specimen_Date]
, z.[interval_spec_attend]
, z.Pillar
, COALESCE(z.[spec_attend_flag], 0) [spec_attend_flag]
FROM #temp_ec_daily As x
LEFT JOIN #temp_ec_daily_phe As z
	ON x.NCDR_ID = z.NCDR_ID 
	AND Interval_spec_attend between -6 and 14 
	AND spec_attend_flag = 1
INNER JOIN #temp_ec_daily_dq As dq
	ON x.Provider_Current = dq.Provider_Current 
	AND dq.DiagThreshold = '0.75' 
	AND dq.First_Good_count_CD IS NOT NULL 
	AND dq.Last_Good_count_CD >= '20210317'
) a
WHERE (ECDS_COVID IN ('COVID-19 Diagnosis' , 'COVID-19 Notification') 
		OR [Earliest_Specimen_Date] IS NOT NULL)



/* pick earliest COVID date if there's more than one row for a patient */
IF OBJECT_ID('tempdb..#temp2') IS NOT NULL					
    DROP TABLE #temp2	

SELECT Patient_ID
, MIN(Attendance_Date) min_att
INTO #temp2
FROM #temp1
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp3') IS NOT NULL					
    DROP TABLE #temp3	

SELECT t1.*
INTO #temp3
FROM #temp1 t1
INNER JOIN #temp2 t2
ON t1.Patient_ID = t2.Patient_ID
AND t1.Attendance_Date = t2.min_att


/* tie break on ID */
IF OBJECT_ID('tempdb..#temp4') IS NOT NULL					
    DROP TABLE #temp4	

SELECT Patient_ID
, MIN(NCDR_ID) min_NCDR
INTO #temp4
FROM #temp3
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp5') IS NOT NULL					
    DROP TABLE #temp5	

SELECT t3.*
, CASE WHEN earliest_specimen_date IS NOT NULL AND ECDS_COVID in ('COVID-19 Diagnosis', 'COVID-19 Notification') THEN 'Both'
				WHEN earliest_specimen_date is NULL then 'ECDS'								
				WHEN ECDS_COVID = 'Other' then 'Testing'				
				ELSE 'ERROR' END src	
INTO #temp5
FROM #temp3 t3
INNER JOIN #temp4 t4
ON t3.Patient_ID = t4.Patient_ID
AND t3.NCDR_ID = t4.min_NCDR


/* get admissions via A&E  */

IF OBJECT_ID('tempdb..#temp6') IS NOT NULL					
    DROP TABLE #temp6	

SELECT *
into #temp6
FROM
(
SELECT x.[NCDR_ID]
, x.[Patient_ID]
, x.Attendance_Date
, x.Provider_Current
, x.Dimention_6
, Case  When (x.Dimention_8 like '%1240751000000100%' 
			OR x.Dimention_8 like '%1300721000000109%' 
			OR x.Dimention_8 like '%1300731000000106%' 
			OR x.Dimention_8 like '%1240761000000102%' 
			OR x.Dimention_8 like '%1240561000000108%' 
			OR x.Dimention_8 like '%1240571000000101%' 
			OR x.Dimention_8 like '%1240541000000107%' 
			OR x.Dimention_8 like '%1240531000000103%' 
			OR x.Dimention_8 like '%1240521000000100%' 
			OR x.Dimention_8 like '%1240551000000105%' 
			OR x.Dimention_8 like '%1325171000000109%'  
			OR x.Dimention_8 like '%1325181000000106%' 
			OR x.Dimention_8 like '%1325161000000102%') Then 'COVID-19 Diagnosis' 
        When (x.Clinical_Disease_Notification like '%1240751000000100%' 
			OR x.Clinical_Disease_Notification like '%1300721000000109%' 
			OR x.Clinical_Disease_Notification like '%1300731000000106%' 
			OR x.Clinical_Disease_Notification like '%1240761000000102%' 
			OR x.Clinical_Disease_Notification like '%1240561000000108%' 
			OR x.Clinical_Disease_Notification like '%1240571000000101%' 
			OR x.Clinical_Disease_Notification like '%1240541000000107%' 
			OR x.Clinical_Disease_Notification like '%1240531000000103%' 
			OR x.Clinical_Disease_Notification like '%1240521000000100%' 
			OR x.Clinical_Disease_Notification like '%1240551000000105%' 
			OR x.Clinical_Disease_Notification like '%1325171000000109%'  
			OR x.Clinical_Disease_Notification like '%1325181000000106%' 
			OR  x.Clinical_Disease_Notification like '%1325161000000102%') Then 'COVID-19 Notification' Else 'Other' 
		End As ECDS_COVID
, z.[Earliest_Specimen_Date]
, z.[interval_spec_attend]
, z.Pillar
, COALESCE(z.[spec_attend_flag], 0) [spec_attend_flag]
FROM #temp_ec_daily As x
LEFT JOIN #temp_ec_daily_phe As z
	ON x.NCDR_ID = z.NCDR_ID 
	AND Interval_spec_attend between -6 and 14 
	AND spec_attend_admit_flag = 1
INNER JOIN #temp_ec_daily_dq As dq
	ON x.Provider_Current = dq.Provider_Current 
	AND dq.DiagThreshold = '0.75' 
	AND dq.First_Good_count_CD IS NOT NULL 
	AND dq.Last_Good_count_CD >= '20210317'
) a
WHERE ((ECDS_COVID IN ('COVID-19 Diagnosis' , 'COVID-19 Notification') AND Dimention_6 IN('Admitted to hospital', 'Transferred to other provider', 'Patient died'))
		OR [Earliest_Specimen_Date] IS NOT NULL)


/* pick earliest  date if there's more than one row for a patient */
IF OBJECT_ID('tempdb..#temp7') IS NOT NULL					
    DROP TABLE #temp7	

SELECT Patient_ID
, MIN(Attendance_Date) min_att
INTO #temp7
FROM #temp6
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp8') IS NOT NULL					
    DROP TABLE #temp8	

SELECT t6.*
INTO #temp8
FROM #temp6 t6
INNER JOIN #temp7 t7
ON t6.Patient_ID = t7.Patient_ID
AND t6.Attendance_Date = t7.min_att


/* tie break on ID */
IF OBJECT_ID('tempdb..#temp9') IS NOT NULL					
    DROP TABLE #temp9	

SELECT Patient_ID
, MIN(NCDR_ID) min_NCDR
INTO #temp9
FROM #temp8
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp10') IS NOT NULL					
    DROP TABLE #temp10	

SELECT t8.*
, CASE WHEN earliest_specimen_date IS NOT NULL AND ECDS_COVID in ('COVID-19 Diagnosis', 'COVID-19 Notification') THEN 'Both'
				WHEN earliest_specimen_date is NULL then 'ECDS'								
				WHEN ECDS_COVID = 'Other' then 'Testing'				
				ELSE 'ERROR' END src	
INTO #temp10
FROM #temp8 t8
INNER JOIN #temp9 t9
ON t8.Patient_ID = t9.Patient_ID
AND t8.NCDR_ID = t9.min_NCDR


/* get admissions to APC  */

IF OBJECT_ID('tempdb..#temp11') IS NOT NULL					
    DROP TABLE #temp11	

SELECT *
into #temp11
FROM
(
SELECT x.[NCDR_ID]
, x.[Patient_ID]
, x.Attendance_Date
, x.Provider_Current
, x.Dimention_6
, Case When (x.Dimention_8 LIKE '%U071%'  OR x.Dimention_8 LIKE '%U072%' OR x.Dimention_8 LIKE '%U04%' OR x.Dimention_8 LIKE '%B972%')
		 Then 'Covid-19 Diagnosis' 
		 Else 'Other/Unknown Diagnosis' 
		 End As APCS_COVID
, z.[Earliest_Specimen_Date]
, z.[interval_spec_attend]
, z.Pillar
, COALESCE(z.[spec_admit_flag], 0) [spec_admit_flag]
, x.Length_of_Stay
FROM #temp1_ip_daily As x
LEFT JOIN #temp_ip_daily_phe As z
	ON x.NCDR_ID = z.NCDR_ID 
	AND Interval_spec_attend between -6 and 14 
	AND spec_admit_flag = 1
INNER JOIN #temp_ip_daily_dq As dq
	ON x.Provider_Current = dq.Provider_Current 
	AND dq.Activity_Type = 'Total'
	AND dq.DiagThreshold = '0.75' 
	AND dq.First_Good_count_WD IS NOT NULL 
	AND dq.Last_Good_count_WD >= '20210317'
) a
WHERE (APCS_COVID IN ('COVID-19 Diagnosis' ) 
		OR [Earliest_Specimen_Date] IS NOT NULL)


/* pick earliest  date if there's more than one row for a patient */
IF OBJECT_ID('tempdb..#temp12') IS NOT NULL					
    DROP TABLE #temp12	

SELECT Patient_ID
, MIN(Attendance_Date) min_att
INTO #temp12
FROM #temp11
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp13') IS NOT NULL					
    DROP TABLE #temp13	

SELECT t11.*
INTO #temp13
FROM #temp11 t11
INNER JOIN #temp12 t12
ON t11.Patient_ID = t12.Patient_ID
AND t11.Attendance_Date = t12.min_att


/* tie break on ID */
IF OBJECT_ID('tempdb..#temp14') IS NOT NULL					
    DROP TABLE #temp14	

SELECT Patient_ID
, MIN(NCDR_ID) min_NCDR
INTO #temp14
FROM #temp13
GROUP BY Patient_ID

/* join back to create one row per patient */
IF OBJECT_ID('tempdb..#temp15') IS NOT NULL					
    DROP TABLE #temp15	

SELECT t13.*
, CASE WHEN earliest_specimen_date IS NOT NULL AND APCS_COVID in ('COVID-19 Diagnosis') THEN 'Both'
				WHEN earliest_specimen_date is NULL then 'APCS'								
				WHEN APCS_COVID = 'Other/Unknown Diagnosis' then 'Testing'				
				ELSE 'ERROR' END src	
INTO #temp15
FROM #temp13 t13
INNER JOIN #temp14 t14
ON t13.Patient_ID = t14.Patient_ID
AND t13.NCDR_ID = t14.min_NCDR
