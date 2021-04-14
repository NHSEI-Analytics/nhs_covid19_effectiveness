
@VaccStartDate As Date,
@VaccEndDate As Date

/* outer loop */
/* insert into table using a loop and a control table to define count boundaries */
IF OBJECT_ID('tempdb..#del_prog3') IS NOT NULL
DROP TABLE #del_prog3
create table #del_prog3 
(r int, iteration_date datetime)

IF OBJECT_ID('tempdb..#OP_ctrl3') IS NOT NULL
DROP TABLE #OP_ctrl3
create table #OP_ctrl3
(id int identity
, rn int)


insert into #OP_ctrl3 values (6)
insert into #OP_ctrl3 values (7)
insert into #OP_ctrl3 values (8)
insert into #OP_ctrl3 values (9)
insert into #OP_ctrl3 values (10)
insert into #OP_ctrl3 values (11)
insert into #OP_ctrl3 values (12)
insert into #OP_ctrl3 values (13)
insert into #OP_ctrl3 values (14)
insert into #OP_ctrl3 values (15)
insert into #OP_ctrl3 values (16)
insert into #OP_ctrl3 values (17)
insert into #OP_ctrl3 values (18)
insert into #OP_ctrl3 values (19)
insert into #OP_ctrl3 values (20)
insert into #OP_ctrl3 values (21)
insert into #OP_ctrl3 values (22)
insert into #OP_ctrl3 values (23)
insert into #OP_ctrl3 values (24)
insert into #OP_ctrl3 values (25)
insert into #OP_ctrl3 values (26)
insert into #OP_ctrl3 values (27)
insert into #OP_ctrl3 values (28)
insert into #OP_ctrl3 values (29)
insert into #OP_ctrl3 values (30)
insert into #OP_ctrl3 values (31)
insert into #OP_ctrl3 values (32)
insert into #OP_ctrl3 values (33)
insert into #OP_ctrl3 values (34)
insert into #OP_ctrl3 values (35)
insert into #OP_ctrl3 values (36)
insert into #OP_ctrl3 values (37)
insert into #OP_ctrl3 values (38)
insert into #OP_ctrl3 values (39)
insert into #OP_ctrl3 values (40)


DECLARE @id_control3 INT
DECLARE @id_control_max3 INT
DECLARE @ReportDelay int 

SELECT @id_control3 = (select top 1 ID from #OP_ctrl3)
SELECT @id_control_max3 = (select max(ID) from #OP_ctrl3)

SET @ReportDelay = (select rn from #OP_ctrl3 where id = @id_control3)

WHILE @id_control3 <= @id_control_max3
BEGIN

/* get cohort of patients who received vaccine and who didn't previously have COVID */
IF OBJECT_ID('tempdb..#temp1') IS NOT NULL
    DROP TABLE #temp1

SELECT @ReportDelay ReportDelay
, [Der_Pseudo_NHS_Number]
      ,[Age_Band_MPI]
	  --, CASE WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 80 and 82 THEN 'Female: 80-82 matched to 75-77'
			--	WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 80 and 82 THEN 'Male: 80-82 matched to 75-77'
			--	WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 83 and 84 THEN 'Female: 83-84 matched to 78-79'
			--	WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 83 and 84 THEN 'Male: 83-84 matched to 78-79'
			--ELSE [Age_Band_MPI] END Age_Band_calc
	  , CASE WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 80 and 81 THEN 'Female: 80-81 matched to 76-77'
				WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 80 and 81 THEN 'Male: 80-81 matched to 76-77'
				WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 82 and 83 THEN 'Female: 82-83 matched to 78-79'
				WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 82 and 83 THEN 'Male: 82-83 matched to 78-79'
			ELSE [Age_Band_MPI] END Age_Band_calc
	  ,case		
			when Care_Home_Flag is not null then 'Care Home Residents'
			when [Age_at_MPI_Census_Date] >= 80 and Care_Home_Flag is null then 'Non-Care Home Residents 80 and over'
			when [Age_at_MPI_Census_Date] < 80 and Care_Home_Flag is null then 'Non-Care Home Residents under 80'
	   end as Age_Control
	   -- New Ethnic Grouping Selection for 5 groups
	  ,CASE WHEN Ethnic_Group = 'White' THEN 'White'
			WHEN Ethnic_group = 'Black' THEN 'Black'
			WHEN Ethnic_subgroup in ('Pakistani', 'Bangladeshi') THEN 'Pakistani and Bangladeshi'
			WHEN (Ethnic_Group in ('Conflicted', 'Not Known') OR Ethnic_Group IS NULL) THEN 'Unknown'
		Else 'Other BAME' END Ethnic_Group_calc 
	  ,[Vaccination_Priority_Group]
	  ,[LSOA_2011_Code]
      ,[MSOA_2011_Code]
	  ,[COVID_UPRN]
	  , CASE WHEN [Care_Home_Flag] IS NULL THEN 0 else 1 end AS [Care_Home_Flag_Binary]
	  -- Pull in Earliest COVID19 Diagnosis Date instead of calculating here
	  ,[Earliest_COVID19_Diagnosis_Date] as [COVID_1st_recorded]
	, [Vaccination_Date_1st]
	, [Vaccination_Date_2nd]
	, Case
             When Frailty_Group IS NOT NULL Then 1
             When Extremely_Clinically_Vulnerable_Group = 1 Then 1
             When Core_Clinically_Vulnerable_Group = 1 Then 3
             Else 4
       End As Health_Risk,
       Case
             When living_with_young_flag = 1 Then 1
             When living_alone_flag = 1 Then 2
             Else 3
       End As Living_Arrangements
	 , Case 
			When IMD_Decile in (1,2) then 1
			When IMD_Decile in (3,4) then 2
			When IMD_Decile in (5,6) then 3
			When IMD_Decile in (7,8) then 4
			When IMD_Decile in (9,10) then 5
	   END AS IMD_Quintile
	   ,[Date_Of_Death_ONS]
	   , CASE WHEN Seasonal_Flu_Vaccination_Date_1st < '20201208' THEN 1 ELSE 0 END Flu_vaccine
	   , CASE WHEN Non_Elective_Stay_Flag_2M IS NULL THEN 3 ELSE Non_Elective_Stay_Flag_2M END Recent_NEL_Spell
  INTO [#temp1]
  FROM [#temp_vacc]
  WHERE [Vaccination_Date_1st] BETWEEN @VaccStartDate AND @VaccEndDate and 
		-- Again using earliest COVID 19 date instead of calculating here 
		([Earliest_COVID19_Diagnosis_Date] > Vaccination_Date_1st OR 
		[Earliest_COVID19_Diagnosis_Date] is null) AND 
		(Date_of_Death_ONS >= @VaccStartDate OR Date_of_Death_ONS IS NULL)
		AND Age_at_MPI_Census_Date BETWEEN 80 AND 83



/* get records where patient hasn't yet been vaccinated and doesn't have a history of COVID prior to vaccination period */
IF OBJECT_ID('tempdb..#temp2') IS NOT NULL
    DROP TABLE #temp2

SELECT @ReportDelay ReportDelay
	, [Der_Pseudo_NHS_Number]
      ,[Age_Band_MPI]
	  --, CASE WHEN  Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 75 and 77 THEN 'Female: 80-82 matched to 75-77'
			--	WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 75 and 77 THEN 'Male: 80-82 matched to 75-77'
			--	WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 78 and 79 THEN 'Female: 83-84 matched to 78-79'
			--	WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 78 and 79 THEN 'Male: 83-84 matched to 78-79'
			--ELSE [Age_Band_MPI] END Age_Band_calc
	  , CASE WHEN  Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 76 and 77 THEN 'Female: 80-81 matched to 76-77'
				WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 76 and 77 THEN 'Male: 80-81 matched to 76-77'
				WHEN Gender = 'Female' AND Age_at_MPI_Census_Date BETWEEN 78 and 79 THEN 'Female: 82-83 matched to 78-79'
				WHEN Gender = 'Male' AND Age_at_MPI_Census_Date BETWEEN 78 and 79 THEN 'Male: 82-83 matched to 78-79'
			ELSE [Age_Band_MPI] END Age_Band_calc
	  ,case		
			when Care_Home_Flag is not null then 'Care Home Residents'
			when [Age_at_MPI_Census_Date] >= 80 and Care_Home_Flag is null then 'Non-Care Home Residents 80 and over'
			when [Age_at_MPI_Census_Date] < 80 and Care_Home_Flag is null then 'Non-Care Home Residents under 80'
	   end as Age_Control
	   -- New Ethnic Grouping selection for 5 groups
      ,CASE WHEN Ethnic_Group = 'White' THEN 'White'
			WHEN Ethnic_group = 'Black' THEN 'Black'
			WHEN Ethnic_subgroup in ('Pakistani', 'Bangladeshi') THEN 'Pakistani and Bangladeshi'
			WHEN (Ethnic_Group in ('Conflicted', 'Not Known') OR Ethnic_Group IS NULL) THEN 'Unknown'
		Else 'Other BAME' END Ethnic_Group_calc
		-- Updated for new Priority Groups. Those between 75 and 79 to be matched to vaccinated cohort in group 2A
	  ,CASE WHEN Age_at_MPI_Census_Date BETWEEN 75 and 79 THEN '2A' ELSE [Vaccination_Priority_Group] END [Vaccination_Priority_Group]
      ,[MSOA_2011_Code]
	  ,[LSOA_2011_Code]
	  ,[COVID_UPRN]
	  , CASE WHEN [Care_Home_Flag] IS NULL THEN 0 else 1 end AS [Care_Home_Flag_Binary]
	  -- Pull in Earliest COVID19 Diagnosis Date instead of calculating here
	  ,[Earliest_COVID19_Diagnosis_Date] as [COVID_1st_recorded]
	  ,Case
             When Frailty_Group IS NOT NULL Then 1
             When Extremely_Clinically_Vulnerable_Group = 1 Then 1
             When Core_Clinically_Vulnerable_Group = 1 Then 3
             Else 4
       End As Health_Risk,
       Case
             When living_with_young_flag = 1 Then 1
             When living_alone_flag = 1 Then 2
             Else 3
       End As Living_Arrangements
	   , Case 
			When IMD_Decile in (1,2) then 1
			When IMD_Decile in (3,4) then 2
			When IMD_Decile in (5,6) then 3
			When IMD_Decile in (7,8) then 4
			When IMD_Decile in (9,10) then 5
	   END AS IMD_Quintile
	   ,[Date_Of_Death_ONS]
	   , CASE WHEN Seasonal_Flu_Vaccination_Date_1st < '20201208' THEN 1 ELSE 0 END Flu_vaccine
	   , CASE WHEN Non_Elective_Stay_Flag_2M IS NULL THEN 3 ELSE Non_Elective_Stay_Flag_2M END Recent_NEL_Spell
  INTO #temp2
  FROM [#temp_vacc]
  -- Filter to exclude those vaccinated after last good date of data (today's date -8)
  WHERE ([Vaccination_Date_1st] IS NULL OR [Vaccination_Date_1st] > DATEADD(day,-14,(DATEADD(day, @ReportDelay*-1, convert(date, '2021-02-09' )))))
  AND ([Earliest_COVID19_Diagnosis_Date] > @VaccStartDate OR 
	  [Earliest_COVID19_Diagnosis_Date] IS NULL)
	AND 
	(Date_of_Death_ONS >= @VaccStartDate OR Date_of_Death_ONS IS NULL)
	AND Age_at_MPI_Census_Date BETWEEN 76 AND 79
	AND [Deducted_flag] <> 1
	



/* insert into table using a loop and a control table to define count boundaries */
IF OBJECT_ID('tempdb..#del_prog2') IS NOT NULL
DROP TABLE #del_prog2
create table #del_prog2 
(r int, iteration_date datetime)

IF OBJECT_ID('tempdb..#OP_ctrl2') IS NOT NULL
DROP TABLE #OP_ctrl2
create table #OP_ctrl2
(id int identity
, rn int)

insert into #OP_ctrl2 values (1)
insert into #OP_ctrl2 values (2)
insert into #OP_ctrl2 values (3)
insert into #OP_ctrl2 values (4)
insert into #OP_ctrl2 values (5)

DECLARE @id_control INT
DECLARE @id_control_max INT
DECLARE @rn int 

SELECT @id_control = (select top 1 ID from #OP_ctrl2)
SELECT @id_control_max = (select max(ID) from #OP_ctrl2)

SET @rn = (select rn from #OP_ctrl2 where id = @id_control)

WHILE @id_control <= @id_control_max
BEGIN
  
/* add row numbers to tables */
IF OBJECT_ID('tempdb..#temp1a') IS NOT NULL
    DROP TABLE #temp1a

SELECT ReportDelay
, [Der_Pseudo_NHS_Number]
      ,[Age_Band_MPI]
	  , Age_Band_calc
	  , Age_Control
	  ,[Ethnic_group_calc]
	  ,[Health_Risk]
	  ,[Living_Arrangements]
	  ,[IMD_Quintile]
	  ,[Vaccination_Priority_Group]
      ,[MSOA_2011_Code]
	  ,[LSOA_2011_Code]
	  ,[COVID_UPRN]
	  ,[Care_Home_Flag_Binary]
	  ,[COVID_1st_recorded]
	  ,[Vaccination_Date_1st]
	  ,[Vaccination_Date_2nd]
	  ,[Date_Of_Death_ONS]
	  ,[Flu_Vaccine]
	  ,[Recent_NEL_Spell]
	  , ROW_NUMBER() OVER (PARTITION BY [MSOA_2011_Code]
										, Age_Band_Calc 
										, [Care_Home_Flag_Binary]
										, Age_Control
										, [Vaccination_Priority_Group]
										, Ethnic_Group_calc
										, IMD_Quintile
										, Health_Risk
										, Living_Arrangements
										, Flu_Vaccine
										, Recent_NEL_Spell
							ORDER BY  ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)) ) rn
into #temp1a
FROM #temp1


IF OBJECT_ID('tempdb..#temp2a') IS NOT NULL
    DROP TABLE #temp2a

SELECT [Der_Pseudo_NHS_Number]
      ,[Age_Band_MPI]
	  , Age_Band_calc
	  ,Age_Control
      ,[Ethnic_group_calc]
	  ,[Health_Risk]
	  ,[Living_Arrangements]
	  , IMD_Quintile
	  ,[Vaccination_Priority_Group]
      ,[MSOA_2011_Code]
	  ,[LSOA_2011_Code]
	  ,[COVID_UPRN]
	  , [Care_Home_Flag_Binary]
	  , [COVID_1st_recorded]
	  ,[Date_Of_Death_ONS]
	  ,[Flu_Vaccine]
	  ,[Recent_NEL_Spell]
	  , ROW_NUMBER() OVER (PARTITION BY [MSOA_2011_Code]
										, Age_Band_calc
										, Age_Control
										, [Care_Home_Flag_Binary]
										, [Vaccination_Priority_Group]
										, Ethnic_Group_calc
										, IMD_Quintile
										, Health_Risk
										, Living_Arrangements
										, Flu_Vaccine
										, Recent_NEL_Spell
							ORDER BY  ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)) ) rn
into #temp2a
FROM #temp2


/*DROP INDEX idx_t2a ON #temp2a*/
CREATE NONCLUSTERED INDEX idx_t2a
ON #temp2a (Age_Band_calc,[Age_Control],[Vaccination_Priority_Group],[MSOA_2011_Code],[Care_Home_Flag_Binary], [Ethnic_Group_calc],[rn],[COVID_1st_recorded],[IMD_Quintile],[Living_Arrangements],[Health_Risk], [Flu_Vaccine], [Recent_NEL_Spell])
INCLUDE ([Der_Pseudo_NHS_Number],[COVID_UPRN])


/* join results */
IF OBJECT_ID('tempdb..#temp3') IS NOT NULL
    DROP TABLE #temp3

SELECT t1a.ReportDelay
, t1a.Der_Pseudo_NHS_Number  Der_Pseudo_NHS_Number_vacc
, t1a.[Vaccination_Date_1st]
, t1a.[Vaccination_Date_2nd]
, t1a.[MSOA_2011_Code]
, t1a.[Age_Band_calc]
, t1a.[Age_Control]
, t1a.[Vaccination_Priority_Group]
, t1a.[COVID_UPRN]
, t1a.[Care_Home_Flag_Binary]
, t1a.[Ethnic_Group_calc]
, t1a.[COVID_1st_recorded]		COVID_1st_recorded_vacc
, t1a.Health_Risk
, t1a.Living_Arrangements
, t1a.IMD_Quintile
, t1a.Flu_Vaccine
, t1a.Recent_NEL_Spell
, t1a.rn
, t2a.Der_Pseudo_NHS_Number		Der_Pseudo_NHS_Number_not_vacc
, t2a.[COVID_UPRN]				COVID_UPRN_not_vacc
, t2a.[COVID_1st_recorded]
, t2a.rn						rn_not_vacc
INTO #temp3
FROM #temp1a t1a
INNER JOIN #temp2a t2a
on t1a.[MSOA_2011_Code] = t2a.[MSOA_2011_Code]
AND t1a.[Age_Band_calc] = t2a.[Age_Band_calc]
AND t1a.[Vaccination_Priority_Group]  = t2a.[Vaccination_Priority_Group]
AND t1a.[Care_Home_Flag_Binary]  = t2a.[Care_Home_Flag_Binary]
--AND t1a.[Age_Control] = t2a.[Age_Control]
AND t1a.Ethnic_Group_calc = t2a.Ethnic_Group_calc
AND t1a.[COVID_UPRN] <> t2a.[COVID_UPRN]
AND t1a.Health_Risk = t2a.Health_Risk
AND t1a.Living_Arrangements = t2a.Living_Arrangements
AND t1a.IMD_Quintile = t2a.IMD_Quintile
AND t1a.Flu_vaccine = t2a.Flu_vaccine
AND t1a.Recent_NEL_Spell = t2a.Recent_NEL_Spell
AND t1a.rn = t2a.rn
AND (t2a.[COVID_1st_recorded] > t1a.[Vaccination_Date_1st] OR t2a.[COVID_1st_recorded] IS NULL)
AND (t2a.[Date_Of_Death_ONS] > t1a.[Vaccination_Date_1st] OR t2a.[Date_Of_Death_ONS] IS NULL)

/* in the matched data how many patients were in hospital at the time of vaccination? */
IF OBJECT_ID('tempdb..#tempA') IS NOT NULL
    DROP TABLE #tempA

SELECT Der_Pseudo_NHS_Number_vacc
, Vaccination_Date_1st
, COVID_UPRN_vacc
, MAX(apc_record_vacc) apc_record_vacc
, COVID_1st_recorded_vacc
, Der_Pseudo_NHS_Number_not_vacc
, COVID_UPRN_not_vacc
, MAX(apc_record_not_vacc) apc_record_not_vacc
, COVID_1st_recorded_not_vacc
INTO #tempA
FROM
	(
	SELECT Der_Pseudo_NHS_Number_vacc
	, Vaccination_Date_1st
	, COVID_UPRN COVID_UPRN_vacc
	, CASE WHEN vaccination_date_1st BETWEEN ip1.Attendance_Date and ip1.Discharge_Date THEN 1 ELSE 0 END apc_record_vacc
	, COVID_1st_recorded_vacc
	, Der_Pseudo_NHS_Number_not_vacc
	, COVID_UPRN_not_vacc
	, CASE WHEN vaccination_date_1st BETWEEN ip2.Attendance_Date and ip2.Discharge_Date  THEN 1 ELSE 0 END apc_record_not_vacc
	, COVID_1st_recorded COVID_1st_recorded_not_vacc
	FROM #temp3 t3
	LEFT OUTER JOIN [#temp_ip_spells] ip1
	on t3.Der_Pseudo_NHS_Number_vacc = ip1.Patient_ID
	LEFT OUTER JOIN [#temp_ip_spells] ip2
	on t3.Der_Pseudo_NHS_Number_not_vacc = ip2.Patient_ID
	) a
GROUP BY 
Der_Pseudo_NHS_Number_vacc
, COVID_UPRN_vacc
, Vaccination_Date_1st
, COVID_1st_recorded_vacc
, Der_Pseudo_NHS_Number_not_vacc
, COVID_UPRN_not_vacc
, COVID_1st_recorded_not_vacc


/* write to output table */

INSERT INTO [#temp_out_1]
SELECT ReportDelay
, @rn Match_Count
, t3.Der_Pseudo_NHS_Number_vacc
, Age_Band_calc					Age_Band_calc_vacc
, Age_Control					Age_Control_vacc
, Vaccination_Priority_Group	Vaccination_Priority_Group_vacc
, MSOA_2011_Code				MSOA_2011_Code_vacc
, Care_Home_Flag_Binary			Care_Home_Flag_Binary_vacc
, Ethnic_group_calc				Ethnic_Group_calc_vacc
, Health_Risk					Health_Risk_vacc
, Living_Arrangements			Living_Arrangements_vacc
, IMD_Quintile					IMD_Quintile_vacc
, Flu_Vaccine
, Recent_NEL_Spell
, t3.COVID_1st_recorded_vacc
, t3.Vaccination_Date_1st
, t3.Vaccination_Date_2nd
, t3.Der_Pseudo_NHS_Number_not_vacc	Der_Pseudo_NHS_Number
, Age_Band_Calc
, t3.COVID_UPRN_not_vacc			COVID_UPRN 		
, COVID_1st_recorded			
FROM #temp3 t3
INNER JOIN #tempA tA
on t3.Der_Pseudo_NHS_Number_vacc = tA.Der_Pseudo_NHS_Number_vacc
WHERE tA.apc_record_vacc = 0 
AND tA.apc_record_not_vacc = 0

/* get percentages to include in output */

INSERT INTO [#temp_out_2]
SELECT * 
FROM
	(
	select ReportDelay
	, @rn rn
	, 0 match_count
	, count(1) cnt from #temp1
	GROUP BY ReportDelay
	
	union all
	select ReportDelay
	, @rn
	, 0 match_count
	, count(1) cnt from #temp3
	GROUP BY ReportDelay
	) a
ORDER BY Match_Count



/* completeness 2 */
INSERT INTO [#temp_out_3]

SELECT * 
FROM
	(
	select ReportDelay
	, @rn rn
	, 0 match_count
	, 'All' Age_Control
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END Vaccination_Window

	, count(1) cnt from #temp1
	
	GROUP BY  ReportDelay
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END 
	
	union all

	select ReportDelay
	, @rn rn
	, 0 match_count
	, Age_Control
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END  Vaccination_Window

	, count(1) cnt from #temp1
	
	GROUP BY ReportDelay
	, Age_Control
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END 
	
	union all
	select ReportDelay 
	, @rn 
	, match_count
	, Age_Control_vacc
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END  Vaccination_Window
	, count(1) cnt from [#temp_out_1]

	GROUP BY ReportDelay
	, Match_count
	,  Age_Control_vacc
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END 

	union all

	select  ReportDelay 
	, @rn 
	, match_count
	, 'All' Age_Control
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END  Vaccination_Window
	, count(1) cnt from [#temp_out_1]
	group by ReportDelay
	, Match_Count
	, CASE WHEN Vaccination_Date_1st BETWEEN '2020-12-15' AND '2020-12-20' THEN '15/12/2020 - 20/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2020-12-21' AND '2020-12-24' THEN '21/12/2020 - 24/12/2020'
			WHEN Vaccination_Date_1st BETWEEN '2021-01-07' AND '2021-01-10' THEN '07/01/2021 - 10/01/2021'
			ELSE 'ERROR' END 
	) a
ORDER BY Match_Count
, Age_Control
, Vaccination_Window


/* COVID in the control group */

INSERT INTO [#temp_out_1]

SELECT ReportDelay
, @rn rn
, count(1) cnt
, SUM(CASE WHEN [COVID_1st_recorded] IS NOT NULL THEN 1 ELSE 0 END) cnt_COVID
FROM #temp2
GROUP BY ReportDelay
  
     -- very important to obtain the latest rowcount to avoid infinite loops 
   DELETE FROM #OP_ctrl2 where id = @id_control
   insert into #del_prog2
   select @id_control, getdate()
   SET @id_control = @id_control+1
   SET @rn = (select rn from #OP_ctrl2 where id = @id_control)
   CONTINUE
     
   -- next batch
   END




/* end outer loop */

     -- very important to obtain the latest rowcount to avoid infinite loops 
   DELETE FROM #OP_ctrl3 where id = @id_control
   insert into #del_prog3
   select @id_control, getdate()
   SET @id_control3 = @id_control3+1
   SET @ReportDelay = (select rn from #OP_ctrl3 where id = @id_control3)
   CONTINUE
     
   -- next batch
   END
