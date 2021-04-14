------------------------- COVID-19 Vaccination Effectiveness -----------------------------------

-- Author: NHS England and Improvement Performance Analysis Team - April 2021 

-- Email: england.NHSEI-advanced-analytics@nhs.net 

-- github: https://github.com/NHSEI-Analytics


-- Summary: The analysis here contributes to an NHS England and Improvement study on COVID-19 vaccine effectiveness. 

-           For more information please see the pre-publication version of the study: 
            
-- Disclaimer: This is a simplified version of the work used for the final analysis.  
--             Record level data is used in the final analysis which has been suppressed in this script. 
--             File locations and database directories have been suppressed behind temp databases. 




The analysis is split into 3 sections: 

  - 01 Pairwise Matching 
 
        Applies the pairwise matching process to vaccination data. Matching vaccinated individuals to unvaccinated 
        individuals on a number of criteria. 

 - 02 Matched AE APC Join

        Takes the matched patients and joins any AE / APC hospitalisation records with COVID-19. 


 - 03 Bootstrapping Adjustment 
 
        Applies bootstrapping and an adjustment methodology to the matched data, for positive tests, AE admissions and APC admissions. 
        Outputs summary tables to excel for use with the publication. 
