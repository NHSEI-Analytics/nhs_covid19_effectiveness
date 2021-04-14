The R script contains the data processing following the case matching. We first sample the case matched data with replacement. This process is applied 100 times for each of the 5 matched samples, which is repeated for the 3 areas of interest - positive COVID-19 cases, A&E attendances with COVID-19 and admittance to hospital through A&E with COVID-19. 

To correct for a bias where individuals in the control group contain a progressively higher proportion of people who test positive for COVID-19, we apply a correction to sequentially adjust event rates in the intervention and control group. 

With the adjusted set of (100 * 5) samples values for each of the key statistics, we then summarise the data. This includes preparing means across the samples and cumulative totals of the means. Confidence intervals are calculated from the distribution of the statistics. For graphical purposes only, seven-day moving averages of the rates are calculated. 

The script then generates two outputs. An excel file with the summary tables and automatically generated charts and a set of RDS files of the summaries for further graphical development. 
