############################################## COVID-19 VACCINATION EFFECTIVENESS - ANALYSIS PIPELINE #################################################

## Author: NHS England and Improvement Performance Analysis Team - April 2021 
## Email: england.NHSEI-advanced-analytics@nhs.net 
## github: https://github.com/NHSEI-Analytics

## Summary: This script reads matched data of vaccinated and unvaccinated cohorts, applies a bootstrapping and adjustment methodology, 
##          summarises the results, and outputs to an excel workbook with accompanying ggplot2 charts. 

## Disclaimer: This is a simplified version of the script used in the final analysis. 
##             Record level data is used in the final analysis which has been suppressed in this script. 
##             File locations and database directories have been suppressed behind text files. 


# Users will need to ensure the below packages are installed
library(tidyverse)
library(DBI)
library(openxlsx)
library(zoo)

# Read raw matched data
# =====================

## Define sql script and database connection arguments (suppressed for the purpose of code reporting)
sql_script <- read.delim("sql.txt")
dsn <- read.delim("dsm.txt")
database <- read.delim("database.txt")

# Open database connection
con_ncdr <- DBI::dbConnect(drv = odbc::odbc(), dsn = dsn, database = database)

# Read raw data from database
dat_raw_dec15to20_ctrl76to79_extended <- DBI::dbGetQuery(conn = con_ncdr, statement = sql_script)


# Prepare datasets
# ================

dat_raw_positive_dec15to20_ctrl76to79_extended <- dat_raw_dec15to20_ctrl76to79_extended %>%
  select(ReportDelay, Match_Count, VaccToCOVID_vacc, VaccToCOVID_not_vacc, COVID_in_time_vacc, COVID_in_time_not_vacc,
         COVID_1st_recorded_vacc, COVID_1st_recorded_not_vacc)

dat_raw_ae_attend_dec15to20_ctrl76to79_extended <- dat_raw_dec15to20_ctrl76to79_extended %>%
  select(ReportDelay, Match_Count, VaccToCOVID_vacc = VaccToAE_Attend_vacc, VaccToCOVID_not_vacc = VaccToAE_Attend_not_vacc,
         COVID_in_time_vacc = AE_Attend_in_time_vacc, COVID_in_time_not_vacc = AE_Attend_in_time_not_vacc, 
         COVID_1st_recorded_vacc = AE_Attendance_Date_vacc, COVID_1st_recorded_not_vacc = AE_Attendance_Date_not_vacc)

dat_raw_ae_admit_dec15to20_ctrl76to79_extended <- dat_raw_dec15to20_ctrl76to79_extended %>%
  select(ReportDelay, Match_Count, VaccToCOVID_vacc = VaccToAE_Admit_vacc, VaccToCOVID_not_vacc = VaccToAE_Admit_not_vacc,
         COVID_in_time_vacc = AE_Admit_in_time_vacc, COVID_in_time_not_vacc = AE_Admit_in_time_not_vacc, 
         COVID_1st_recorded_vacc = AE_Admission_Date_vacc, COVID_1st_recorded_not_vacc = AE_Admission_Date_not_vacc)

dat_raw_apc_admit_dec15to20_ctrl76to79_original <- dat_raw_dec15to20_ctrl76to79_original %>%
  ## Exclude LOS greater than 42
  filter((APC_LOS_vacc <= 42 | is.na(APC_LOS_vacc)) , (APC_LOS_not_vacc <= 42 | is.na(APC_LOS_not_vacc))) %>%
  select(ReportDelay, Match_Count, VaccToCOVID_vacc = VaccToAPC_Admit_vacc, VaccToCOVID_not_vacc = VaccToAPC_Admit_not_vacc,
         COVID_in_time_vacc = APC_Admit_in_time_vacc, COVID_in_time_not_vacc = APC_Admit_in_time_not_vacc, 
         COVID_1st_recorded_vacc = APC_Admission_Date_vacc, COVID_1st_recorded_not_vacc = APC_Admission_Date_not_vacc)


# Preparation for function 
# ========================

# Create Report Delay / COVID Delay Reference Data to account for missing records
out_ref <- data.frame()

n <- 46

for (x in 6:40) {
  
n <- n - 1

dat_ref <- data.frame(ReportDelay = x, COVID_Delay = 1:n)

out_ref <- out_ref %>%
  bind_rows(dat_ref)

}

dat_ref <- out_ref %>%
  mutate(Match_Count = 1) %>%
  bind_rows(out_ref %>% mutate(Match_Count = 2)) %>%
  bind_rows(out_ref %>% mutate(Match_Count = 3)) %>%
  bind_rows(out_ref %>% mutate(Match_Count = 4)) %>%
  bind_rows(out_ref %>% mutate(Match_Count = 5))

dat_ref_15to20 <- dat_ref %>%
  mutate(Cohort = "Vaccinated") %>%
  bind_rows(dat_ref %>% mutate(Cohort = "Control"))


# Define Bootstrapping and adjustment function 
# ============================================

fun_boot <- function(x, y) {

# Create blank data frame for output
out_summary <- data.frame()
n_sample <- 0 

# For loop to run samples through whole process
for (i in 1:100) {
  
n_sample <- n_sample + 1

# Summarise total matches
dat_matches <- x %>%
  select(ReportDelay, Match_Count) %>%
  mutate(count = 1) %>%
  group_by(ReportDelay, Match_Count) %>%
  summarise(count = sum(count)) %>%
  ungroup()

# Bootstrap sample data by ReportDelay, MatchCount with replacement
dat_sample <- x %>%
  left_join(dat_matches, by = c("ReportDelay", "Match_Count")) %>%
  group_by(ReportDelay, Match_Count) %>%
  sample_n(size=mean(count), replace = TRUE) %>%
  ungroup()

# Wrangle COVID positive data
dat_covid <- dat_sample %>%
  filter(VaccToCOVID_vacc > 0) %>%
  filter(!is.na(COVID_1st_recorded_vacc) | !is.na(COVID_1st_recorded_not_vacc)) %>%
  select(ReportDelay, Match_Count, VaccToCOVID_vacc, COVID_in_time_vacc)  %>%
  filter(COVID_in_time_vacc == 1) %>%
  group_by(ReportDelay, Match_Count, VaccToCOVID_vacc) %>%
  summarise(COVID_in_time_vacc = sum(COVID_in_time_vacc)) %>%
  ungroup() %>%
  full_join(., dat_sample %>%
              filter(VaccToCOVID_not_vacc > 0) %>%
              filter(!is.na(COVID_1st_recorded_vacc) | !is.na(COVID_1st_recorded_not_vacc)) %>%
              select(ReportDelay, Match_Count, VaccToCOVID_not_vacc, COVID_in_time_not_vacc) %>%
              filter(COVID_in_time_not_vacc == 1) %>%
              group_by(ReportDelay, Match_Count, VaccToCOVID_not_vacc) %>%
              summarise(COVID_in_time_not_vacc = sum(COVID_in_time_not_vacc)) %>%
              ungroup(),
            by = c("ReportDelay", "Match_Count", "VaccToCOVID_vacc" = "VaccToCOVID_not_vacc")) %>%
  arrange(ReportDelay, Match_Count, VaccToCOVID_vacc) %>%
  rename(Vaccinated = COVID_in_time_vacc, Control = COVID_in_time_not_vacc) %>%
  gather(key = Cohort, value = Value, -ReportDelay, -Match_Count, -VaccToCOVID_vacc) %>%
  select(ReportDelay, Match_Count, COVID_Delay = VaccToCOVID_vacc, Cohort, Value) %>%
  left_join(., dat_matches, by = c("ReportDelay", "Match_Count"))

# Calculate rates per 100,000
dat_rates <- dat_covid %>%
  mutate(Rate = (Value / count) * 100000) 

# Apply further corrections with static dataset
dat_rates <- y %>%
  left_join(., dat_rates, by = c("COVID_Delay", "Match_Count", "ReportDelay", "Cohort")) %>%
  # Replace NAs with 0
  mutate(Rate = ifelse(is.na(Rate), 0, Rate))

# Define max ReportDelay
max_rd <- max(dat_rates$ReportDelay)

# Prepare data frame of max ReportDelay
out <- dat_rates %>%
  filter(ReportDelay == max(ReportDelay)) %>%
  mutate(ratio = 1, 
         adjusted = Rate * ratio) 

# Loop to apply adjustments to rates
## For loop is necessary here, as opposed to lapply techniques, as each iteration depends on previous 
## values that are outputted from the for loop 
for (i in (max_rd-1):1) {

n <- i
  
current <- dat_rates %>%
  filter(ReportDelay == n)  %>%
  group_by(ReportDelay, Match_Count) %>%
  mutate(max = max(COVID_Delay))%>%
  ungroup() %>%
  filter(COVID_Delay <= max-1) %>%
  group_by(Match_Count, Cohort) %>%
  summarise(total_current = sum(Rate, na.rm=TRUE)) %>%
  ungroup()

lag <- out %>%
  filter(ReportDelay == n+1) %>%
  group_by(Match_Count, Cohort) %>%
  summarise(total_lag = sum(adjusted, na.rm=TRUE)) %>%
  ungroup()

adj <- lag %>%
  left_join(current, by = c("Match_Count", "Cohort")) %>%
  mutate(ratio = total_lag / total_current,
         ReportDelay = n) %>%
  select(-total_lag, -total_current)

out_adj <- dat_rates %>%
  filter(ReportDelay == n) %>%
  left_join(., adj, by = c("ReportDelay", "Match_Count", "Cohort")) %>%
  mutate(adjusted = Rate * ratio)

out <- out %>%
  bind_rows(out_adj)

}

# Prepare output from adjustment loop
out_sum <- out %>%
  mutate(sample = n_sample) %>%
  filter(!is.na(Cohort)) %>%
  group_by(sample, ReportDelay, Match_Count, Cohort) %>%
  filter(COVID_Delay == max(COVID_Delay)) %>%
  ungroup() %>%
  bind_rows(out %>% filter(ReportDelay == max(ReportDelay)) %>%
              filter(COVID_Delay != max(COVID_Delay)) %>%
              mutate(sample = n_sample)) %>%
  select(sample, ReportDelay, Match_Count, COVID_Delay, Cohort, adjusted) %>%
  spread(key = Cohort, value = adjusted) %>%
  mutate(Difference = Control - Vaccinated,
         `% Difference` = (Vaccinated / Control) - 1) %>%
  select(-ReportDelay) %>%
  gather(key = Measure, value = adjusted, -sample, -Match_Count, -COVID_Delay) %>%
  mutate(adjusted = case_when(adjusted == "NaN" ~ 0,
                              adjusted == "Inf" ~ 0,
                              TRUE ~ adjusted)) %>%
  spread(key = COVID_Delay, value = adjusted, fill = NA)

out_summary <- out_summary %>%
  bind_rows(out_sum) 

}

return(out_summary)

}

# Run data through bootstrapping function
# =======================================

out_positive_dec15to20_ctrl76to79_extended <- fun_boot(x = dat_raw_positive_dec15to20_ctrl76to79_extended, y = dat_ref_15to20)
out_ae_attend_dec15to20_ctrl76to79_extended <- fun_boot(x = dat_raw_ae_attend_dec15to20_ctrl76to79_extended, y = dat_ref_15to20)
out_ae_admit_dec15to20_ctrl76to79_extended <- fun_boot(x = dat_raw_ae_admit_dec15to20_ctrl76to79_extended, y = dat_ref_15to20)
out_apc_admit_dec15to20_ctrl76to79_extended <- fun_boot(x = dat_raw_apc_admit_dec15to20_ctrl76to79_extended, y = dat_ref_15to20)

# Run summary tables functions 
# ============================

## Totals 15to20
fun_sum_totals_15to20 <- function(x) {

sum_totals <- x %>%
  gather(key = COVID_Delay, value = value, -Measure, -sample, -Match_Count) %>%
  filter(Measure %in% c("Vaccinated", "Control")) %>%
  group_by(Measure, COVID_Delay) %>%
  summarise(value = mean(value, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(value = ifelse(value == "NaN", NA, value),
         grouping = case_when(between(COVID_Delay,1,41) ~ "Total (1 to 41)")) %>%
  filter(grouping == "Total (1 to 41)") %>%
  select(-grouping) %>%
  group_by(Measure) %>%
  summarise(`Total (1 to 41)` = sum(value, na.rm=TRUE)) %>%
  ungroup() %>%
  left_join(x %>%
              gather(key = COVID_Delay, value = value, -Measure, -sample, -Match_Count) %>%
              filter(Measure %in% c("Vaccinated", "Control")) %>%
              group_by(Measure, COVID_Delay) %>%
              summarise(value = mean(value, na.rm=TRUE)) %>%
              ungroup() %>%
              mutate(value = ifelse(value == "NaN", NA, value),
                     grouping = case_when(between(COVID_Delay,14,41) ~ "Total (14 to 41)")) %>%
              filter(grouping == "Total (14 to 41)") %>%
              select(-grouping) %>%
              group_by(Measure) %>%
              summarise(`Total (14 to 41)` = sum(value, na.rm=TRUE)) %>%
              ungroup(), 
            by = "Measure") %>%
  left_join(x %>%
              gather(key = COVID_Delay, value = value, -Measure, -sample, -Match_Count) %>%
              filter(Measure %in% c("Vaccinated", "Control")) %>%
              group_by(Measure, COVID_Delay) %>%
              summarise(value = mean(value, na.rm=TRUE)) %>%
              ungroup() %>%
              mutate(value = ifelse(value == "NaN", NA, value),
                     grouping = case_when(between(COVID_Delay,1,45) ~ "Total (1 to 45)")) %>%
              filter(grouping == "Total (1 to 45)") %>%
              select(-grouping) %>%
              group_by(Measure) %>%
              summarise(`Total (1 to 45)` = sum(value, na.rm=TRUE)) %>%
              ungroup(), 
            by = "Measure") %>%
  mutate(Measure = case_when(Measure == "Control" ~ "1. Control",
                             Measure == "Vaccinated" ~ "2. Vaccinated"))
  
}

out_sum_totals_positive_dec15to20_ctrl76to79_extended <- fun_sum_totals_15to20(x = out_positive_dec15to20_ctrl76to79_extended)
out_sum_totals_ae_attend_dec15to20_ctrl76to79_extended <- fun_sum_totals_15to20(x = out_ae_attend_dec15to20_ctrl76to79_extended)
out_sum_totals_ae_admit_dec15to20_ctrl76to79_extended <- fun_sum_totals_15to20(x = out_ae_admit_dec15to20_ctrl76to79_extended)
out_sum_totals_apc_admit_dec15to20_ctrl76to79_extended <- fun_sum_totals_15to20(x = out_apc_admit_dec15to20_ctrl76to79_extended)
  
# 7 Day average means
fun_sum_all <- function(x) {

sum_avg <- x %>%
  gather(key = COVID_Delay, value = value, -Match_Count, -sample, -Measure) %>%
  filter(Measure %in% c("Vaccinated", "Control")) %>%
  group_by(Measure, COVID_Delay) %>%
  summarise(value = mean(value, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay), 
         value = ifelse(value == "NaN", NA, value)) %>%
  arrange(Measure, COVID_Delay) %>%
  group_by(Measure) %>%
  mutate(avg = rollapply(value, width=7, FUN=function(y) mean(y, na.rm=TRUE), fill=NA, align="center")) %>%
  # mutate(avg = rollmean(value, k=7, align="center", na.pad=TRUE)) %>%
  ungroup() %>%
  select(-value) %>%
  spread(key = Measure, value = avg) %>%
  mutate(Difference = Control - Vaccinated) %>%
  gather(key = Measure, value = value, -COVID_Delay) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay),
         Measure = case_when(Measure == "Control" ~ "1. Control",
                             Measure == "Vaccinated" ~ "2. Vaccinated",
                             Measure == "Difference" ~ "3. Difference",
                             Measure == "Difference (%)" ~ "4. Difference (%)")) %>%
  spread(key = COVID_Delay, value = value) 
  
sum_perc <- x %>%
  gather(key = COVID_Delay, value = value, -Match_Count, -sample, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
  filter(Measure %in% c("Vaccinated", "Control", "Difference")) %>%
  arrange(Measure, COVID_Delay) %>%
  group_by(Match_Count, sample, Measure) %>%
  mutate(avg = rollapply(value, width=7, FUN=function(y) mean(y, na.rm=TRUE), fill=NA, align="center")) %>%
  #mutate(avg = rollmean(value, k=7, align="center", na.pad=TRUE)) %>%
  ungroup() %>%
  select(-value) %>%
  spread(key = Measure, value = avg) %>%
  mutate(`% Difference` = (Vaccinated / Control) - 1) %>%
  gather(key = Measure, value = avg, -COVID_Delay, -Match_Count, -sample) %>%
  group_by(COVID_Delay, Measure) %>%
  summarise(`a. 95% Conf (Upper)` = quantile(avg, 0.95, na.rm=TRUE),
            `b. 95% Conf (Lower)` = quantile(avg, 0.05, na.rm=TRUE)) %>%
  ungroup() %>%
  gather(key = perc, value = value, -COVID_Delay, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay),
         Measure = case_when(Measure == "Control" ~ "1",
                             Measure == "Vaccinated" ~ "2",
                             Measure == "Difference" ~ "3",
                             Measure == "% Difference" ~ "4")) %>%
  mutate(Measure = paste0(Measure, perc)) %>%
  select(-perc) %>%
  spread(key = COVID_Delay, value = value) %>%
  filter(Measure != "% Difference")

sum_perc_all <- sum_avg %>%
  bind_rows(sum_perc) %>%
  arrange(Measure) %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  spread(key = Measure, value = value) %>%
  mutate(`4. Difference (%)` = (`2. Vaccinated` / `1. Control`) - 1) %>%
  gather(key = Measure, value = value, -COVID_Delay) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
  spread(key = COVID_Delay, value = value)

return(sum_perc_all)

}

out_sum_all_positive_dec15to20_ctrl76to79_extended <- fun_sum_all(x = out_positive_dec15to20_ctrl76to79_extended)
out_sum_all_ae_attend_dec15to20_ctrl76to79_extended <- fun_sum_all(x = out_ae_attend_dec15to20_ctrl76to79_extended)
out_sum_all_ae_admit_dec15to20_ctrl76to79_extended <- fun_sum_all(x = out_ae_admit_dec15to20_ctrl76to79_extended)
out_sum_all_apc_admit_dec15to20_ctrl76to79_extended <- fun_sum_all(x = out_apc_admit_dec15to20_ctrl76to79_extended)

# Binned Totals
fun_sum_bin <- function(x) {

sum_bin_all <- x %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  filter(COVID_Delay %in% c("17", "24", "31", "38")) %>%
  mutate(bin = case_when(COVID_Delay == "17" ~ "14 to 20",
                         COVID_Delay == "24" ~ "21 to 27",
                         COVID_Delay == "31" ~ "28 to 24",
                         COVID_Delay == "38" ~ "35 to 41")) %>%
  select(-COVID_Delay) %>%
  spread(key = bin, value = value)

return(sum_bin_all)

}

out_sum_bin_positive_dec15to20_ctrl76to79_extended <- fun_sum_bin(x = out_sum_all_positive_dec15to20_ctrl76to79_extended)
out_sum_bin_ae_attend_dec15to20_ctrl76to79_extended <- fun_sum_bin(x = out_sum_all_ae_attend_dec15to20_ctrl76to79_extended)
out_sum_bin_ae_admit_dec15to20_ctrl76to79_extended <- fun_sum_bin(x = out_sum_all_ae_admit_dec15to20_ctrl76to79_extended)
out_sum_bin_apc_admit_dec15to20_ctrl76to79_extended <- fun_sum_bin(x = out_sum_all_apc_admit_dec15to20_ctrl76to79_extended)

# Cumulative Totals
fun_cum_all <-  function(x) {

sum_cum_all <-  x %>%
  gather(key = COVID_Delay, value = value, -Match_Count, -sample, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
  filter(Measure %in% c("Vaccinated", "Control")) %>%
  group_by(Measure, COVID_Delay) %>%
  summarise(value = mean(value, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(value = ifelse(value == "NaN", NA, value)) %>%
  group_by(Measure) %>%
  mutate(cumulative = cumsum(value)) %>%
  ungroup() %>%
  select(-value) %>%
  spread(key = COVID_Delay, value = cumulative) %>%
  mutate(Measure = case_when(Measure == "Control" ~ "1. Control",
                             Measure == "Vaccinated" ~ "2. Vaccinated")) %>%
  bind_rows(x %>%
              gather(key = COVID_Delay, value = value, -Match_Count, -sample, -Measure) %>%
              mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
              filter(Measure %in% c("Vaccinated", "Control")) %>%
              group_by(sample, Match_Count, Measure, COVID_Delay) %>%
              summarise(value = mean(value, na.rm=TRUE)) %>%
              ungroup() %>%
              mutate(value = ifelse(value == "NaN", NA, value)) %>%
              group_by(sample, Match_Count, Measure) %>%
              mutate(cumulative = cumsum(value)) %>%
              group_by(COVID_Delay, Measure) %>%
              summarise(`a. 95% Conf (Upper)` = quantile(cumulative, 0.95, na.rm=TRUE),
                        `b. 95% Conf (Lower)` = quantile(cumulative, 0.05, na.rm=TRUE)) %>%
              ungroup() %>%
              gather(key = perc, value = value, -COVID_Delay, -Measure) %>%
              mutate(COVID_Delay = as.numeric(COVID_Delay),
                     Measure = case_when(Measure == "Control" ~ "1",
                                         Measure == "Vaccinated" ~ "2")) %>%
              mutate(Measure = paste0(Measure, perc)) %>%
              select(Measure, COVID_Delay, value) %>%
              spread(key = COVID_Delay, value = value)) %>%
  arrange(Measure)

return(sum_cum_all)

}  

out_cum_all_positive_dec15to20_ctrl76to79_extended <- fun_cum_all(x = out_positive_dec15to20_ctrl76to79_extended)
out_cum_all_ae_attend_dec15to20_ctrl76to79_extended <- fun_cum_all(x = out_ae_attend_dec15to20_ctrl76to79_extended)
out_cum_all_ae_admit_dec15to20_ctrl76to79_extended <- fun_cum_all(x = out_ae_admit_dec15to20_ctrl76to79_extended)
out_cum_all_apc_admit_dec15to20_ctrl76to79_extended <- fun_cum_all(x = out_apc_admit_dec15to20_ctrl76to79_extended)


# Output results to workbook and extract
# ======================================

fun_write <- function(x, y, out1, out2, out3, out4) {

# add sheets
addWorksheet(wb, y)

# Join Totals and bin Tables  
out_bin <- out2 %>%
  left_join(out3, by = "Measure")

# Write data
writeData(wb, y, out_bin, startCol = 4, startRow = 2, rowNames = FALSE)
writeData(wb, y, out1, startCol = 4, startRow = 16, rowNames = FALSE)
writeData(wb, y, x, startCol = 2, startRow = 30, rowNames = FALSE)

## Generate and insert graphs

dat_graph_1 <- out1 %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
  filter(Measure %in% c("1. Control", "1b. 95% Conf (Lower)", "1a. 95% Conf (Upper)", "2. Vaccinated", "2b. 95% Conf (Lower)", "2a. 95% Conf (Upper)"))

graph_1 <- ggplot() + 
  geom_line(dat_graph_1 %>% filter(Measure == "1. Control"), mapping = aes(x = COVID_Delay, y = value, color = Measure), size = 1.5) + 
  geom_line(dat_graph_1 %>% filter(Measure == "1b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_1 %>% filter(Measure == "1a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_1 %>% filter(Measure == "2. Vaccinated"), mapping = aes(x = COVID_Delay, y = value, color = Measure), size = 1.5) + 
  geom_line(dat_graph_1 %>% filter(Measure == "2b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_1 %>% filter(Measure == "2a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  theme_minimal() + 
  scale_x_continuous(breaks = scales::pretty_breaks(n = 20)) +
  scale_y_continuous(breaks = scales::pretty_breaks(n = 20)) + 
  scale_color_manual(values = c("1. Control" = "#ED8B00", "1b. 95% Conf (Lower)" = "#ED8B00", "1a. 95% Conf (Upper)" = "#ED8B00",
                                "2. Vaccinated" = "#005EB8", "2b. 95% Conf (Lower)" = "#005EB8", "2a. 95% Conf (Upper)" = "#005EB8")) + 
  guides(color = guide_legend(override.aes = list(linetype = c("solid", "dotted", "dotted", "solid", "dotted", "dotted")))) +
  labs(color = "",  x = "", y = "")

ggsave(filename = paste0("Graphs/graph_1_",y,".png"), graph_1, width = 8, height = 4, dpi = 300, units = "in", device = "png")
insertImage(wb, y, paste0("Graphs/graph_1_",y,".png"), width=8, height=4, units="in", startRow = 3, startCol = 53, dpi = 300)


dat_graph_2 <- out1 %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay)) %>%
  filter(Measure %in% c("4. Difference (%)", "4a. 95% Conf (Upper)", "4b. 95% Conf (Lower)"))

graph_2 <- ggplot() + 
  geom_line(dat_graph_2 %>% filter(Measure == "4. Difference (%)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), size = 1.5) + 
  geom_line(dat_graph_2 %>% filter(Measure == "4a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) + 
  geom_line(dat_graph_2 %>% filter(Measure == "4b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) + 
  theme_minimal() + 
  scale_x_continuous(breaks = scales::pretty_breaks(n = 20)) +
  scale_y_continuous(breaks = scales::pretty_breaks(n = 20)) +
  scale_color_manual(values = c("4. Difference (%)" = "#330072", "4a. 95% Conf (Upper)" = "#AE2573", "4b. 95% Conf (Lower)" = "#AE2573")) + 
  guides(color = guide_legend(override.aes = list(linetype = c("solid", "dotted", "dotted")))) +
  labs(color = "",  x = "", y = "")

ggsave(filename = paste0("Graphs/graph_2_",y,".png"), graph_2, width = 8, height = 4, dpi = 300, units = "in", device = "png")
insertImage(wb, y, paste0("Graphs/graph_2_",y,".png"), width=8, height=4, units="in", startRow = 26, startCol = 53, dpi = 300)


dat_graph_3 <- out2 %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  filter(Measure %in% c("4. Difference (%)", "4a. 95% Conf (Upper)", "4b. 95% Conf (Lower)"))

graph_3 <- ggplot() + 
  geom_line(dat_graph_3 %>% filter(Measure == "4. Difference (%)"), mapping = aes(x = COVID_Delay, y = value, group = Measure, color = Measure), size = 1.5) +
  geom_line(dat_graph_3 %>% filter(Measure == "4a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, group = Measure, color = Measure), linetype = "dotted", size = 1) + 
  geom_line(dat_graph_3 %>% filter(Measure == "4b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, group = Measure, color = Measure), linetype = "dotted", size = 1) +
  scale_color_manual(values = c("4. Difference (%)" = "#330072", "4a. 95% Conf (Upper)" = "#AE2573", "4b. 95% Conf (Lower)" = "#AE2573")) + 
  guides(color = guide_legend(override.aes = list(linetype = c("solid", "dotted", "dotted")))) +
  theme_minimal() + 
  scale_y_continuous(breaks = scales::pretty_breaks(n = 20)) +
  labs(color = "",  x = "", y = "")

ggsave(filename = paste0("Graphs/graph_3_",y,".png"), graph_3, width = 8, height = 4, dpi = 300, units = "in", device = "png")
insertImage(wb, y, paste0("Graphs/graph_3_",y,".png"), width=8, height=4, units="in", startRow = 46, startCol = 53, dpi = 300)


dat_graph_4 <- out4 %>%
  gather(key = COVID_Delay, value = value, -Measure) %>%
  mutate(COVID_Delay = as.numeric(COVID_Delay))

graph_4 <- ggplot() + 
  geom_line(dat_graph_4 %>% filter(Measure == "1. Control"), mapping = aes(x = COVID_Delay, y = value, color = Measure), size = 1.5) + 
  geom_line(dat_graph_4 %>% filter(Measure == "1b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_4 %>% filter(Measure == "1a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_4 %>% filter(Measure == "2. Vaccinated"), mapping = aes(x = COVID_Delay, y = value, color = Measure), size = 1.5) + 
  geom_line(dat_graph_4 %>% filter(Measure == "2b. 95% Conf (Lower)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  geom_line(dat_graph_4 %>% filter(Measure == "2a. 95% Conf (Upper)"), mapping = aes(x = COVID_Delay, y = value, color = Measure), linetype = "dotted", size = 1) +
  theme_minimal() + 
  scale_x_continuous(breaks = scales::pretty_breaks(n = 20)) +
  scale_y_continuous(breaks = scales::pretty_breaks(n = 20)) + 
  scale_color_manual(values = c("1. Control" = "#ED8B00", "1b. 95% Conf (Lower)" = "#ED8B00", "1a. 95% Conf (Upper)" = "#ED8B00",
                                "2. Vaccinated" = "#005EB8", "2b. 95% Conf (Lower)" = "#005EB8", "2a. 95% Conf (Upper)" = "#005EB8")) + 
  guides(color = guide_legend(override.aes = list(linetype = c("solid", "dotted", "dotted", "solid", "dotted", "dotted")))) +
  labs(color = "",  x = "", y = "")

ggsave(filename = paste0("Graphs/graph_4_",y,".png"), graph_4, width = 8, height = 4, dpi = 300, units = "in", device = "png")
insertImage(wb, y, paste0("Graphs/graph_4_",y,".png"), width=8, height=4, units="in", startRow = 66, startCol = 53, dpi = 300)

}

wb <- createWorkbook()

fun_write(x = out_positive_dec15to20_ctrl76to79_extended, y = "positive_dec15to20_ctrl76to79", out1 = out_sum_all_positive_dec15to20_ctrl76to79_extended, out2 = out_sum_bin_positive_dec15to20_ctrl76to79_extended, out3 = out_sum_totals_positive_dec15to20_ctrl76to79_extended, out4 = out_cum_all_positive_dec15to20_ctrl76to79_extended)
fun_write(x = out_ae_attend_dec15to20_ctrl76to79_extended, y = "ae_attend_dec15to20_ctrl76to79", out1 = out_sum_all_ae_attend_dec15to20_ctrl76to79_extended, out2 = out_sum_bin_ae_attend_dec15to20_ctrl76to79_extended, out3 = out_sum_totals_ae_attend_dec15to20_ctrl76to79_extended, out4 = out_cum_all_ae_attend_dec15to20_ctrl76to79_extended)
fun_write(x = out_ae_admit_dec15to20_ctrl76to79_extended, y = "ae_admit_dec15to20_ctrl76to79", out1 = out_sum_all_ae_admit_dec15to20_ctrl76to79_extended, out2 = out_sum_bin_ae_admit_dec15to20_ctrl76to79_extended, out3 = out_sum_totals_ae_admit_dec15to20_ctrl76to79_extended, out4 = out_cum_all_ae_admit_dec15to20_ctrl76to79_extended)
fun_write(x = out_apc_admit_dec15to20_ctrl76to79_extended, y = "apc_admit_dec15to20_ctrl76to79", out1 = out_sum_all_apc_admit_dec15to20_ctrl76to79_extended, out2 = out_sum_bin_apc_admit_dec15to20_ctrl76to79_extended, out3 = out_sum_totals_apc_admit_dec15to20_ctrl76to79_extended, out4 = out_cum_all_apc_admit_dec15to20_ctrl76to79_extended)

file_name <- paste0("Outputs/Vaccinations_Data_Bootstrap_Extended_", format(Sys.time(), "%Y%m%d"), ".xlsx")
saveWorkbook(wb, file_name, overwrite = TRUE)


# Output extracts
# ===============

extract_samples <- bind_rows(out_positive_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "positive_dec15to20_ctrl76to79_extended"),
                             out_ae_attend_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_attend_dec15to20_ctrl76to79_extended"),
                             out_ae_admit_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_admit_dec15to20_ctrl76to79_extended")) %>%
  gather(key = COVID_Delay, value = value, -dataset, -sample, -Match_Count, -Measure)

file_name <- paste0("Extracts/Extract_Samples", format(Sys.time(), "%Y%m%d"), ".RDS")
saveRDS(extract_samples, file_name) 


extract_sum_all <- bind_rows(out_sum_all_positive_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "positive_dec15to20_ctrl76to79_extended"),
                             out_sum_all_ae_attend_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_attend_dec15to20_ctrl76to79_extended"),
                             out_sum_all_ae_admit_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_admit_dec15to20_ctrl76to79_extended"))  %>%
  gather(key = COVID_Delay, value = value, -dataset, -Measure)

file_name <- paste0("Extracts/Extract_Sum_All", format(Sys.time(), "%Y%m%d"), ".RDS")
saveRDS(extract_sum_all, file_name) 


extract_cum_all <- bind_rows(out_cum_all_positive_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "positive_dec15to20_ctrl76to79_extended"),
                             out_cum_all_ae_attend_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_attend_dec15to20_ctrl76to79_extended"),
                             out_cum_all_ae_admit_dec15to20_ctrl76to79_extended %>%
                               mutate(dataset = "ae_admit_dec15to20_ctrl76to79_extended")) %>%
  gather(key = COVID_Delay, value = value, -dataset, -Measure)

file_name <- paste0("Extracts/Extract_Cum_All", format(Sys.time(), "%Y%m%d"), ".RDS")
saveRDS(extract_cum_all, file_name) 

