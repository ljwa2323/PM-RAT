

setwd("/home/luojiawei/pengxiran_project/")

# rm(list=ls());gc()

library(data.table)
library(dplyr)
library(magrittr)
# library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)

operation <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv",
                    header=T)
operation <- as.data.frame(operation)

med_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/med_outcome.csv",header=T)
med_outcome[1:4,]

icd_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/icd_outcome.csv",header=T)

# icd_outcome[1:4,]
# apply(icd_outcome[,4:ncol(icd_outcome)],2,sum)

ward_vital_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vital_outcome.csv",header=T)
ward_vital_outcome[1:4,]

lab_outcome_1 <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_1.csv",header=T)
lab_outcome_2 <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_2.csv",header=T)
lab_outcome_3 <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_3.csv",header=T)

lab_outcome_1[1:4,]
lab_outcome_2[1:4,]
lab_outcome_3[1:4,]
lab_outcome <- cbind(lab_outcome_1,lab_outcome_2[,2,drop=F], lab_outcome_3[,2,drop=F])

lab_outcome[1:5,]
# table(lab_outcome$ALI)

names(operation)
names(ward_vital_outcome)
outcomes_df <- merge(operation[,c(1,2,3,30:36)], ward_vital_outcome[,c(1,3:5,7)], by="op_id", all.x=T)
names(med_outcome)
outcomes_df <- merge(outcomes_df, med_outcome[,c(1,5:9)], by="op_id", all.x=T)
names(icd_outcome)
outcomes_df <- merge(outcomes_df, icd_outcome[,c(1,2:26)], by="op_id", all.x=T)
names(lab_outcome)
outcomes_df <- merge(outcomes_df, lab_outcome, by="op_id", all.x=T)

outcomes_df[1:4,]
dim(outcomes_df)

fwrite(outcomes_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/static_outcomes.csv",row.names=F)
