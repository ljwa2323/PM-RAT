
setwd("/home/luojiawei/pengxiran_project/")

# rm(list=ls());gc()

library(tableone)
library(tibble)
library(data.table)
library(dplyr)

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)
vital <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals_derived.csv")
vital[1:4,]

names(operations_derived)
operations_derived[1:4,c(1,18,19)]

vital_comb <- merge(vital, operations_derived[,c(1,16,17)], by=c("op_id"), all.x=T)

unique(vital$item_name)

vital_comb[vital_comb$item_name == "etsevo" & vital_comb$chart_time - vital_comb$orin_time < 0,][1:30,]
# dim(vital_comb[vital_comb$item_name == "ppf" & vital_comb$chart_time - vital_comb$orin_time < 0,])



