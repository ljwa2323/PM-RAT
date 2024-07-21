setwd("/home/luojiawei/pengxiran_project/")

library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)

source("/home/luojiawei/pengxiran_project/EMR_LIP.R")

medications <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/medications.csv",header=T)

med_dict <- read_excel("./var_dict.xlsx", sheet = 4, col_names = T)

medications[1:4,]
med_dict[1:4,]
uniq_med <- unique(med_dict$name4)

old_dn <- medications$drug_name
new_dn <- rep(NA, length(old_dn))

for(i in 1:length(uniq_med)){
    # i<-1
    tar_dn <- med_dict$name2[med_dict$name4==uniq_med[i]]
    index <- which(old_dn %in% tar_dn)
    new_dn[index] <- uniq_med[i]
    print(i)
}

# 将 new_dn 作为新列添加到 medications 数据表中
medications$drug_name1 <- new_dn

medications[1:4,]

fwrite(medications, file="/home/luojiawei/pengxiran_project/derived_table/medications_derived.csv", row.names=F)



