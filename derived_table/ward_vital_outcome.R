setwd("/home/luojiawei/pengxiran_project/")

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

ward_vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals.csv",header=T)

ward_vitals[ward_vitals$item_name=="spo2" & ward_vitals$value <= 90,][1:10,]
range(ward_vitals$value[ward_vitals$item_name=="spo2"],na.rm=T)

operation[operation$death_30d==1,][1:2,]

wv_k <- as.data.frame(ward_vitals[which(ward_vitals$subject_id %in% operation$subject_id[operation$death_30d==1] & ward_vitals$item_name == "hr"),])
range(wv_k$value)

ward_vitals <- ward_vitals[which(ward_vitals$item_name %in% c("crrt","ecmo", "vent")),]

ward_vitals[1:1000,]
dim(ward_vitals)
unique(ward_vitals$item_name)


ward_vitals[ward_vitals$item_name=="vent",]


operation[1:5,]

operation_row <- operation[1,]
subject_vit <- ward_vitals[which(ward_vitals$subject_id == operation_row$subject_id & ward_vitals$chart_time > operation_row$opend_time & ward_vitals$chart_time < operation_row$discharge_time), ]
as.data.frame(subject_vit[1:10,])

# 初始化一个列表来暂时存储结果
results_list <- list()
which(operation$subject_id == 188297542)
# 遍历operation的每一行
for (i in 1:nrow(operation)) {#nrow(operation)
  # i <- 88564
  operation_row <- operation[i,]
  subject_vit <- ward_vitals[which(ward_vitals$subject_id == operation_row$subject_id & 
                                   ward_vitals$chart_time > operation_row$opend_time & 
                                   ward_vitals$chart_time < operation_row$discharge_time), ]
  if (nrow(subject_vit) == 0) {
    crrt_flag <- 0
    ecmo_flag <- 0
    vent_flag <- 0
    vent_time <- 0
  } else {
    crrt_flag <- ifelse(any(subject_vit$item_name == "crrt"), 1, 0)
    ecmo_flag <- ifelse(any(subject_vit$item_name == "ecmo"), 1, 0)
    vent_flag <- ifelse(any(subject_vit$item_name == "vent"), 1, 0)
    index <- which(subject_vit$item_name == "vent")
    if(length(index) > 0){
      subject_vit_ <- subject_vit[index,]
      subject_vit_ <- subject_vit_[order(subject_vit_$chart_time, decreasing = F),]
      ind <- which(subject_vit_$value == 1)
      ind <- setdiff(ind, nrow(subject_vit_))
      vent_time <- sum(diff(subject_vit_$chart_time)[ind])
    }
  }
  
  # 将每行的结果作为一个新的data.frame添加到列表中
  results_list[[i]] <- data.frame(op_id=operation_row$op_id, 
                                  subject_id=operation_row$subject_id, 
                                  crrt=crrt_flag, 
                                  ecmo=ecmo_flag, 
                                  vent=vent_flag,
                                  vent_time=vent_time)
  if (i %% 1000 == 0) {
    print(i)
  }
}

# 将列表转换为data.frame
results_df <- do.call(rbind, results_list)

# 查看结果
print(results_df[1:10,])

quantile(results_df$vent_time, prob=c(0.25,0.5,0.75,0.9,0.95,0.97,0.99),na.rm=T)
results_df[results_df$vent_time > 1000,]
table(ifelse(results_df$vent_time>24*60, 1, 0))

results_df$vent_time_24h <- ifelse(results_df$vent_time>24*60, 1, 0)

round(apply(results_df[,3:5], 2, mean), 6)

cor(results_df[,3:5])

fwrite(results_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vital_outcome.csv", row.names=F)



# ward_vital_outcome <- results_df
# names(ward_vital_outcome)

# med_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/med_outcome.csv",header=T)

# med_outcome[1:4,]

# icd_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/icd_outcome.csv",header=T)

# icd_outcome[1:4,]
# operation[1:4,]
# operation$sex <- ifelse(operation$sex == "F", 0, 1)
# names(operation)
# operation$op_duration[is.na(operation$op_duration)] <- 0
# operation$or_duration[is.na(operation$or_duration)] <- 0
# operation$an_duration[is.na(operation$an_duration)] <- 0
# operation$icu_duration[is.na(operation$icu_duration)] <- 0
# operation$death_30d <- ifelse(is.na(operation$inhosp_death_time), 0, ifelse((operation$inhosp_death_time - operation$opend_time) <= 30 * 24 * 60, 1, 0))

# operation[is.na(operation$death_30d),]

# outcomes_df <- merge(operation[,c(1,2,3,6,7,8,9,11,29,30:35)], ward_vital_outcome[,c(1,3,4,5)], by="op_id", all.x=T)

# names(med_outcome)
# outcomes_df <- merge(outcomes_df, med_outcome[,c(1,5:9)], by="op_id", all.x=T)
# names(icd_outcome)
# outcomes_df <- merge(outcomes_df, icd_outcome[,c(1,2:24)], by="op_id", all.x=T)






