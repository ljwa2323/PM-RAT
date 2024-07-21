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

operation[1:2,]

pre_diag <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前diag.csv")
pre_diag[1:2,]

ward_vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals.csv",header=T)
ward_vitals[1:4,]

ward_vitals_vent <- ward_vitals[which(ward_vitals$item_name=="vent"),]

# 初始化一个列表来暂时存储结果
results_list <- list()
# which(operation$subject_id == 188297542)
# 遍历operation的每一行
for (i in 1:nrow(operation)) {#nrow(operation)
  # i <- 88564
  operation_row <- operation[i,]
  subject_vit <- ward_vitals_vent[which(ward_vitals_vent$subject_id == operation_row$subject_id & 
                                   ward_vitals_vent$chart_time < operation_row$orin_time & 
                                   ward_vitals_vent$chart_time > operation_row$admission_time), ]
  if (nrow(subject_vit) == 0) {
    vent_flag <- 0
    vent_time <- 0
  } else {
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
                                  vent=vent_flag,
                                  vent_time=vent_time)
  if (i %% 1000 == 0) {
    print(i)
  }
}

# 将列表转换为data.frame
pre_vent <- do.call(rbind, results_list)

pre_vent[1:4,]
range(pre_vent$vent_time)
table(pre_vent$vent)

operation[1:4,]

names(operation)

names(operation)[c(1,2,3,6:9,11,12,15)]



pre_diag[1:4,]

# 定义一个函数来检查和标记每个条件
check_conditions <- function(pre_diag) {
  # 创建一个空的data.frame来存储结果
  results <- data.frame(op_id = unique(pre_diag$op_id))
  
  # 定义条件和对应的ICD编码
  conditions <- list(
    Steroid_Use = "Z80",
    Ascites = "R19",
    Systemic_Sepsis = "A42",
    Disseminated_Cancer = "C81",
    Diabetes = c("E10", "E11", "E12", "E13", "E14", "E15"),
    Hypertension = "I10",
    Congestive_Heart_Failure = "I50",
    Dyspnea = "R07",
    Severe_COPD = "J45",
    Acute_Renal_Failure = "N18"
  )
  
  # 遍历每个条件，检查并标记
  for (condition_name in names(conditions)) {
    icd_codes <- conditions[[condition_name]]
    # 为每个 op_id 聚合结果
    condition_presence <- aggregate(icd10_cm ~ op_id, pre_diag, function(x) any(x %in% icd_codes))
    # 将结果合并到 results 数据框中
    results <- merge(results, condition_presence, by = "op_id", all.x = TRUE)
    # 将逻辑值转换为整数
    results[[condition_name]] <- as.integer(results$icd10_cm)
    results$icd10_cm <- NULL  # 移除辅助列
  }
  
  return(results)
}

# 应用函数并查看结果
condition_results <- check_conditions(pre_diag)
head(condition_results)

# apply(condition_results[,2:ncol(condition_results)], 2, mean)


names(operation)[c(1,2,3,6:9,11,12,15)]
ds_nsqip <- operation[,c(1,2,3,6:9,11,12,15)]
ds_nsqip <- merge(ds_nsqip, condition_results, by="op_id", all.x=T)
ds_nsqip[1:4,]
pre_vent[1:4,]
ds_nsqip <- merge(ds_nsqip, pre_vent[,c(1,3),drop=F], by="op_id", all.x=T)
ds_nsqip[1:4,]

write.csv(ds_nsqip, 
          "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ds_nsqip.csv", 
          row.names=FALSE, 
          fileEncoding="gb2312")



ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv")
ds_id[1:4,]


dim(ds_id)
dim(ds_nsqip)
names(ds_id)[c(1,4,5,11,54,48:52)]
ds_nsqip <- merge(ds_id[,c(1,4,5,11,54,48:52)], ds_nsqip, by="op_id", all.x=T)
ds_nsqip[1:4,]
dim(ds_nsqip)
names(ds_nsqip)
# Start of Selection
# 选出这些列的值全为0，或者NA的，以及set列==2的，从这些行中随机选择50行
selected_rows <- ds_nsqip %>%
  filter((rowSums(is.na(ds_nsqip[, 3:10])) == ncol(ds_nsqip[, 3:10]) | rowSums(ds_nsqip[, 3:10] == 0, na.rm = TRUE) == ncol(ds_nsqip[, 3:10])) | ds_nsqip$set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/对照50人.csv", row.names=F,fileEncoding="gb2312")



# 选出 death_30d 列为1， set列==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(death_30d == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/death_30d_50人.csv", row.names=F,fileEncoding="gb2312")


# 随机选出 los==1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(los == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/los_50人.csv", row.names=F,fileEncoding="gb2312")


# 随机选出 AKI_2 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(AKI_2 == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/AKI_50人.csv", row.names=F,fileEncoding="gb2312")


# 随机选出 ALI 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(ALI_2 == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/ALI_50人.csv", row.names=F,fileEncoding="gb2312")



# 随机选出 cardiac_comp 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(cardiac_comp == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/cardiac_comp_50人.csv", row.names=F,fileEncoding="gb2312")


# 随机选出 cardiac_comp_stroke 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(cardiac_comp_stroke == 1 & set == 2) %>%
  sample_n(50)
# End of Selection

write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/cardiac_comp_stroke_50人.csv", row.names=F,fileEncoding="gb2312")

# 随机选出 stroke 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(stroke == 1 & set == 2) %>%
  sample_n(50)
write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/stroke_50人.csv", row.names=F,fileEncoding="gb2312")

# 随机选出 icu_duration_12h 为1，set==2 的随机50行
selected_rows <- ds_nsqip %>%
  filter(icu_duration_12h == 1 & set == 2) %>%
  sample_n(50)
write.csv(selected_rows, "/home/luojiawei/pengxiran_project_data/icu_duration_12h_50人.csv", row.names=F,fileEncoding="gb2312")


# ======================================

ds_nsqip <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ds_nsqip.csv")
ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv")
ds_id[1:4,]

dim(ds_id)
dim(ds_nsqip)
names(ds_id)[c(1,4,5,11,50:54)]
ds_nsqip <- merge(ds_id[,c(1,4,5,11,50:54)], ds_nsqip, by="op_id", all.x=T)
ds_nsqip[1:4,]

ds_nsqip_select <- read_excel("/home/luojiawei/pengxiran_project/结果文件夹/NSQIP255表.xlsx",
                sheet=1, col_names = T) %>% as.data.frame

names(ds_nsqip_select)

ds_nsqip_select <- ds_nsqip_select %>% distinct(op_id, .keep_all = TRUE)
ds_nsqip <- ds_nsqip[which(ds_nsqip$op_id %in% ds_nsqip_select$op_id), ]

ds_nsqip <- merge(ds_nsqip_select[,"op_id",drop=F], ds_nsqip, by="op_id",all=T)
# 使用 order 和 match 函数手动排序
ds_nsqip <- ds_nsqip[order(match(ds_nsqip$op_id, ds_nsqip_select$op_id)), ]
dim(ds_nsqip)
ds_nsqip[1:2,]
as.data.frame(ds_nsqip_select[1:2,])

names(ds_nsqip)[c(5:7,19:29)]
names(ds_nsqip_select)[c(7:9,25:35)]

all(ds_nsqip[which(ds_nsqip$op_id==477076536),c(5:7,19:29)] == 
ds_nsqip_select[ds_nsqip_select$op_id == 477076536,c(7:9,25:35)])

# 以行为单位比较两个数据集指定列的相似性，并记录结果
comparison_vector <- apply(ds_nsqip, 1, function(row1) {
  row2 <- ds_nsqip_select[,c(7:9,25:35)][which(ds_nsqip_select$op_id == row1['op_id']),]
  if (nrow(row2) > 0) {
    as.integer(!all(row1[c(5:7,19:29)] == row2))
  } else {
    1  # 如果没有匹配的行，默认为不相同
  }
})

length(comparison_vector)
ds_nsqip$need_change <- comparison_vector

# 对于不一样的行，用 ds_nsqip 的对应行的列来覆盖 ds_nsqip_select 中对应行的列
for (i in 1:nrow(ds_nsqip_select)) {
  # i <- 1
  op_id <- ds_nsqip_select$op_id[i]
  row1 <- ds_nsqip[ds_nsqip$op_id == op_id, c(5:7,19:29)]
  row2 <- ds_nsqip_select[i, c(7:9,25:35)]
  
  if (nrow(row1) > 0 && !all(row1 == row2, na.rm = TRUE)) {
    ds_nsqip_select[i, c(7:9,25:35)] <- row1
  }
}

# 清理列名中的特殊字符
names(ds_nsqip_select) <- gsub("(\\.)|(\\n)|(\\r)", "_", names(ds_nsqip_select))
names(ds_nsqip_select) <- gsub(" ", "_", names(ds_nsqip_select))

names(ds_nsqip_select)

ds_nsqip_select <- merge(ds_nsqip_select, ds_nsqip[,c("op_id","need_change")], by="op_id", all.x=T)

library(writexl)
write_xlsx(ds_nsqip_select, 
            path = "/home/luojiawei/pengxiran_project_data/nsqip_241.xlsx",
            col_names=T)


ds_nsqip <- readxl::read_xlsx("/home/luojiawei/pengxiran_project_data/nsqip_241.xlsx", sheet=1, col_names=T)
ds_nsqip[1:4,]

write.csv(ds_nsqip[,"op_id",drop=F], file="/home/luojiawei/pengxiran_project_data/ds_id_nsqip.csv", row.names=F)