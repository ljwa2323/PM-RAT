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

# operation[operation$op_id == 426754753, ]
# diag[diag$subject_id == 163380841,]

diag <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/diagnosis.csv",header=T)
names(diag)
diag[1:3,]

# 首先确保 operation 和 diag 都是 data.frame 类型
operation <- as.data.frame(operation)
diag <- as.data.frame(diag)
diag$chart_time <- diag$chart_time / 60
diag[1:10,]

# 将 operation 中的 orin_time 转换为数值型，以便比较
operation$orin_time <- as.numeric(as.character(operation$orin_time))

# 创建一个空列表用于存储结果
result_list <- list()

# 遍历 operation 表格中的每一个 op_id
for (i in 1:nrow(operation)) { # nrow(operation)
#   i<-1
  current_op_id <- operation$op_id[i]
  current_subject_id <- operation$subject_id[i]
  current_orin_time <- operation$orin_time[i]
  
  # 通过 subject_id 在 diag 表格中找到对应的记录
  subject_diags <- diag[diag$subject_id == current_subject_id, ]
  
  # 筛选出 chart_time < orin_time 的记录
  preop_diags <- subject_diags[subject_diags$chart_time < current_orin_time, c(1, 3), drop=F]
  
  # 去重
  preop_diags <- unique(preop_diags)
    
  # 如果有结果，添加到列表中
  if (nrow(preop_diags) > 0) {
    preop_diags$op_id <- rep(current_op_id, nrow(preop_diags))
    result_list[[length(result_list) + 1]] <- preop_diags
  }
  if(i %% 1000 == 0) print(i)
}

# 使用 do.call 和 rbind 一次性合并所有结果
result <- do.call(rbind, result_list)

# 查看结果
result[1:10,]

fwrite(result[,c(3,2),drop=F], file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前diag.csv",
                row.names=F)


ds <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前diag.csv")

ds[1:10,]

# 计算每个 op_id 的唯一数量
unique_op_id_count <- length(unique(ds$op_id))

# 计算每个 icd10_cm 编码的出现次数
icd10_cm_counts <- table(ds$icd10_cm)

# 计算发生率
icd10_cm_rates <- icd10_cm_counts / unique_op_id_count

# 将结果转换为 data.frame 并查看
icd10_cm_rates_df <- as.data.frame(icd10_cm_rates)
colnames(icd10_cm_rates_df) <- c("icd10_cm", "rate")
icd10_cm_rates_df[1:10,]
dim(icd10_cm_rates_df)

write.csv(icd10_cm_rates_df, file="/home/luojiawei/pengxiran_project/结果文件夹/icd统计.csv",
                row.names=F)



ds <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前diag.csv")

ds[1:4,]

operation[1:4,]


# 定义疾病和对应的ICD-10编码范围
diseases <- list(
  Essential_Hypertension = "I10",
  Coronary_Heart_Disease = paste0("I",str_pad(20:25, 2, "left", "0")),
  Congestive_Heart_Failure = "I50",
  Atrial_Fibrillation_and_Flutter = "I48",
  Abnormalities_of_Heart_Beat = "R00",
  Diabetes_Mellitus = paste0("E",str_pad(10:14, 2, "left", "0")),
  Cerebral_Infarction = "I63",
  Transient_Cerebral_Ischemic_Attacks = "G45",
  Emphysema_or_COPD = paste0("J",43:44),
  Asthma = "J45",
  Acute_Upper_Respiratory_Infections = paste0("J",str_pad(0:6, 2, "left", "0")),
  Acute_Lower_Respiratory_Infections = paste0("J",str_pad(9:22, 2, "left", "0")),
  Abnormalities_of_Breathing = "R06",
  Malignant_Neoplasms = paste0("C",str_pad(0:97, 2, "left", "0")),
  In_Situ_Neoplasms = paste0("D",str_pad(0:9, 2, "left", "0")),
  Benign_Neoplasms = paste0("D",str_pad(10:36, 2, "left", "0")),
  Neoplasms_of_Uncertain_Behavior = paste0("D",str_pad(37:48, 2, "left", "0")),
  Chronic_Kidney_Disease = "N18",
  Chronic_Viral_Hepatitis = "B18",
  Liver_Disease = paste0("K",str_pad(70:77, 2, "left", "0")),
  Gastro_Esophageal_Reflux_Disease = "K21",
  Anemia = paste0("D",str_pad(50:64, 2, "left", "0")),
  Disorder_of_Thyroid = paste0("E",str_pad(0:7, 2, "left", "0"))
)

# 首先，我们需要将疾病的ICD-10编码范围转换为正则表达式，以便进行匹配
diseases_regex <- lapply(diseases, function(code) {
  if (is.character(code)) {
    paste0("^", code)
  } else {
    paste0("^(", paste(code, collapse="|"), ")")
  }
})

# 初始化operation表中的每种疾病列为0
for (disease in names(diseases)) {
  operation[[disease]] <- 0
}

# 遍历operation表格中的每个 op_id
for (i in 1:nrow(operation)) {
  # i <- 1
  current_op_id <- operation$op_id[i]
  
  # 获取当前op_id对应的所有诊断代码
  current_diags <- ds[ds$op_id == current_op_id, icd10_cm]
  
  # 检查每种疾病是否存在
  for (disease in names(diseases)) {
    # disease <- names(diseases)[1]
    # 使用正则表达式匹配ICD-10编码
    if (any(grepl(diseases_regex[[disease]], current_diags))) {
      operation[i, disease] <- 1
    }
  }

  if(i %% 1000 == 0) print(i)
}

# 查看operation表格的前几行，确认疾病标记列已正确添加
operation[1:4,]
names(operation)
cbind(apply(operation[,c(38:60)], 2, sum))
dim(operation)

names(operation)
fwrite(operation[,c(1:3,38:60)], file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前病史.csv",row.names=F)




