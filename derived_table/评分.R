
setwd("/home/luojiawei/pengxiran_project/")

# rm(list=ls());gc()

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

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)

as.data.frame(operations_derived[1:2,])

diag <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/diagnosis.csv",header=T)

diag$chart_time <- diag$chart_time / 60

as.data.frame(diag[1:3,])

# 创建一个函数来检查ICD10编码是否在指定范围内
check_icd10 <- function(icd10_code) {
  # 检查是否匹配C00-D49或Z85
  if (str_detect(icd10_code, "^C[0-9]+|^D[0-4][0-9]") | str_detect(icd10_code, "^Z85")) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

# 为operations_derived添加一个新列，初始值为0
operations_derived[, icd10_match := 0]

# 遍历每个op_id
for (i in 1:nrow(operations_derived)) {#nrow(operations_derived)
  # 获取当前行的subject_id和orin_time
  current_subject_id <- operations_derived[i, subject_id]
  current_orin_time <- operations_derived[i, orin_time]
  
  # 获取该subject_id对应的所有icd10_cm编码及其chart_time
  subject_icd10_codes <- diag[subject_id == current_subject_id, .(icd10_cm, chart_time)]
  
  # 筛选出chart_time在orin_time之前的icd10_cm编码
  valid_icd10_codes <- subject_icd10_codes[chart_time < current_orin_time, icd10_cm]
  
  # 检查是否有任何编码匹配C00-D49或Z85
  if (any(sapply(valid_icd10_codes, check_icd10))) {
    # 如果有匹配的，将icd10_match设置为1
    operations_derived[i, icd10_match := 1]
  }
  if (i %% 1000 == 0) print(i)
}

# 查看更新后的operations_derived
print(operations_derived[1:10,])

table(operations_derived$icd10_match)

##  ===============================  sort 评分 ===============================
as.data.frame(operations_derived[1:3,])
table(operations_derived$emop)

# 使用 data.table 的方式重新计算 sort 评分
operations_derived[, sort_score := {
  # 1. ASA 分数
  asa_score <- fifelse(asa == 3, 1.411, fifelse(asa == 4, 2.388, fifelse(asa == 5, 4.081, 0)))
  
  # 2. EMOP 分数
  emop_score <- fifelse(emop == 1, 1.657, 0)
  
  # 3. 手术类型分数
  surgery_type_score <- fifelse(substr(icd10_pcs, 1, 2) %in% c("0D", "0F", "0G", "0W", "0X", "0Y", "02"), 0.712, 0)
  
  # 4. 手术时间分数
  surgery_duration_score <- fifelse(op_duration > 120, 0.381, 0)
  
  # 5. ICD10 匹配分数
  icd10_match_score <- fifelse(icd10_match == 1, 0.667, 0)
  
  # 6. 年龄分数
  age_score <- fifelse(age >= 65 & age <= 79, 0.777, fifelse(age >= 80, 1.591, 0))
  
  # 总分
  asa_score + emop_score + surgery_type_score + surgery_duration_score + icd10_match_score + age_score
}, by = .(op_id)]

# 查看更新后的数据
print(operations_derived[, .(op_id, subject_id, sort_score)])

as.data.frame(operations_derived[1:60,])

operations_derived[which(is.na(operations_derived$sort_score))[1:10],]

dim(operations_derived)

#  ==============================  CCI 评分 ===========================

diag[1:10,]

# 1 分的疾病编码
score_1_codes <- c("I21", "I50", "I73", "F01", "I63", "M35", "K25", "K26", "K27", "E11", "J44", "K70")
# 2 分的疾病编码
score_2_codes <- c("I69", "N18")
score_2_codes <- c(score_2_codes, 
                    paste0("C",str_pad(0:99, 2, "left", "0")),
                    paste0("D",str_pad(0:49, 2, "left", "0")))
# 3 分的疾病编码
score_3_codes <- c("K74", "K75", "K76")
# 6 分的疾病编码
score_6_codes <- c("C77", "C78", "C79", "C80", "B20", "B21", "B22", "B23", "B24")

diag$score <- rep(0, nrow(diag))
diag$score[diag$icd10_cm %in% score_1_codes] <- 1
diag$score[diag$icd10_cm %in% score_2_codes] <- 2
diag$score[diag$icd10_cm %in% score_3_codes] <- 3
diag$score[diag$icd10_cm %in% score_6_codes] <- 6

as.data.frame(operations_derived[1:2,])

# 确保 diag 和 operations_derived 是 data.table 类型
setDT(diag)
setDT(operations_derived)

# 遍历 operations_derived 中的每一行
operations_derived[, CCI_score := {
  # 获取当前行的 subject_id 和 orin_time
  current_subject_id <- subject_id
  current_orin_time <- orin_time
  current_admission_time <- admission_time

  subject_scores <- diag[subject_id == current_subject_id & chart_time < current_orin_time, .(icd10_cm, score)]
  unique_subject_scores <- unique(subject_scores, by = "icd10_cm")
  
  # 对选出的得分求和
  sum(unique_subject_scores$score)
}, by = .(op_id)]

# 查看更新后的数据
# print(operations_derived[, .(op_id, subject_id, CCI_score)])

diag[1:4,]

as.data.frame(operations_derived[1:3,])

table(operations_derived$CCI_score)

prop.table(table(ifelse(operations_derived$CCI_score >=3, 1, 0)))

diag[1:40,]

names(operations_derived)

fwrite(operations_derived[,c(1,2,3,38,39,40),drop=F], file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv",row.names=F)


scores <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv")
scores[1:4,]
# hist(scores$CCI_score)


operations_derived[,c(1,2,3,36,37,38),drop=F][1:100,]


library(pROC)
roc(operations_derived$death_30d~operations_derived$asa)
roc(operations_derived$death_30d~operations_derived$CCI_score)
roc(operations_derived$death_30d~operations_derived$sort_score)

# ============================  

# 缺血性心脏病_病史	I20-I25	1分
# 充血性心力衰竭病史	I50	1分
# 卒中病史_选择/短暂脑缺血发作病史时间/颅内出血_选择	I60-I63或者G45	1分
# 糖尿病_控制方式	E11	1分
# 肌酐	labs(creatinine>2mg/dL)	1分
# 手术风险评估_手术部位	icd10_pcs	( 腹部手术:0D,0F,0G,0T,0U,0V,10, 胸腔手术:0B,颅脑手术:00,心脏大血管手术:02,上血管手术:03,05) 为1分

diag <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/diagnosis.csv",header=T)
diag$chart_time <- diag$chart_time / 60
as.data.frame(diag[1:3,])

labs <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs.csv",header=T)
labs_ <- labs[which(labs$item_name %in% c("creatinine")),]
dim(labs_)
labs_[1:4,]

operations_derived[1:3,]

# 首先筛选出需要的疾病编码
relevant_icd10_codes <- c("I20", "I21", "I22", "I23", "I24", "I25", "I50", "I60", "I61", "I62", "I63", "G45", "E11")
diag_relevant <- diag[icd10_cm %in% relevant_icd10_codes]
dim(diag_relevant)


# 筛选出肌酐大于2mg/dL的记录
labs_relevant <- labs_[item_name == "creatinine" & value > 2]
dim(labs_relevant)

# 手术部位对应的icd10_pcs编码
surgery_icd10_pcs_codes <- c("0D", "0F", "0G", "0T", "0U", "0V", "10", "0B", "00", "02", "03", "05")

# 遍历operations_derived的每一行，计算总分
operations_derived[, total_score := {
  current_subject_id <- subject_id
  current_orin_time <- orin_time
  current_admission_time <- admission_time
  
  # 获取当前subject_id对应的所有疾病编码及其chart_time，并去重
  subject_scores <- diag_relevant[subject_id == current_subject_id & chart_time <= current_orin_time, .(icd10_cm)]
  unique_subject_scores <- unique(subject_scores, by = "icd10_cm")
  
  # 计算疾病历史得分
  disease_score <- nrow(unique_subject_scores)
  
  # 计算肌酐得分
  # creatinine_score <- ifelse(any(labs_relevant[subject_id == current_subject_id & chart_time >= current_admission_time & chart_time <= current_orin_time]), 1, 0)
  creatinine_score <- as.integer(sum(labs_relevant[subject_id == current_subject_id & chart_time >= current_admission_time & chart_time <= current_orin_time, .N] > 0))


  # 计算手术部位得分
  surgery_score <- ifelse(substr(icd10_pcs, 1, 2) %in% surgery_icd10_pcs_codes, 1, 0)
  
  # 总分
  sum(c(disease_score, creatinine_score, surgery_score),na.rm=T)
}, by = .(op_id)]

# 查看更新后的数据
# print(operations_derived[, .(op_id, subject_id, total_score)])
operations_derived[1:4,]
# library(pROC)
# plot(roc(operations_derived$death_30d ~ operations_derived$total_score))

scores[1:4,]

scores <- cbind(scores, "RCRI" = operations_derived$total_score)

fwrite(scores, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv",row.names=F)

