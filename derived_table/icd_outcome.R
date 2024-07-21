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

diag <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/diagnosis.csv",header=T)
diag$chart_time <- diag$chart_time / 60
diag[1:3,]

operation[1:3,]

# 首先，确保operation和diag都是data.frame类型
operation <- as.data.frame(operation)
diag <- as.data.frame(diag)

# 定义各种并发症对应的ICD-10编码范围
complications <- list(
    I_POC = c("T81"),
    I_IPCD_RS = c("J95"),
    I_IPCD_NS = c("G97"),
    I_IPCD_DS = c("K91"),
    I_IPCD_CS = c("I97"),
    I_IPCD_MSS = c("M96"),
    I_IPCD_GUS = c("N99"),
    I_IPCD_SKIN = c("L76"),
    I_IPCD_EYE = c("H59"),
    I_IPCD_EAR = c("H95"),
    I_EMCD = c("E89"),
    I_Hypotension = c("I95"),
    I_SAH = c("I60"),
    I_ICH = c("I60", "I61", "I62"),
    I_CI = c("I63"),
    I_AP = c("I20"),
    I_AMI = c("I21"),
    I_AIHD = c("I20", "I24"),
    I_CIHD = c("I25"),
    I_AF = c("I44"),
    I_CA = c("I46"),
    I_PT = c("I47"),
    I_Arrhythmia = c("I44", "I48"),
    I_PE = c("I26"),
    I_VTT = c("I80"),
    I_PVT = c("I81"),
    I_TE = c("I63", "I21", "I26", "I80", "I81", "I82"),
    I_Fever = c("R50"),
    I_Pain = c("G89"),
    I_PONV = c("R11")
)

# 定义一个函数，用于检查特定的ICD-10编码是否属于某个并发症类别
check_complication <- function(icd_code) {

  # 检查ICD编码是否在任何并发症类别中
  for (complication in names(complications)) {
    if (icd_code %in% complications[[complication]]) {
      return(complication)
    }
  }
  return(NA)
}

check_complication("K91")


# 创建一个新的数据框，用于存储每个op_id的并发症信息
complications_df <- data.frame(op_id = operation$op_id)

# 对于每种并发症，添加一列，初始值设为0
for (complication in names(check_complication(""))) {
  complications_df[[complication]] <- 0
}

# 遍历每个手术记录
for (i in 1:nrow(operation)) {#
  # 获取当前手术的信息
  # i<-1
  current_op <- operation[i, ]
  
  # 找到所有与当前手术相关的诊断记录
  related_diags <- diag[which(diag$subject_id == current_op$subject_id & diag$chart_time > current_op$orin_time & diag$chart_time < current_op$discharge_time), ]

  # 如果related_diags不为空，则检查每个相关诊断是否对应于某个并发症
  if (nrow(related_diags) > 0) {
    for (j in 1:nrow(related_diags)) {
      # j<-1
      complication <- check_complication(related_diags$icd10_cm[j])
      if (!is.na(complication)) {
        complications_df[i, complication] <- 1
      }
    }
  } else {
    # 如果没有相关的诊断记录，可以在这里执行其他操作，例如记录日志等
    # 例如: print(paste("No related diagnoses for op_id:", current_op$op_id))
  }
  if (i %% 2000 == 0) {
    print(i)
  }
}

# 查看结果
print(complications_df[1:4,])

for(i in 2:ncol(complications_df)){
  complications_df[[i]] <- ifelse(is.na(complications_df[[i]]), 0, complications_df[[i]])
}


round(apply(complications_df[,2:ncol(complications_df)], 2, mean),6)

# 查看结果
fwrite(complications_df, file = "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/icd_outcome.csv", row.names = FALSE)
# write.csv(complications_df, file = "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/icd_outcome.csv", row.names = FALSE)


complications_df <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/icd_outcome.csv",header=T)

dim(complications_df)
complications_df[1:5,]
names(complications_df)