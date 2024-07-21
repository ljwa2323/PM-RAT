
# rm(list=ls());gc()


#  新增变量 BMI, 麻醉持续时间，手术持续时间，手术室持续时间，ICU持续时间，CPB持续时间

setwd("/home/luojiawei/pengxiran_project/")

library(data.table)
library(dplyr)
library(magrittr)
# library(mice)
# library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)

operation <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations.csv",
                    header=T)

dim(operation)

operation[1:2,]

# 计算BMI
operation$bmi <- operation$weight / (operation$height/100)^2


# 手术持续时间
operation$los <- operation$discharge_time - operation$admission_time

# 手术持续时间
operation$op_duration <- operation$opend_time - operation$opstart_time

# 在手术室的持续时间
operation$or_duration <- operation$orout_time - operation$orin_time

# 麻醉持续时间
operation$an_duration <- operation$anend_time - operation$anstart_time

# cpb持续时间
operation$cpb_duration <- operation$cpboff_time - operation$cpbon_time

# icu持续时间
operation$icu_duration <- operation$icuout_time - operation$icuin_time

operation$sex <- ifelse(operation$sex == "F", 0, 1)
# names(operation)
operation$op_duration[is.na(operation$op_duration)] <- 0
operation$or_duration[is.na(operation$or_duration)] <- 0
operation$an_duration[is.na(operation$an_duration)] <- 0
operation$icu_duration[is.na(operation$icu_duration)] <- 0
operation$death_30d <- ifelse(is.na(operation$inhosp_death_time), 0, ifelse((operation$inhosp_death_time - operation$opend_time) <= 30 * 24 * 60, 1, 0))

operation[1:3,]

NAME <- names(operation)

# 创建查找表
pcs_type_lookup <- data.frame(
  pcs_type = c("08", "09", "0C", "0H", "0J", "0K", "0T", "0U", "0V", "10", "0D", "0F", "00", "0G", "0B", "0P", "0Q", "0N", "0S", "0R", "0L", "0M", "02", "03", "04", "05", "06", "07", "0W", "0Y", "0X", "01", "0E", "0A"),
  type_name = c("Head_and_Neck", "Head_and_Neck", "Head_and_Neck", "Skin_and_Soft_Tissue", "Skin_and_Soft_Tissue", "Skin_and_Soft_Tissue", "Urinary_and_Reproductive_Systems", "Urinary_and_Reproductive_Systems", "Urinary_and_Reproductive_Systems", "Urinary_and_Reproductive_Systems", "Gastrointestinal_System", "Hepatobiliary_System_and_Pancreas", "Central_Nervous_System_and_Cranial_Nerves", "Endocrine_System", "Respiratory_System", "Bones_and_Joints", "Bones_and_Joints", "Bones_and_Joints", "Bones_and_Joints", "Bones_and_Joints", "Bones_and_Joints", "Bones_and_Joints", "Heart_and_Great_Vessels", "Peripheral_Vascular_System", "Peripheral_Vascular_System", "Peripheral_Vascular_System", "Peripheral_Vascular_System", "Lymphatic_and_Hemic_Systems", "Other", "Other", "Other", "Other", "Other", "Other")
)

# 提取前两位字符并生成新列
operation$pcs_type <- substr(operation$icd10_pcs, 1, 2)

# 合并查找表
operation <- merge(operation, pcs_type_lookup, by = "pcs_type", all.x = TRUE)
operation <- as.data.frame(operation)
operation <- operation[,c(NAME,"type_name")]
# 查看结果
head(operation)


fwrite(operation, "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv",
        row.names=F)


# operation <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv")

# operation[1:2,]


