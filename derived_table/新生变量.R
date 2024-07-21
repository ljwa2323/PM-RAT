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
library(tidyr)

source("/home/luojiawei/pengxiran_project/EMR_LIP.R")

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)

as.data.frame(operations_derived[1:4,])
vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals.csv",
                    header=T)

vitals[1:4,]
tar_item <- c("dopai","dobui","epii","mlni","nepi")
vitals_ <- vitals[which(vitals$item_name %in% tar_item),]
dim(vitals_)

vitals_[1:40,]

library(dplyr)

# 假设vitals_是已经按照要求筛选过的数据框
# 首先将所有的value转换为数值型，以便进行计算
vitals_$value <- as.numeric(as.character(vitals_$value))

# 使用dplyr处理数据
vis_data <- vitals_ %>%
  group_by(op_id, chart_time, item_name) %>%
  summarise(value = mean(value), .groups = 'drop') %>%
  spread(key = item_name, value = value, fill = 0) %>%
  mutate(VIS = dopai + dobui + 100*epii + 10*mlni + 100*nepi) %>%
  select(op_id, chart_time, VIS)

# 查看结果
print(vis_data)

# 为vis_data添加subject_id列，这里假设每个op_id对应唯一的subject_id
# 首先，从vitals_中提取op_id和subject_id的唯一对应关系
op_subject_mapping <- unique(vitals_[, .(op_id, subject_id)])

# 然后，将vis_data与op_subject_mapping按op_id合并，以获取subject_id
vis_data <- merge(vis_data, op_subject_mapping, by = "op_id")

# 为vis_data添加item_name列，所有行的item_name都是"VIS"
vis_data$item_name <- "VIS"

# 调整vis_data的列顺序和列名，以匹配vitals_的格式
vis_data <- vis_data %>%
  select(op_id, subject_id, chart_time, item_name, VIS) %>%
  rename(value = VIS)

# 将vis_data合并到vitals_中
vitals_combined <- rbind(vitals, vis_data)

vitals_combined[1:4,]

# 查看合并后的结果
print(head(vitals_combined))



vitals[1:4,]
tar_item <- c("d10w","d50w","d5w","hns","hs","ns")
vitals_ <- vitals[which(vitals$item_name %in% tar_item),]
dim(vitals_)

vitals_[1:10,]

# 假设vitals_是已经按照要求筛选过的数据框
# 首先将所有的value转换为数值型，以便进行计算
vitals_$value <- as.numeric(as.character(vitals_$value))

# 使用dplyr处理数据
tci_data <- vitals_ %>%
  group_by(op_id, chart_time, item_name) %>%
  summarise(value = mean(value), .groups = 'drop') %>%
  spread(key = item_name, value = value, fill = 0) %>%
  mutate(TCI = d10w + d50w + d5w + hns + hs + ns) %>%
  select(op_id, chart_time, TCI)

# 查看结果
print(tci_data[1:5,])

# 为tci_data添加subject_id列，这里假设每个op_id对应唯一的subject_id
# 首先，从vitals_combined中提取op_id和subject_id的唯一对应关系
op_subject_mapping <- unique(vitals_[, .(op_id, subject_id)])

# 然后，将tci_data与op_subject_mapping按op_id合并，以获取subject_id
tci_data <- merge(tci_data, op_subject_mapping, by = "op_id", all.x = TRUE)

# 为tci_data添加item_name列，所有行的item_name都是"TCI"
tci_data$item_name <- "TCI"

# 调整tci_data的列顺序和列名，以匹配vitals_combined的格式
tci_data <- tci_data %>%
  select(op_id, subject_id, chart_time, item_name, TCI) %>%
  rename(value = TCI)

# 将tci_data合并到vitals_combined中
vitals_combined <- rbind(vitals_combined, tci_data)

# 查看合并后的结果
print(head(vitals_combined))




vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals_derived.csv")
vitals$value[vitals$item_name == "alb20" & vitals$value==1] <- 50
vitals <- vitals[-which(vitals$item_name == "alb"),]

vitals[1:4,]
tar_item <- c("alb20","alb5")
vitals_ <- vitals[which(vitals$item_name %in% tar_item),]

dim(vitals_)
table(vitals_$value[vitals_$item_name=="alb20"])

vitals_[1:10,]

# 假设vitals_是已经按照要求筛选过的数据框
# 首先将所有的value转换为数值型，以便进行计算
vitals_$value <- as.numeric(as.character(vitals_$value))

# 使用 pivot_wider() 替代 spread()
alb_data <- vitals_ %>%
  group_by(op_id, chart_time, item_name) %>%
  summarise(value = mean(value), .groups = 'drop') %>%
  pivot_wider(names_from = item_name, values_from = value, values_fill = list(value = 0)) %>%
  mutate(alb = 0.2*alb20 + 0.05*alb5) %>%
  select(op_id, chart_time, alb)

# 查看结果
print(alb_data[1:5,])

# 为alb_data添加subject_id列，这里假设每个op_id对应唯一的subject_id
# 首先，从vitals_combined中提取op_id和subject_id的唯一对应关系
op_subject_mapping <- unique(vitals_[, .(op_id, subject_id)])

# 然后，将alb_data与op_subject_mapping按op_id合并，以获取subject_id
alb_data <- merge(alb_data, op_subject_mapping, by = "op_id", all.x = TRUE)

# 为alb_data添加item_name列，所有行的item_name都是"TCI"
alb_data$item_name <- "alb"

# 调整alb_data的列顺序和列名，以匹配vitals_combined的格式
alb_data <- alb_data %>%
  select(op_id, subject_id, chart_time, item_name, alb) %>%
  rename(value = alb)

# 将alb_data合并到vitals_combined中
vitals_combined <- rbind(vitals, alb_data)
# vitals_combined <- rbind(vitals_combined, alb_data)

# 查看合并后的结果
print(head(vitals_combined))
dim(vitals_combined)
table(vitals_combined$value[vitals_combined$item_name == "alb20"])




vitals[1:4,]
tar_item <- c("sft","ftn","aft","rfti")
vitals_ <- vitals[which(vitals$item_name %in% tar_item),]
dim(vitals_)

vitals_[1:10,]

# 假设vitals_是已经按照要求筛选过的数据框
# 首先将所有的value转换为数值型，以便进行计算
vitals_$value <- as.numeric(as.character(vitals_$value))

mme_data <- vitals_ %>%
  group_by(op_id, chart_time, item_name) %>%
  summarise(value = mean(value), .groups = 'drop') %>%
  spread(key = item_name, value = value, fill = 0)

mme_data[1:5,]
operations_derived[1:5,]

# 使用merge函数根据op_id合并mme_data和operations_derived的相关列
mme_data <- merge(mme_data, operations_derived[, .(op_id, weight, an_duration)], by = "op_id", all.x = TRUE)

# 查看合并后的结果
print(head(mme_data))
summary(mme_data)
mme_data$weight[is.na(mme_data$weight)] <- median(mme_data$weight, na.rm=T)
mme_data$an_duration[is.na(mme_data$an_duration)] <- median(mme_data$an_duration, na.rm=T)


# 使用dplyr处理数据
mme_data <- mme_data %>%
  mutate(MME=sft*1000+ftn*200+aft*15+rfti*weight*70*an_duration/1000) %>%
  select(op_id, chart_time, MME)

# 查看结果
print(mme_data[1:5,])

# 为mme_data添加subject_id列，这里假设每个op_id对应唯一的subject_id
# 首先，从vitals_combined中提取op_id和subject_id的唯一对应关系
op_subject_mapping <- unique(vitals_[, .(op_id, subject_id)])

# 然后，将mme_data与op_subject_mapping按op_id合并，以获取subject_id
mme_data <- merge(mme_data, op_subject_mapping, by = "op_id", all.x = TRUE)

# 为mme_data添加item_name列，所有行的item_name都是"TCI"
mme_data$item_name <- "MME"

# 调整mme_data的列顺序和列名，以匹配vitals_combined的格式
mme_data <- mme_data %>%
  select(op_id, subject_id, chart_time, item_name, MME) %>%
  rename(value = MME)

mme_data[1:4,]

# 将mme_data合并到vitals_combined中
vitals_combined <- rbind(vitals_combined, mme_data)

# 查看合并后的结果
print(head(vitals_combined))


fwrite(vitals_combined, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals_derived.csv",row.names=F)

