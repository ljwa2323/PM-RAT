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
library(pROC)

source("/home/luojiawei/pengxiran_project/EMR_LIP.R")

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)
dim(operations_derived)
as.data.frame(operations_derived[1:2,])
# range(operations_derived$admission_time)
# as.data.frame(operations_derived[operations_derived$admission_time > 0,][1:4,])

scores <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv")
dim(scores)
scores[1:5,]

operations_derived <- merge(operations_derived, scores[,c(1,5:7)],by="op_id",all.x=T)
names(operations_derived)

ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv")
ds_id[1:3,]
names(ds_id)
operations_derived <- merge(operations_derived[,-30], ds_id[,c(1,5,12,13,15,50:56)],by="op_id",all.x=T)



names(operations_derived)
# 样本筛选
operations_derived <- operations_derived %>%
  filter(age >= 16, 
         op_duration >= 15, 
         !(antype %in% c("Neuraxial", "MAC", "Regional")))

operations_derived[1:5,]

names(operations_derived)

# asa
# sort_score
# CCI_score
# RCRI
ds <- data.frame(y = operations_derived$icu_duration_12h, x=operations_derived$asa, id=operations_derived$op_id)
ds <- na.omit(ds)
m1 <- glm(y ~ x, data=ds, family=binomial())
pred <- predict(m1, ds,type="response")
ds_out <- data.frame(y_true = ds$y, y_pred_prob_0 = pred, id=ds$id)
roc(y_true~y_pred_prob_0, data=na.omit(ds_out))
# 将预测结果添加到测试数据集

fwrite(ds_out, file="/home/luojiawei/pengxiran_project/结果文件夹/模型性能/icu_duration_12h_asa.csv", row.names=F)


