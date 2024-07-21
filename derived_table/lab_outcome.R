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

source("/home/luojiawei/pengxiran_project/EMR_LIP.R")

operation <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv",
                    header=T)
operation <- as.data.frame(operation)

labs <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs.csv",header=T)

unique(labs$item_name)

labs_ <- labs[which(labs$item_name %in% c("troponin_t")),]
labs_$value <- labs_$value * 1000
dim(labs_)

labs_[1:4,]

operation[1:4,]

# 初始化一个列表来暂时存储结果
results_list <- list()

for (i in 1:nrow(operation)) {
    operation_row <- operation[i,]
    lab_k <- labs_[which(labs_$subject_id == operation_row$subject_id),]
    v1 <- get_first(lab_k$value[which(lab_k$chart_time <= operation_row$opstart_time)])
    v2 <- get_last(lab_k$value[which(lab_k$chart_time >= operation_row$opend_time)])
    
    # 初始化标记为NA，适用于缺失数据的情况
    C_PMI <- NA
    C_MINS <- NA
    
    # 检查v1和v2是否都不是NA
    if (!is.na(v1) && !is.na(v2)) {
        delta <- v2 - v1
        # 计算C-PMI标记
        C_PMI <- delta > 14
        # 计算C-MINS标记，第二种方法只依赖于v2
        C_MINS <- (v2 >= 20 && delta >= 5) || v2 >= 65
    } else if (!is.na(v2)) {
        # 如果v1是NA，但v2不是NA，只根据v2来判断C-MINS的第二种情况
        C_MINS <- v2 >= 65
    }
    
    # 将结果存储在列表中
    results_list[[i]] <- list(op_id = operation_row$op_id, C_PMI = C_PMI, C_MINS = C_MINS)

    if (i %% 1000 == 0) {
        print(i)
    }
}


# 如果需要，可以将结果列表转换为数据框
results_df <- do.call(rbind, lapply(results_list, as.data.frame))

results_df[1:4,]

round(apply(results_df[,2:3], 2, function(x) sum(x, na.rm=T)), 6)

fwrite(results_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_1.csv",row.names=F)





ward_vital_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vital_outcome.csv",header=T)
ward_vital_outcome[1:4,]
labs_ <- labs[which(labs$item_name %in% c("creatinine")),]
labs_ <- labs_[!duplicated(labs_[,c("subject_id","chart_time")], ), ]
labs_ <- labs_[order(labs_$subject_id, labs_$chart_time, decreasing = F), ]

dim(labs_)
# range(labs_$value)

labs_[1:20,]

operation[1:4,]

ward_vital_outcome[1:4,]


# 初始化一个列表来暂时存储结果
results_list <- list()

for (i in 1:nrow(operation)) { #nrow(operation)
    # i <- 1
    operation_row <- operation[i,]
    # 确保选取的是creatinine
    lab_k <- labs_[which(labs_$subject_id == operation_row$subject_id),]
    crrt <- ward_vital_outcome$crrt[i]
    # t1 <- get_last(lab_k$chart_time[which(lab_k$chart_time <= operation_row$opstart_time)])
    t1 <- operation_row$opend_time
    v1 <- get_last(lab_k$value[which(lab_k$chart_time <= operation_row$opstart_time)])
    t2 <- get_first(lab_k$chart_time[which(lab_k$chart_time >= operation_row$opend_time)])
    v2 <- get_first(lab_k$value[which(lab_k$chart_time >= operation_row$opend_time)])
    
    # 初始化AKI为NA，适用于缺失数据的情况
    AKI <- NA
    
    # 检查v1和v2是否都不是NA，并且时间在规定范围内
    if (!is.na(v1) && !is.na(v2)) {
        ratio <- v2 / v1
        increase <- v2 - v1
        time_diff_hours <- (t2 - t1) / 60  # 将时间差转换为小时
        
        # 根据规则计算AKI等级
        if (v2 >= 4 || crrt == 1) {
            AKI <- 3
        } else if ((ratio >= 3) && time_diff_hours <= 168) {  # 7天内
            AKI <- 3
        } else if (ratio >= 2 && ratio < 3 && time_diff_hours <= 168) {  # 7天内
            AKI <- 2
        } else if ((ratio >= 1.5 && ratio < 2 && time_diff_hours <= 168) || (increase >= 0.3 && time_diff_hours <= 48)) {  # 7天或48小时内
            AKI <- 1
        } else {
            AKI <- 0
        }
    }
    
    # 将结果存储在列表中
    # results_list[[i]] <- list(op_id = operation_row$op_id, AKI = AKI)
    results_list[[i]] <- list(op_id = operation_row$op_id, AKI = AKI, t1 = t1, t2 = t2, v1 = v1, v2 = v2)
    if (i %% 1000 == 0) {
        print(i)
    }
}
# 如果需要，可以将结果列表转换为数据框
results_df <- do.call(rbind, lapply(results_list, as.data.frame))

results_df[1:20,]

ward_vital_outcome[1:4,]

results_df <- merge(results_df, ward_vital_outcome[,c(1,3,4,5)], by='op_id', all.x=T)

str(results_df)
results_df[results_df$AKI>=1 & !is.na(results_df$AKI), ][1:10,]



summary(results_df$v1)
summary(results_df$v2)

table(results_df$AKI)
prop.table(table(results_df$AKI))

fwrite(results_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_2.csv",row.names=F)
fwrite(results_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/AKI情况.csv",row.names=F)









labs_ <- labs[which(labs$item_name %in% c("alt")),]
dim(labs_)
# range(labs_$value)
quantile(labs_$value, probs=c(0.99))

labs_[1:4,]

operation[1:4,]

# 初始化一个列表来暂时存储结果
results_list <- list()

for (i in 1:nrow(operation)) {
    operation_row <- operation[i,]
    # 确保选取的是alt
    lab_k <- labs_[which(labs_$subject_id == operation_row$subject_id & labs_$item_name == "alt"),]
    death <- operation_row$death_30d

    # 根据性别定义正常上限值
    normal_upper_limit <- ifelse(operation_row$sex == 0, 35, 40)  # 假设性别字段：0为女性，1为男性

    v1 <- get_last(lab_k$value[which(lab_k$chart_time <= operation_row$opstart_time)])
    v2 <- get_first(lab_k$value[which(lab_k$chart_time >= operation_row$opend_time)])
    
    # 初始化ALI等级为NA，适用于缺失数据的情况
    ALI <- NA
    
    # 检查v1和v2是否都不是NA
    if (!is.na(v1) && !is.na(v2)) {
        ratio <- v2 / normal_upper_limit
        
        # 根据规则计算ALI等级
        if (death == 1) {
            ALI <- "5"
        } else if (ratio > 20) {
            ALI <- "4"
        } else if (ratio > 5 && ratio <= 20) {
            ALI <- "3"
        } else if (ratio > 3 && ratio <= 5) {
            ALI <- "2"
        } else if (ratio > 2 && ratio <= 3) {
            ALI <- "1"
        } else {
            ALI <- "0" # 未达到任何ALI等级的标准
        }
    }
    
    # 将结果存储在列表中
    results_list[[i]] <- list(op_id = operation_row$op_id, ALI = ALI)
    
    if (i %% 1000 == 0) {
        print(i)
    }
}

# 如果需要，可以将结果列表转换为数据框
results_df <- do.call(rbind, lapply(results_list, as.data.frame))

results_df[1:4,]

table(results_df$ALI)

fwrite(results_df, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/lab_outcome_3.csv",row.names=F)
