
setwd("/home/luojiawei/pengxiran_project/")

# rm(list=ls());gc()

library(tableone)
library(tibble)
library(data.table)
library(dplyr)

ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
ds_id <- ds_id[,1:4,drop=F]

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)

operations_derived[1:4,]

ds_id <- merge(ds_id, operations_derived, by=c("op_id","subject_id", "hadm_id"), all.x=T)

scores <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv")

ds_id <- merge(ds_id, scores, by=c("op_id","subject_id", "hadm_id"), all.x=T)

static_outcome_df <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/static_outcomes.csv",header=T)
names(static_outcome_df)
static_outcome_df$cardiac_comp <- ifelse(apply(static_outcome_df[,c(26,27,23,31,28,36,37,45,46),with=F], 1, function(x)sum(x,na.rm=T))>0,1,0)
table(static_outcome_df$cardiac_comp)

static_outcome_df$stroke <- ifelse(apply(static_outcome_df[,c(20,21,32),with=F], 1, function(x)sum(x,na.rm=T))>0,1,0)
table(static_outcome_df$stroke)

static_outcome_df$cardiac_comp_stroke <- ifelse(apply(static_outcome_df[,c("cardiac_comp", "stroke")], 1, function(x)sum(x,na.rm=T))>0,1,0)
table(static_outcome_df$cardiac_comp_stroke)
names(static_outcome_df)

static_outcome_df$AKI_2 <- ifelse(static_outcome_df$AKI > 0, 1, 0)
static_outcome_df$ALI_2 <- ifelse(static_outcome_df$ALI > 0, 1, 0)

names(static_outcome_df)
ds_id[1:2,]
ds_id <- merge(ds_id, static_outcome_df[,c(1:3,11:53)], by=c("op_id","subject_id", "hadm_id"), all.x=T)

ds_id[1:4,]

# roc(ds_id$death_30d~ds_id$asa)
# roc(ds_id$death_30d~ds_id$CCI_score)
# roc(ds_id$death_30d~ds_id$sort_score)
# roc(ds_id$death_30d~ds_id$RCRI)

disease <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前病史.csv")
disease <- as.data.frame(disease)

disease[1:4,]

ds_id <- merge(ds_id, disease, by=c("op_id","subject_id", "hadm_id"), all.x=T)

ds_id[1:4,]


ds1 <- as.data.frame(ds_id)


names(ds1)
ds1$cpb_duration[ds1$cpb_duration <= 0] <- NA
ds1$have_cpb <- ifelse(is.na(ds1$cpb_duration), 0, 1)
ds1$icu_duration[ds1$icu_duration <= 0] <- NA
ds1$have_icu <- ifelse(is.na(ds1$icu_duration), 0, 1)

ds1[1:4,]

names(ds1)
vars <- names(ds1)[c(7:15,30:38,40:110)]
str_vars <- names(ds1)[c(8,11,12,13,14,15,37,38,43:110)]
num_vars <- vars[!(vars %in% str_vars)]

ds1[,str_vars] <- lapply(ds1[,str_vars], function(x) {x <- as.character(x); x<-factor(x); x})
ds1[,num_vars] <- lapply(ds1[,num_vars], as.numeric)

# # 对每一列进行正态性检验
normality_test <- lapply(num_vars, function(var) {
  # var <- num_vars[1]
  x <- ds1[[var]]
  result <- tryCatch(ad.test(x), error = function(e) {
    list(p.value = 0.01)
  })
  return(result)
})

# # 将列表转换为一个命名列表
normality_test <- setNames(normality_test, num_vars)

non_norm <- c()
for (i in seq_along(normality_test)) {
  if (normality_test[[i]]$p.value < 0.05) {
    non_norm <- c(non_norm, names(normality_test)[i])
  }
}
# non_norm <- c()
non_norm <- non_norm[-c(1,2,3,4)]

# Grouped summary table
tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = ds1, strata = "set")

# Print table
tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
rn<-row.names(tab1)
tab1<-as.data.frame(tab1)
tab1 <- rownames_to_column(tab1, var = "Variable")
tab1$Variable<-rn

tab1

write.csv(tab1, file="./结果文件夹/Table1_1.csv", row.names=F, fileEncoding='gb2312')



ds1[1:2,]

library(pROC)
table(ds1$AKI)

ds1[1:4,]

roc(ds1$death_30d ~ ds1$sort_score)
roc(ds1$death_30d ~ ds1$RCRI)
roc(ds1$death_30d ~ ds1$CCI_score)
roc(ds1$death_30d ~ as.integer(ds1$asa))

roc(ds1$vent ~ ds1$sort_score)
roc(ds1$vent ~ ds1$RCRI)
roc(ds1$vent ~ ds1$CCI_score)
roc(ds1$vent ~ as.integer(ds1$asa))

roc(ds1$crrt ~ ds1$sort_score)
roc(ds1$crrt ~ ds1$RCRI)
roc(ds1$crrt ~ ds1$CCI_score)
roc(ds1$crrt ~ as.integer(ds1$asa))

roc(ds1$AKI_2 ~ ds1$sort_score)
roc(ds1$AKI_2 ~ ds1$RCRI)
roc(ds1$AKI_2 ~ ds1$CCI_score)
roc(ds1$AKI_2 ~ as.integer(ds1$asa))

names(ds1)

roc(ds1$cardiac_comp ~ ds1$sort_score)
roc(ds1$cardiac_comp ~ ds1$RCRI)
roc(ds1$cardiac_comp ~ ds1$CCI_score)
roc(ds1$cardiac_comp ~ as.integer(ds1$asa))

roc(ds1$stroke ~ ds1$sort_score)
roc(ds1$stroke ~ ds1$RCRI)
roc(ds1$stroke ~ ds1$CCI_score)
roc(ds1$stroke ~ as.integer(ds1$asa))

roc(ds1$cardiac_comp_stroke ~ ds1$sort_score)
roc(ds1$cardiac_comp_stroke ~ ds1$RCRI)
roc(ds1$cardiac_comp_stroke ~ ds1$CCI_score)
roc(ds1$cardiac_comp_stroke ~ as.integer(ds1$asa))

roc(factor(ifelse(ds1$los < 7 * 24 * 60 , 0, 1)) ~ ds1$sort_score)
roc(factor(ifelse(ds1$los < 7 * 24 * 60 , 0, 1)) ~ ds1$RCRI)
roc(factor(ifelse(ds1$los < 7 * 24 * 60 , 0, 1)) ~ ds1$CCI_score)
roc(factor(ifelse(ds1$los < 7 * 24 * 60 , 0, 1)) ~ as.integer(ds1$asa))

roc(factor(ifelse(ds1$cpb_duration < 2 * 60 , 0, 1)) ~ ds1$sort_score)
roc(factor(ifelse(ds1$cpb_duration < 2 * 60 , 0, 1)) ~ ds1$RCRI)
roc(factor(ifelse(ds1$cpb_duration < 2 * 60 , 0, 1)) ~ ds1$CCI_score)
roc(factor(ifelse(ds1$cpb_duration < 2 * 60 , 0, 1)) ~ as.integer(ds1$asa))





roc(ifelse(ds1$AKI > 0, 1, 0) ~ ds1$sort_score)
roc(ifelse(ds1$AKI > 0, 1, 0) ~ ds1$CCI_score)
roc(ifelse(ds1$AKI > 0, 1, 0) ~ ds1$asa)




library(mgcv)
fit <- mgcv::gam(death_30d ~ s(bmi), data=operations_derived, family='binomial')
plot(fit, select=1)

# 指定输出的 PNG 文件路径和名称
png(file = "./结果文件夹/fit_plot.png")

# 绘制图形
plot(fit, select=1)

# 关闭图形设备
dev.off()

labs <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs_hadm.csv")
# labs <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs.csv")
str(labs)
labs[1:4,]
ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
# N_hadm <- length(unique(ds_id$hadm_id))
labs <- labs[which(labs$subject_id %in% unique(ds_id$subject_id)), ]
ds_id <- unique(ds_id[,c(2,4),drop=F])
ds_id[1:4,]

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)
operations_derived[1:4,]

# uniq_sid <- unique(labs$subject_id)
# new_labs <- lapply(1:length(uniq_sid), function(k) {
#   labs_k <- labs[labs$subject_id == uniq_sid[k], , drop = F]
#   labs_k$hadm_id <- rep(NA, nrow(labs_k))
  
#   for (i in 1:nrow(labs_k)) {
#     operations_k <- operations_derived[operations_derived$subject_id == labs_k$subject_id[i], ]
#     closest_index <- which.min(abs(labs_k$chart_time[i] - operations_k$admission_time) + 
#                                abs(labs_k$chart_time[i] - operations_k$discharge_time))
#     labs_k$hadm_id[i] <- operations_k$hadm_id[closest_index]
#   }
#   if (k %% 100 == 0) print(k)
#   return(labs_k)
# })

# labs <- do.call(rbind, new_labs)
# fwrite(labs, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs_hadm.csv", row.names=F)

labs[1:3,]
dim(labs)
names(ds_id)
labs <- merge(labs, ds_id, by='subject_id', all.x=T)
labs[1:3,]


N_subject <- length(unique(labs$subject_id))
N_hadm <- length(unique(labs$hadm_id))
uniq_item <- sort(unique(labs$item_name))
result <- list()
for(i in 1:length(uniq_item)){
  # i <- 5
  # names(labs_1)
  labs_1 <- labs[which(labs$item_name == uniq_item[i]),,drop=F]
  labs_ <- labs_1[,c(4,6),drop=F];names(labs_) <- c(uniq_item[i],"set")
  p_test <- tryCatch(ad.test(labs_[[1]]), error = function(e) {
    list(p.value = 0.01)
  })
  vars <- uniq_item[i]
  str_vars <- c()
  # Grouped summary table
  tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = labs_, strata = "set")
  # Print table
  non_norm <- ifelse(p_test$p.value < 0.05, c(uniq_item[i]), c())
  tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
  rn<-row.names(tab1)
  tab1<-as.data.frame(tab1)
  tab1 <- rownames_to_column(tab1, var = "Variable")
  tab1$Variable<-rn
  cur_r <- tab1[2,,drop=F]
  cur_r$record_counts <- tab1$Overall[1]
  cur_r$N_subject <- length(unique(labs_1$subject_id))
  cur_r$N_hadm <- length(unique(labs_1$hadm_id))
  cur_r$miss_rate_subject <- (N_subject - length(unique(labs_1$subject_id))) / N_subject
  cur_r$miss_rate_hadm <- (N_hadm - length(unique(labs_1$hadm_id))) / N_hadm
  result[[i]] <- cur_r
  print(i)
}
result <- do.call(rbind, result)
row.names(result) <- NULL
result

write.csv(result, file="./结果文件夹/Table1_1_lab.csv", row.names=F, fileEncoding='gb2312')



vital <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals_derived.csv")
vital[1:4,]
ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
ds_id <- ds_id[,1:4,drop=F]
vital <- vital[which(vital$op_id %in% unique(ds_id$op_id)), ]

# u <- c("air","dobui","dopai","eph","epi","epii","mlni","nepi","ntgi","phe","vaso","VIS","aft","ftn","rfti","sft","mdz","ppf","ppfi","cryo","ffp","pc","pheresis","psa","rbc","d10w","d50w","d5w","hns","hs","ns","TCI","alb20","alb5","alb","hes","ebl","uo","sti","stii","stiii","stv5")
# vital <- vital[-which(vital$item_name %in% u & vital$value <= 0), ]

# table(vital$value[vital$item_name == "alb20"])
# vital$value[vital$item_name == "alb20" & vital$value==1] <- 50

ds_id[1:4,]
ds_id <- unique(ds_id[,c(1,2,4),drop=F])

N_subject <- length(unique(ds_id$subject_id))
N_op <- length(unique(ds_id$op_id))

vital <- merge(vital, ds_id, by=c('op_id','subject_id'), all.x=T)

vital[1:4,]

uniq_item <- sort(unique(vital$item_name))
# which(uniq_item == "etiso")
result <- list()
count <- 1
for(i in 1:length(uniq_item)){
  # i<-1
  vital_1 <- vital[which(vital$item_name == uniq_item[i]),,drop=F]
  vital_ <- vital_1[,c(5,6),drop=F];names(vital_) <- c(uniq_item[i],"set")
  group_flag <- T
  if (length(unique(vital_$set)) != 3) {
    group_flag <- F
  }
  p_test <- tryCatch(ad.test(vital_[[1]]), error = function(e) {
    list(p.value = 0.01)
  })
  vars <- uniq_item[i]
  str_vars <- c()
  # Grouped summary table
  if(group_flag){
    tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = vital_, strata = "set")
  } else{
    tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = vital_)
    uu <- aggregate(vital_[,vars,with=F],by=vital_[,"set"],FUN = function(x) {
      uu <- round(quantile(x, c(0.25, 0.5, 0.75)),2)
      paste0(uu[2], " [", uu[1], ", ", uu[3], "]")
    })
  }
  # Print table
  non_norm <- ifelse(p_test$p.value < 0.05, c(uniq_item[i]), c())
  tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
  rn<-row.names(tab1)
  tab1<-as.data.frame(tab1)
  tab1 <- rownames_to_column(tab1, var = "Variable")
  tab1$Variable<-rn
  
  if(group_flag){
    cur_r <- tab1[2,,drop=F]
  } else{
    tmp <- data.frame(`1` = ifelse(length(uu[[2]][which(uu[[1]] == 1)]) == 0, NA, uu[[2]][which(uu[[1]] == 1)]), 
                      `2` = ifelse(length(uu[[2]][which(uu[[1]] == 2)]) == 0, NA, uu[[2]][which(uu[[1]] == 2)]), 
                      `3` = ifelse(length(uu[[2]][which(uu[[1]] == 3)]) == 0, NA, uu[[2]][which(uu[[1]] == 3)]), 
                      "p" = NA, "test" = NA)
    names(tmp)[1:3] <- c("1", "2", "3")
    cur_r <- cbind(tab1[2,,drop=F], tmp)
  }
  cur_r$record_counts <- tab1$Overall[1]
  cur_r$N_op <- length(unique(vital_1$op_id))
  cur_r$N_subject <- length(unique(vital_1$subject_id))
  cur_r$miss_rate_subject <- (N_subject - length(unique(vital_1$subject_id))) / N_subject
  cur_r$miss_rate_op <- (N_op - length(unique(vital_1$op_id))) / N_op
  result[[count]] <- cur_r
  count <- count + 1
  print(i)
}
result <- do.call(rbind, result)
row.names(result) <- NULL
result



write.csv(result, file="./结果文件夹/Table1_2_vital.csv", row.names=F, fileEncoding='gb2312')



ward_vital <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals.csv")
ward_vital[1:4,]
ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
ds_id <- ds_id[,1:4,drop=F]
ward_vital <- ward_vital[which(ward_vital$subject_id %in% unique(ds_id$subject_id)), ]

ds_id <- unique(ds_id[,c(2,4),drop=F])
ds_id[1:4,]

uniq_sid <- unique(ward_vital$subject_id)
new_ward_vital <- lapply(1:length(uniq_sid), function(k) {
  ward_vital_k <- ward_vital[which(ward_vital$subject_id == uniq_sid[k]), , drop = F]
  ward_vital_k$hadm_id <- rep(NA, nrow(ward_vital_k))
  
  for (i in 1:nrow(ward_vital_k)) {
    operations_k <- operations_derived[operations_derived$subject_id == ward_vital_k$subject_id[i], ]
    closest_index <- which.min(abs(ward_vital_k$chart_time[i] - operations_k$admission_time) + 
                               abs(ward_vital_k$chart_time[i] - operations_k$discharge_time))
    ward_vital_k$hadm_id[i] <- operations_k$hadm_id[closest_index]
  }
  if (k %% 100 == 0) print(k)
  return(ward_vital_k)
})

ward_vital <- do.call(rbind, new_ward_vital)
fwrite(ward_vital, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals_hadm.csv", row.names=F)



ward_vital[1:3,]
dim(ward_vital)
names(ds_id)
ward_vital <- merge(ward_vital, ds_id, by='subject_id', all.x=T)
ward_vital[1:3,]


N_subject <- length(unique(ward_vital$subject_id))
N_hadm <- length(unique(ward_vital$hadm_id))
uniq_item <- sort(unique(ward_vital$item_name))
uniq_item <- uniq_item[c(1,4:8,10:15)]
result <- list()
for(i in 1:length(uniq_item)){
  # i <- 5
  # names(ward_vital_1)
  ward_vital_1 <- ward_vital[which(ward_vital$item_name == uniq_item[i]),,drop=F]
  ward_vital_ <- ward_vital_1[,c(4,6),drop=F];names(ward_vital_) <- c(uniq_item[i],"set")
  p_test <- tryCatch(ad.test(ward_vital_[[1]]), error = function(e) {
    list(p.value = 0.01)
  })
  vars <- uniq_item[i]
  str_vars <- c()
  # Grouped summary table
  tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = ward_vital_, strata = "set")
  # Print table
  non_norm <- ifelse(p_test$p.value < 0.05, c(uniq_item[i]), c())
  tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
  rn<-row.names(tab1)
  tab1<-as.data.frame(tab1)
  tab1 <- rownames_to_column(tab1, var = "Variable")
  tab1$Variable<-rn
  cur_r <- tab1[2,,drop=F]
  cur_r$record_counts <- tab1$Overall[1]
  cur_r$N_subject <- length(unique(ward_vital_1$subject_id))
  cur_r$N_hadm <- length(unique(ward_vital_1$hadm_id))
  cur_r$miss_rate_subject <- (N_subject - length(unique(ward_vital_1$subject_id))) / N_subject
  cur_r$miss_rate_hadm <- (N_hadm - length(unique(ward_vital_1$hadm_id))) / N_hadm
  result[[i]] <- cur_r
  print(i)
}
result <- do.call(rbind, result)
row.names(result) <- NULL
result

write.csv(result, file="./结果文件夹/Table1_1_ward_vital.csv", row.names=F, fileEncoding='gb2312')























operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)

# 初始的手术数量和病人数量
initial_operations <- nrow(operations_derived)
initial_patients <- length(unique(operations_derived$subject_id))

# 排除年龄小于16岁的手术
operations_age_filtered <- operations_derived %>%
  filter(age >= 16)
operations_after_age_filter <- nrow(operations_age_filtered)
patients_after_age_filter <- length(unique(operations_age_filtered$subject_id))

# 排除手术时间小于15分钟的手术
operations_duration_filtered <- operations_age_filtered %>%
  filter(op_duration >= 15)
operations_after_duration_filter <- nrow(operations_duration_filtered)
patients_after_duration_filter <- length(unique(operations_duration_filtered$subject_id))

# 排除麻醉类型为Neuraxial, MAC, Regional的手术
operations_antype_filtered <- operations_duration_filtered %>%
  filter(!(antype %in% c("Neuraxial", "MAC", "Regional")))
operations_after_antype_filter <- nrow(operations_antype_filtered)
patients_after_antype_filter <- length(unique(operations_antype_filtered$subject_id))

# 输出每一步过滤后的手术数量和病人数量
cat("初始手术数量:", initial_operations, "初始病人数量:", initial_patients, "\n")
cat("年龄过滤后手术数量:", operations_after_age_filter, "年龄过滤后病人数量:", patients_after_age_filter, "\n")
cat("手术时间过滤后手术数量:", operations_after_duration_filter, "手术时间过滤后病人数量:", patients_after_duration_filter, "\n")
cat("麻醉类型过滤后手术数量:", operations_after_antype_filter, "麻醉类型过滤后病人数量:", patients_after_antype_filter, "\n")


ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
ds_id <- ds_id[,1:4,drop=F]
ds_id[1:4,]

# 在已经筛选的数据集上添加数据集索引的统计
set_counts <- ds_id %>%
  group_by(set) %>%
  summarise(
    operations_count = n(),
    patients_count = n_distinct(subject_id)
  )

# 输出每个数据集的手术数量和病人数量
print(set_counts)





ds <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals.csv")
ds[which(ds$item_name == "vent" & ds$subject_id == 188297542),]








ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame
ds_id <- ds_id[,1:4,drop=F]

operations_derived <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations_derived.csv", header=T)

operations_derived[1:4,]

ds_id <- merge(ds_id, operations_derived, by=c("op_id","subject_id", "hadm_id"), all.x=T)

ds_id[1:3,]

# 计算每列的非NA的unique(subject_id)和unique(op_id)的数量
columns_to_analyze <- c("age", "sex", "weight", "height", "bmi", "asa", "emop", "department", "antype", "icd10_pcs", "or_duration", "op_duration", "an_duration", "cpb_duration")

# 初始化列表来存储结果
subject_id_counts <- list()
op_id_counts <- list()

# 遍历每列，计算非NA的unique(subject_id)和unique(op_id)的数量
for (col in columns_to_analyze) {
  non_na_data <- ds_id[!is.na(ds_id[[col]]), ]
  subject_id_counts[[col]] <- length(unique(non_na_data$subject_id))
  op_id_counts[[col]] <- length(unique(non_na_data$op_id))
}

# 计算整个数据集的unique(subject_id)和unique(op_id)的数量
total_unique_subjects <- length(unique(ds_id$subject_id))
total_unique_ops <- length(unique(ds_id$op_id))

# 计算每列的缺失率
subject_miss_rate <- sapply(subject_id_counts, function(x) (total_unique_subjects - x) / total_unique_subjects)
op_miss_rate <- sapply(op_id_counts, function(x) (total_unique_ops - x) / total_unique_ops)

# 结果输出
results <- data.frame(
  Column = columns_to_analyze,
  Unique_Subjects = unlist(subject_id_counts),
  Unique_Ops = unlist(op_id_counts),
  Subject_Miss_Rate = subject_miss_rate,
  Op_Miss_Rate = op_miss_rate
)

results

write.csv(results, file="/home/luojiawei/pengxiran_project/结果文件夹/subtable6补充.csv", fileEncoding="gb2312")
