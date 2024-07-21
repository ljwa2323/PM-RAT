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
dim(operations_derived)
as.data.frame(operations_derived[1:2,])
# range(operations_derived$admission_time)
# as.data.frame(operations_derived[operations_derived$admission_time > 0,][1:4,])

scores <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/scores.csv")
dim(scores)
scores[1:2,]
disease <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/术前病史.csv")
disease <- as.data.frame(disease)
dim(disease)
disease[1:2,]
operations_derived <- merge(operations_derived, scores[,c(1,5:7)], by="op_id", all.x=T)
operations_derived <- merge(operations_derived, disease[,c(1,4:ncol(disease))], by="op_id", all.x=T)

vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals_derived.csv")
ward_vitals <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/ward_vitals.csv")
vitals[1:3,]
ward_vitals[1:3,]
unique(ward_vitals$item_name)

vitals_ <- rbind(vitals[,2:5,drop=F],ward_vitals)

# as.data.frame(vitals[1:4,])

labs <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs.csv")

static_outcome_df <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/static_outcomes.csv",header=T)

static_outcome_df[1:2,]
# names(static_outcome_df)
# apply(static_outcome_df[,c(9:44)], 2, function(x)mean(x, na.rm=T)) %>% round(., 3)
# prop.table(table(ifelse(static_outcome_df$AKI >=1, 1, 0), useNA = "always"))
# table(static_outcome_df$AKI)
names(static_outcome_df)
static_outcome_df$cardiac_comp <- ifelse(apply(static_outcome_df[,c(26,27,23,31,28,36,37,45,46),with=F], 1, function(x)sum(x,na.rm=T)) > 0,1,0)
table(static_outcome_df$cardiac_comp)

static_outcome_df$stroke <- ifelse(apply(static_outcome_df[,c(20,21,32),with=F], 1, function(x)sum(x,na.rm=T))>0,1,0)
table(static_outcome_df$stroke)

static_outcome_df$cardiac_comp_stroke <- ifelse(apply(static_outcome_df[,c("cardiac_comp", "stroke")], 1, function(x)sum(x,na.rm=T))>0,1,0)
table(static_outcome_df$cardiac_comp_stroke)
names(static_outcome_df)

static_outcome_df$AKI_2 <- ifelse(static_outcome_df$AKI > 0, 1, 0)
static_outcome_df$ALI_2 <- ifelse(static_outcome_df$ALI > 0, 1, 0)

u <- static_outcome_df$los
static_outcome_df$los <- ifelse(u <= 7 * 60 * 24, 0, 1)
u <- static_outcome_df$op_duration
static_outcome_df$op_duration <- ifelse(u < 120, 0, ifelse(u < 240, 1, ifelse( u < 480, 2, 3)))
u <- static_outcome_df$or_duration
static_outcome_df$or_duration <- ifelse(u < 120, 0, ifelse(u < 240, 1, ifelse( u < 480, 2, 3)))
u <- static_outcome_df$an_duration
static_outcome_df$an_duration <- ifelse(u < 120, 0, ifelse(u < 240, 1, ifelse( u < 480, 2, 3)))
u <- static_outcome_df$cpb_duration
static_outcome_df$cpb_duration <- ifelse(is.na(u), 0, ifelse(u < 120, 1, ifelse(u < 240, 2, 3)))
table(static_outcome_df$cpb_duration)
u <- static_outcome_df$icu_duration
static_outcome_df$icu_duration_1d <- ifelse(u > 60*24, 1, 0)
static_outcome_df$icu_duration_12h <- ifelse(u > 60*12, 1, 0)
table(static_outcome_df$icu_duration_12h)
table(static_outcome_df$icu_duration_1d)

static_outcome_df[1:4,]


# cor(static_outcome_df[,c(10:13,46:50)])

# operations_derived[1:4,]
names(operations_derived)
# 样本筛选
operations_derived <- operations_derived %>%
  filter(age >= 16, 
         op_duration >= 15, 
         !(antype %in% c("Neuraxial", "MAC", "Regional")))

dim(operations_derived)
unique(operations_derived$antype)


d_lab <- read_excel("./var_dict.xlsx", sheet=1, col_names = T)
d_lab$agg_f <- "last"
as.data.frame(d_lab)

d_vit <- read_excel("./var_dict.xlsx", sheet=2, col_names = T)
as.data.frame(d_vit)
names(d_vit)
# as.data.frame(d_vit)[,c(1,2,5:8)]


# stat_lab <- get_stat_long(labs, 
#                           d_lab$itemid, 
#                           d_lab$value_type,
#                           "item_name",
#                           "value",
#                           d_lab$cont)

# stat_vit <- get_stat_long(vitals_, 
#                           d_vit$itemid, 
#                           d_vit$value_type,
#                           "item_name",
#                           "value",
#                           d_vit$cont)


# names(operations_derived)
# operations_derived[1:4,]
# unique(operations_derived$department)
stat_static <- get_stat_wide(operations_derived[,c(6,7,8,9,11,12,29,37,41:63)],
                              names(operations_derived)[c(6,7,8,9,11,12,29,37,41:63)],
                              c("num","bin","num","num","cat","bin","num","cat",rep("bin",63-41+1)),
                              rep(NA, 31))

# save(list=c("stat_lab", "stat_vit", "stat_static"), file="./stats.RData")
load(file="./stats.RData")
stat_vit1 <- stat_vit
ind <- which(!is.na(d_vit$cont1))
for(i in 1:length(ind)){
  # i <- 1
  stat_vit1[ind[i]][[1]]$cont <- as.numeric(d_vit$cont1[ind[i]])
}

# stat_static$type_name
miss_data <- operations_derived[,c(6,7,8,9,11,12,29,37,41:63)]
imp <- mice(miss_data, m=1, maxit=5, seed=123)
comp_data <- complete(imp)
# summary(comp_data)
operations_derived[,c(6,7,8,9,11,12,29,37,41:63)] <- comp_data

# summary(operations_derived[,c(6,7,8,9,11,12,29,37,41:63)])

# dim(vitals)
# stat_vit$TCI

operations_derived[1:4,]

# stat_lab
# stat_vit

ds_id <- operations_derived[!duplicated(operations_derived[,c("subject_id","hadm_id","op_id")]), c("subject_id","hadm_id","op_id")]
dim(ds_id)
ds_id[1:4,]

set.seed(123) # 确保可重复性
unique_subject_ids <- unique(ds_id$subject_id)
total_subjects <- length(unique_subject_ids)
indices <- sample(1:total_subjects)

# 计算划分的索引
train_subjects_index <- indices[1:round(total_subjects*0.7)]
test_subjects_index <- indices[(round(total_subjects*0.7) + 1):(round(total_subjects*0.7) + round(total_subjects*0.2))]
val_subjects_index <- indices[(round(total_subjects*0.7) + round(total_subjects*0.2) + 1):total_subjects]

# 获取对应的 subject_id
train_subjects <- unique_subject_ids[train_subjects_index]
test_subjects <- unique_subject_ids[test_subjects_index]
val_subjects <- unique_subject_ids[val_subjects_index]

# 创建新列并根据 subject_id 赋值
ds_id$set <- ifelse(ds_id$subject_id %in% train_subjects, 1,
                    ifelse(ds_id$subject_id %in% test_subjects, 2, 3))

# 检查分配结果
table(ds_id$set)

static_outcome_df[1:4,]
ds_id <- merge(ds_id, static_outcome_df[,c(1,4:ncol(static_outcome_df)),with=F], by="op_id",all.x = T)
names(ds_id)
names(ds_id)[which(names(ds_id) == "death_30d")] <- "in_hospital_death"
ds_id[1:4,]

# fwrite(ds_id, file="/home/luojiawei/pengxiran_project_data/ds_id.csv",row.names=F)
ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame

# ds_id[ds_id$death_30d==1 & ds_id$set==2,][71:80,1:4]

get_1opid_allAlign <- function(k) {
  # k <- 456
  cur_opid <- ds_id$op_id[k]
  cur_sid <- ds_id$subject_id[ds_id$op_id==cur_opid]
  cur_hid <- ds_id$hadm_id[ds_id$op_id==cur_opid]
  
  operation_row <- operations_derived[which(operations_derived$op_id == cur_opid),]
  
  t0 <- operation_row$admission_time
  t1 <- operation_row$discharge_time
  t_in <- operation_row$orin_time
  t_out <- operation_row$orout_time
  t_start <- operation_row$opstart_time
  t_end <- operation_row$opend_time
  # t1_icu <- operation_row$icuout_time

  lab_k <- labs[labs$subject_id == cur_sid, ]
  lab_k1 <- resample_single_long(lab_k,
                              d_lab$itemid,
                              d_lab$value_type,
                              d_lab$agg_f,
                              c((t_in + t0)/2),
                              "item_name",
                              "value",
                              "chart_time",
                              (t_in - t0),
                              direction = "both",
                              keepNArow = F)
  lab_k1 <- lab_k1[,c(1,3:ncol(lab_k1)),drop=F]
  mask_lab <- get_mask(lab_k1, 2:ncol(lab_k1), 1)
  lab_k1 <- fill(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), d_lab$fill1, d_lab$fill2, stat_lab)
  # lab_k1 <- norm_num(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), stat_lab)
  lab_k1 <- lab_k1[,2:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_pre <- mask_lab[,2:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_pre <- lab_k1

  lab_k <- labs[labs$subject_id == cur_sid, ]
  lab_k1 <- resample_single_long(lab_k,
                              d_lab$itemid,
                              d_lab$value_type,
                              d_lab$agg_f,
                              c((2 * t_out + 60 * 24)/2),
                              "item_name",
                              "value",
                              "chart_time",
                              (60 * 24),
                              direction = "both",
                              keepNArow = F)
  lab_k1 <- lab_k1[,c(1,3:ncol(lab_k1)),drop=F]
  mask_lab <- get_mask(lab_k1, 2:ncol(lab_k1), 1)
  lab_k1 <- fill(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), d_lab$fill1, d_lab$fill2, stat_lab)
  # lab_k1 <- norm_num(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), stat_lab)
  lab_k1 <- lab_k1[,2:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_post <- mask_lab[,2:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_post <- lab_k1
  # 遍历 lab_post 的每一列
  index <- which(mask_lab_post[1, ,drop=T] != 1)
  if(length(index) > 0) lab_post[1, index] <- lab_pre[1, index, drop=F]

  vit_k <- rbind(vitals[vitals$op_id == cur_opid, 2:5,drop=F], ward_vitals[ward_vitals$subject_id == cur_sid,,drop=F])

  vit_k1 <- resample_single_long(vit_k,
                              d_vit$itemid,
                              d_vit$value_type,
                              d_vit$agg_f,
                              seq(min(t0,t_in - 10), t_in-5, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              direction = "both",
                              keepNArow = F,
                              keep_first = F)
  # colnames(vit_k1)
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  vit_k1 <- vit_k1[,c(1,3:ncol(vit_k1)),drop=F]
  mask_vit_pre <- get_mask(vit_k1, 2:ncol(vit_k1), 1)
  mask_last <- mask_vit_pre[nrow(mask_vit_pre),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1),drop=T]
  vit_k1 <- fill(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), d_vit$fill1, d_vit$fill2, stat_vit)
  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # dim(vit_k1)
  # colnames(vit_k1)
  vit_pre <- vit_k1[,-c(13:15),drop=F]
  mask_vit_pre <- mask_vit_pre[,-c(13:15),drop=F]

  vit_pre <- vit_pre %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_pre$art_mbp <- (vit_pre$art_sbp + 2 * vit_pre$art_dbp) / 3
  # names(vit_pre)
  # vit_pre <- vit_pre[,c(1,7,10:12,14,18,23,34,36:79),drop=F]
  mask_vit_pre <- mask_vit_pre %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # mask_vit_pre <- mask_vit_pre[,c(1,7,10:12,14,18,23,34,36:79),drop=F]

  vit_k1 <- resample_single_long(vit_k,
                              d_vit$itemid,
                              d_vit$value_type,
                              d_vit$agg_f,
                              seq(t_in, t_out, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              direction = "both",
                              keepNArow = F)
  X <- vit_k1[,c(1,3:ncol(vit_k1)), drop=F]
  # colnames(vit_k1)
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  vit_k1 <- vit_k1[,c(1,3:ncol(vit_k1)),drop=F]
  mask_vit_in <- get_mask(vit_k1, 2:ncol(vit_k1), 1)

  index <- which(mask_vit_in[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 1] <- vit_last[index]
  }
  mask_last <- mask_vit_in[nrow(mask_vit_in),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1),drop=T]

  vit_k1 <- fill(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit1), d_vit$fill1, d_vit$fill2, stat_vit1)
  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # colnames(vit_k1)
  vit_in <- vit_k1[,-c(13:15),drop=F]
  mask_vit_in <- mask_vit_in[,-c(13:15),drop=F]

  vit_in <- vit_in %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_in$art_mbp <- (vit_in$art_sbp + 2 * vit_in$art_dbp) / 3
  # names(vit_in)
  vit_in <- vit_in[,c(1:72),drop=F]
  mask_vit_in <- mask_vit_in %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_vit_in <- mask_vit_in[,c(1:72),drop=F]

  vit_k1 <- resample_single_long(vit_k,
                              d_vit$itemid,
                              d_vit$value_type,
                              d_vit$agg_f,
                              seq(min(t_out+5, t_out + 60 * 24 - 10), t_out + 60 * 24, 5), 
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              direction = "both",
                              keepNArow = F,
                              keep_first = F)
  # colnames(vit_k1)
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])

  vit_k1 <- vit_k1[,c(1,3:ncol(vit_k1)),drop=F]
  mask_vit_post <- get_mask(vit_k1, 2:ncol(vit_k1), 1)

  index <- which(mask_vit_post[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 1] <- vit_last[index]
  }

  vit_k1 <- fill(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), d_vit$fill1, d_vit$fill2, stat_vit)
  vit_k1 <- fill_last_values(vit_k1, mask_vit_post, 2:ncol(vit_k1), 1, d_vit)
  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # dim(vit_k1)
  # colnames(vit_k1)
  vit_post <- vit_k1[,-c(13:15),drop=F]
  mask_vit_post <- mask_vit_post[,-c(13:15),drop=F]

  vit_post <- vit_post %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_post$art_mbp <- (vit_post$art_sbp + 2 * vit_post$art_dbp) / 3
  # names(vit_post)
  # vit_post <- vit_post[,c(1,7,10:12,14,18,23,34,36:79),drop=F]
  mask_vit_post <- mask_vit_post %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # mask_vit_post <- mask_vit_post[,c(1,7,10:12,14,18,23,34,36:79),drop=F]

  
  X <- X %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  if(nrow(vit_pre) == 1){
    vit_pre$dt <- c(0)
  } else{
    vit_pre$dt <- c(0, diff(vit_pre$time))
  }
  vit_pre$dt <- vit_pre$dt / 60 / 24

  if(nrow(vit_in) == 1){
    vit_in$dt <- c(0)
  } else{
    vit_in$dt <- c(0, diff(vit_in$time))
  }
  vit_in$dt <- vit_in$dt / 60 / 24

  if(nrow(vit_post) == 1){
    vit_post$dt <- c(0)
  } else{
    vit_post$dt <- c(0, diff(vit_post$time))
  }
  vit_post$dt <- vit_post$dt / 60 / 24

  y_mat <- list()
  # names(X)
  u <- apply(X[,c("art_mbp","nibp_mbp")], 1, function(x) Min(x, na.rm=T))
  u1 <- vit_in$art_mbp
  m <- mean(vit_pre$art_mbp)
  u1 <- ifelse(u1 < 0.8*m, 1, ifelse(u1 < 1.2*m, 0, 2)) # 术前平均m, 1.2*m-high , 0.8*m low
  u1[is.na(u)] <- NA
  y_mat[[1]] <- cbind("mbp" = lead(u1))

  u <- apply(X[,c("art_sbp","nibp_sbp")], 1, function(x) Min(x, na.rm=T))
  u1 <- vit_in$art_sbp
  m <- mean(vit_pre$art_sbp)
  u1 <- ifelse(u1 < 0.8*m, 1, ifelse(u1 < 1.2*m, 0, 2)) # 术前平均m, 1.2*m-high , 0.8*m low
  y_mat[[2]] <- cbind("sbp" = lead(u1))
  u1[is.na(u)] <- NA
  
  u <- apply(X[,c(27:30),drop=F], 1, function(x) Max(x, na.rm=T))
  u <- ifelse(u < 0.05, 0, ifelse(u < 0.1, 1, ifelse(u < 0.2, 2, 3)))
  y_mat[[3]] <- cbind("st" = lead(u))

  u <- ifelse(apply(X[,c(58:63)], 1, function(x) Any(x > 0, na.rm=T)),1,0)
  u <- lapply(1:length(u), function(i) ifelse(any(u[(i+1):min(i+5,length(u))] == 1),1,0)) %>% unlist
  y_mat[[4]] <- cbind("blood_use" = u)

  u <- ifelse(apply(X[,c(73:74)], 1, function(x) Any(x > 0, na.rm=T)),1,0)
  u <- lapply(1:length(u), function(i) ifelse(any(u[(i+1):min(i+5,length(u))] == 1),1,0)) %>% unlist
  y_mat[[5]] <- cbind("coll_use" = u)

  u <- ifelse(apply(X[,c(39:47)], 1, function(x) Any(x > 0, na.rm=T)),1,0)
  u <- lapply(1:length(u), function(i) ifelse(any(u[(i+1):min(i+5,length(u))] == 1),1,0)) %>% unlist
  y_mat[[6]] <- cbind("vaso_use" = u)
  
  u <- lead(ifelse(X[,"bt",drop=T] < 36.1, 1, ifelse(X[,"bt",drop=T] < 37.2, 0, 2))) # 
  y_mat[[7]] <- cbind("bt" = u)

  u <- X[,"hr",drop=T] # 术前平均m,  1.2*m-high , 0.8*m low
  m <- mean(vit_pre$hr)
  u <- ifelse(u < 0.8*m, 1, ifelse(u < 1.2*m, 0, 2)) # 术前平均m, 1.2*m-high , 0.8*m low
  y_mat[[8]] <- cbind("hr" = lead(u))
  u <- lead(ifelse(X[,"spo2",drop=T] < 90, 1, 0))
  y_mat[[9]] <- cbind("spo2" = u)

  y_mat <- do.call(cbind, y_mat)

  y_mask <- rbind(apply(y_mat, 2, function(x) ifelse(is.na(x), 0, 1)))
  y_mask <- y_mask %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  y_mat <- rbind(apply(y_mat, 2, function(x) ifelse(is.na(x), 0, x)))
  y_mat <- y_mat %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
#   names(operation_row)
  x_s <- as.matrix(operation_row[,c(6,7,8,9,11,12,29,37,41:63),drop=F])
  # x_s <- norm_num(x_s,1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- to_onehot(x_s, 1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- x_s %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  # names(static_outcome_df)
  y_static <- static_outcome_df[which(static_outcome_df$op_id == cur_opid),4:ncol(static_outcome_df)]
  y_mask1 <- rbind(apply(y_static, 2, function(x) ifelse(is.na(x), 0, 1)))
  y_mask1 <- y_mask1 %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  y_static <- rbind(apply(y_static, 2, function(x) ifelse(is.na(x), 0, x)))
  y_static <- y_static %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  oper_info <- operations_derived[which(operations_derived$op_id == cur_opid),c(31,32,33,34),drop=F]
  index <- which(is.na(oper_info[1,]))
  if(length(index) > 0) oper_info[1, index] <- 0

  t_list <- data.frame("time"=seq(t_in, t_out, 5))
  # dim(vit_post)
  # dim(t_list)
  
  return(list(X, 
              lab_pre, mask_lab_pre, lab_post, mask_lab_post, 
              vit_pre, mask_vit_pre, vit_in, mask_vit_in, vit_post, mask_vit_post, 
              t_list, x_s, y_mat, y_mask, y_static, y_mask1,
              oper_info))
}

ds_id$op_id[1201]
datas <- get_1opid_allAlign(123)
datas[[6]][1:2,]
datas[[10]][1:2,]
datas[[11]][1:2,]
# length(datas)
ds_id[1:4,]

which(ds_id$op_id == 490201422)

process_data <- function(k, root_path) {
    # k<-232
    id_k<-ds_id$op_id[k]
    folder_path<-file.path(root_path, id_k)
    create_dir(folder_path, F)
    
    datas <- get_1opid_allAlign(k)
    fwrite(datas[[1]], file=file.path(folder_path, "X.csv"), row.names=F)

    fwrite(datas[[2]], file=file.path(folder_path, "lab_pre.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "mask_lab_pre.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "lab_post.csv"), row.names=F)
    fwrite(datas[[5]], file=file.path(folder_path, "mask_lab_post.csv"), row.names=F)

    fwrite(datas[[6]], file=file.path(folder_path, "vit_pre.csv"), row.names=F)
    fwrite(datas[[7]], file=file.path(folder_path, "mask_vit_pre.csv"), row.names=F)
    fwrite(datas[[8]], file=file.path(folder_path, "vit_in.csv"), row.names=F)
    fwrite(datas[[9]], file=file.path(folder_path, "mask_vit_in.csv"), row.names=F)
    fwrite(datas[[10]], file=file.path(folder_path, "vit_post.csv"), row.names=F)
    fwrite(datas[[11]], file=file.path(folder_path, "mask_vit_post.csv"), row.names=F)

    fwrite(datas[[12]], file=file.path(folder_path, "t_list.csv"), row.names=F)
    fwrite(datas[[13]], file=file.path(folder_path, "x_s.csv"), row.names=F)
    fwrite(datas[[14]], file=file.path(folder_path, "y_mat.csv"), row.names=F)
    fwrite(datas[[15]], file=file.path(folder_path, "y_mask.csv"), row.names=F)
    fwrite(datas[[16]], file=file.path(folder_path, "y_static.csv"), row.names=F)
    fwrite(datas[[17]], file=file.path(folder_path, "y_mask1.csv"), row.names=F)
    fwrite(datas[[18]], file=file.path(folder_path, "oper_info.csv"), row.names=F)
}

root_path <- "/home/luojiawei/pengxiran_project_data/all_op_id1/"
create_dir(root_path, T)

# process_data(1, root_path = root_path)

chunk_size <- 3000
num_rows <- nrow(ds_id)
num_chunks <- ceiling(num_rows / chunk_size)

results <- list()

for (i in 1:num_chunks) {
  start_index <- (i - 1) * chunk_size + 1
  end_index <- min(i * chunk_size, num_rows)
  
  results[[i]] <- mclapply(start_index:end_index, 
                    function(x) {
                      result <- tryCatch({
                        process_data(x, root_path = root_path)
                      }, error = function(e) {
                        print(e)
                        print(x)
                      })
                      if(x %% 1000 == 0) print(x)
                      
                      return(result)
                    }, mc.cores = detectCores())
  gc()
}

results <- do.call(c, results)



