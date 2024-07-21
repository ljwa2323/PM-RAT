

get_1opid_allAlign <- function(k) {
  # k <- 34
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
                              seq(t0, t_in, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              direction = "both",
                              keepNArow = F)
  mask_lab <- get_mask(lab_k1, 3:ncol(lab_k1), 1)
  mask_last <- mask_lab[nrow(mask_lab),-1,drop=T]
  lab_last <- lab_k1[nrow(lab_k1),-1,drop=T]
  lab_k1 <- fill(lab_k1, 3:ncol(lab_k1), 1, get_type(stat_lab), d_lab$fill1, d_lab$fill2, stat_lab)
  # lab_k1 <- norm_num(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), stat_lab)
  lab_k1 <- lab_k1[,1:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_pre <- mask_lab[,1:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_pre <- lab_k1

  lab_k <- labs[labs$subject_id == cur_sid, ]
  lab_k1 <- resample_single_long(lab_k,
                              d_lab$itemid,
                              d_lab$value_type,
                              d_lab$agg_f,
                              seq(t_out, t1, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              direction = "both",
                              keepNArow = F)
  mask_lab <- get_mask(lab_k1, 3:ncol(lab_k1), 1)

  index <- which(mask_lab[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      lab_k1[1, index + 1] <- lab_last[index]
  }

  lab_k1 <- fill(lab_k1, 3:ncol(lab_k1), 1, get_type(stat_lab), d_lab$fill1, d_lab$fill2, stat_lab)
  # lab_k1 <- norm_num(lab_k1, 2:ncol(lab_k1), 1, get_type(stat_lab), stat_lab)
  lab_k1 <- lab_k1[,1:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_post <- mask_lab[,1:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_post <- lab_k1

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
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_pre <- get_mask(vit_k1, 3:ncol(vit_k1), 1)
  mask_last <- mask_vit_pre[nrow(mask_vit_pre),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1,2),drop=T]
  vit_k1 <- fill(vit_k1, 3:ncol(vit_k1), 1, get_type(stat_vit), d_vit$fill1, d_vit$fill2, stat_vit)
  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # dim(vit_k1)
  # colnames(vit_k1)
  vit_pre <- vit_k1[,-c(13:15),drop=F]
  mask_vit_pre <- mask_vit_pre[,-c(13:15),drop=F]

  vit_pre <- vit_pre %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_pre$art_mbp <- (vit_pre$art_sbp + 2 * vit_pre$art_dbp) / 3
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
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_in <- get_mask(vit_k1, 3:ncol(vit_k1), 1)

  index <- which(mask_vit_in[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 2] <- vit_last[index]
  }
  mask_last <- mask_vit_in[nrow(mask_vit_in),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1,2),drop=T]


  vit_k1 <- fill(vit_k1, 3:ncol(vit_k1), 1, get_type(stat_vit1), d_vit$fill1, d_vit$fill2, stat_vit1)
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
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_post <- get_mask(vit_k1, 3:ncol(vit_k1), 1)

  index <- which(mask_vit_post[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 2] <- vit_last[index]
  }

  vit_k1 <- fill(vit_k1, 3:ncol(vit_k1), 1, get_type(stat_vit), d_vit$fill1, d_vit$fill2, stat_vit)
  vit_k1 <- fill_last_values(vit_k1, mask_vit_post, 2:ncol(vit_k1), 1, d_vit)

  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # dim(vit_k1)
  # colnames(vit_k1)
  vit_post <- vit_k1[,-c(13:15),drop=F]
  mask_vit_post <- mask_vit_post[,-c(13:15),drop=F]

  vit_post <- vit_post %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # names(vit_post)
  # vit_post <- vit_post[,c(1,7,10:12,14,18,23,34,36:79),drop=F]
  mask_vit_post <- mask_vit_post %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # mask_vit_post <- mask_vit_post[,c(1,7,10:12,14,18,23,34,36:79),drop=F]

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

#   names(operation_row)
  x_s <- as.matrix(operation_row[,c(6,7,8,9,11,12,29,37,41:63),drop=F])
  # x_s <- norm_num(x_s,1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- to_onehot(x_s, 1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- x_s %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  oper_info <- operations_derived[which(operations_derived$op_id == cur_opid),c(31,32,33,34),drop=F]
  ind <- which(is.na(oper_info[1,,drop=T]))
  if(length(ind) > 0) oper_info[1, ind] <- 0

  t_list <- sort(unique(c(lab_pre$time, lab_post$time, vit_pre$time,vit_in$time,vit_post$time)))
  t_list <- setdiff(t_list, c(0))
  t_list_ds <- data.frame("time"=t_list)
  key_t_ds <- data.frame(t_start = t_start, t_end = t_end, t_out_24 = t_out + 24 * 60, t_in = t_in, t_out = t_out)
  # dim(vit_post)
  # dim(t_list)
  
  return(list(lab_pre, mask_lab_pre, lab_post, mask_lab_post, 
              vit_pre, mask_vit_pre, vit_in, mask_vit_in, vit_post, mask_vit_post, 
              x_s, oper_info,
              t_list_ds, key_t_ds
              ))
}

# datas <- get_1opid_allAlign(which(ds_id$op_id==423695004))
# datas[[1]][1:2,]
# datas[[14]][1:2,]
# length(datas)
# ds_id[1:4,]

process_data <- function(k, root_path) {
    # k<-232
    id_k<-ds_id$op_id[k]
    folder_path<-file.path(root_path, id_k)
    create_dir(folder_path, F)
    
    datas <- get_1opid_allAlign(k)

    fwrite(datas[[1]], file=file.path(folder_path, "lab_pre.csv"), row.names=F)
    fwrite(datas[[2]], file=file.path(folder_path, "mask_lab_pre.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "lab_post.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "mask_lab_post.csv"), row.names=F)

    fwrite(datas[[5]], file=file.path(folder_path, "vit_pre.csv"), row.names=F)
    fwrite(datas[[6]], file=file.path(folder_path, "mask_vit_pre.csv"), row.names=F)
    fwrite(datas[[7]], file=file.path(folder_path, "vit_in.csv"), row.names=F)
    fwrite(datas[[8]], file=file.path(folder_path, "mask_vit_in.csv"), row.names=F)
    fwrite(datas[[9]], file=file.path(folder_path, "vit_post.csv"), row.names=F)
    fwrite(datas[[10]], file=file.path(folder_path, "mask_vit_post.csv"), row.names=F)

    fwrite(datas[[11]], file=file.path(folder_path, "x_s.csv"), row.names=F)
    fwrite(datas[[12]], file=file.path(folder_path, "oper_info.csv"), row.names=F)
    fwrite(datas[[13]], file=file.path(folder_path, "t_list.csv"), row.names=F)
    fwrite(datas[[14]], file=file.path(folder_path, "key_t.csv"), row.names=F)
}

root_path <- "/home/luojiawei/pengxiran_project_data/全程测试数据/"
# create_dir(root_path, T)


k <- which(ds_id$op_id == 401675481)
process_data(k, root_path = root_path)


j1 <- 1
for(j0 in 2:ncol(mask_vit_post)){
  ind <- which(mask_vit_post[,j0] == 1)
  first_ind <- 1
  if(length(ind) >= 1) {
    first_ind <- ind[length(ind)]
  }
  if(d_vit$fill[j1] == "zero"){
    vit_k1[first_ind:nrow(vit_k1),j0] <- "0"
  }
  j1 <- j1 + 1
}
vit_k1[1:5,]



# -----------------样本筛选---------------------


get_1opid_allAlign <- function(k) {
  # k <- 189
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
                              seq(t0, t_in, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              keepNArow = F)
  mask_lab <- get_mask(lab_k1, 3:ncol(lab_k1), 1)
  mask_last <- mask_lab[nrow(mask_lab),-1,drop=T]
  lab_last <- lab_k1[nrow(lab_k1),-1,drop=T]

  lab_k1 <- lab_k1[,1:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_pre <- mask_lab[,1:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_pre <- lab_k1

  lab_k <- labs[labs$subject_id == cur_sid, ]
  lab_k1 <- resample_single_long(lab_k,
                              d_lab$itemid,
                              d_lab$value_type,
                              d_lab$agg_f,
                              seq(t_out, t1, 5),
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              keepNArow = F)
  mask_lab <- get_mask(lab_k1, 3:ncol(lab_k1), 1)

  index <- which(mask_lab[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      lab_k1[1, index + 1] <- lab_last[index]
  }

  lab_k1 <- lab_k1[,1:ncol(lab_k1),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_lab_post <- mask_lab[,1:ncol(mask_lab),drop=F] %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  # dim(lab_k1)
  lab_post <- lab_k1

  vit_k <- rbind(vitals[vitals$op_id == cur_opid, 2:5,drop=F], ward_vitals[ward_vitals$subject_id == cur_sid,,drop=F])

  vit_k1 <- resample_single_long(vit_k,
                              d_vit$itemid,
                              d_vit$value_type,
                              d_vit$agg_f,
                              seq(t0, t_in-5, 5), # t_start - 60*8
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              keepNArow = F,
                              keep_first = F)
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_pre <- get_mask(vit_k1, 3:ncol(vit_k1), 1)
  mask_last <- mask_vit_pre[nrow(mask_vit_pre),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1,2),drop=T]

  # vit_k1 <- norm_num(vit_k1, 2:ncol(vit_k1), 1, get_type(stat_vit), stat_vit)
  # dim(vit_k1)
  # colnames(vit_k1)
  vit_pre <- vit_k1[,-c(14:16),drop=F]
  mask_vit_pre <- mask_vit_pre[,-c(14:16),drop=F]

  vit_pre <- vit_pre %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_pre$art_mbp <- (vit_pre$art_sbp + 2 * vit_pre$art_dbp) / 3
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
                              keepNArow = F)
  
  X <- vit_k1[,c(1,3:ncol(vit_k1)), drop=F]
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_in <- get_mask(vit_k1, 3:ncol(vit_k1), 1)

  index <- which(mask_vit_in[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 2] <- vit_last[index]
  }
  mask_last <- mask_vit_in[nrow(mask_vit_in),-1,drop=T]
  vit_last <- vit_k1[nrow(vit_k1),-c(1,2),drop=T]

  vit_in <- vit_k1[,-c(14:16),drop=F]
  mask_vit_in <- mask_vit_in[,-c(14:16),drop=F]

  vit_in <- vit_in %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  vit_in$art_mbp <- (vit_in$art_sbp + 2 * vit_in$art_dbp) / 3
  # names(vit_in)
  vit_in <- vit_in[,c(1:73),drop=F]
  mask_vit_in <- mask_vit_in %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  mask_vit_in <- mask_vit_in[,c(1:73),drop=F]


  vit_k1 <- resample_single_long(vit_k,
                              d_vit$itemid,
                              d_vit$value_type,
                              d_vit$agg_f,
                              seq(t_out+5, t1, 5), # t_start - 60*8
                              "item_name",
                              "value",
                              "chart_time",
                              5,
                              keepNArow = F,
                              keep_first = F)
  vit_k1[,11] <- ifelse(is.na(vit_k1[,14]), vit_k1[,11], vit_k1[,14])
  vit_k1[,12] <- ifelse(is.na(vit_k1[,15]), vit_k1[,12], vit_k1[,15])
  vit_k1[,13] <- ifelse(is.na(vit_k1[,16]), vit_k1[,13], vit_k1[,16])
  mask_vit_post <- get_mask(vit_k1, 3:ncol(vit_k1), 1)

  index <- which(mask_vit_post[1, -1 ,drop=T] != "1")
  if(length(index) > 0) {
      vit_k1[1, index + 2] <- vit_last[index]
  }

  # dim(vit_k1)
  # colnames(vit_k1)
  vit_post <- vit_k1[,-c(14:16),drop=F]
  mask_vit_post <- mask_vit_post[,-c(14:16),drop=F]

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

#   names(operation_row)
  x_s <- as.matrix(operation_row[,c(6,7,8,9,11,12,29,37,41:63),drop=F])
  # x_s <- norm_num(x_s,1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- to_onehot(x_s, 1:ncol(x_s), NULL, get_type(stat_static), stat_static)
  x_s <- x_s %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  oper_info <- operations_derived[which(operations_derived$op_id == cur_opid),c(31,32,33,34),drop=F]
  ind <- which(is.na(oper_info[1,,drop=T]))
  if(length(ind) > 0) oper_info[1, ind] <- 0

  t_list <- sort(unique(c(lab_pre$time, lab_post$time, vit_pre$time,vit_in$time,vit_post$time)))
  t_list <- setdiff(t_list, c(0))
  t_list_ds <- data.frame("time"=t_list)
  key_t_ds <- data.frame(t_start = t_start, t_end = t_end, t_end_24 = t_end + 24 * 60, t_in = t_in, t_out = t_out)
  # dim(vit_post)
  # dim(t_list)

  y_mat <- list()
  # names(X)
  u <- apply(X[,c("art_mbp","nibp_mbp")], 1, function(x) Min(x, na.rm=T))
  u1 <- vit_in$art_mbp
  m <- mean(vit_pre$art_mbp, na.rm=T)
  u1 <- ifelse(u1 < 0.8*m, 1, ifelse(u1 < 1.2*m, 0, 2)) # 术前平均m, 1.2*m-high , 0.8*m low
  u1[is.na(u)] <- NA
  y_mat[[1]] <- cbind("mbp" = lead(u1))

  u <- apply(X[,c("art_sbp","nibp_sbp")], 1, function(x) Min(x, na.rm=T))
  u1 <- vit_in$art_sbp
  m <- mean(vit_pre$art_sbp, na.rm=T)
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
  m <- mean(vit_pre$hr, na.rm=T)
  u <- ifelse(u < 0.8*m, 1, ifelse(u < 1.2*m, 0, 2)) # 术前平均m, 1.2*m-high , 0.8*m low
  y_mat[[8]] <- cbind("hr" = lead(u))
  u <- lead(ifelse(X[,"spo2",drop=T] < 90, 1, 0))
  y_mat[[9]] <- cbind("spo2" = u)

  y_mat <- do.call(cbind, y_mat)

  y_mask <- rbind(apply(y_mat, 2, function(x) ifelse(is.na(x), 0, 1)))
  y_mask <- y_mask %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  y_mat <- rbind(apply(y_mat, 2, function(x) ifelse(is.na(x), 0, x)))
  y_mat <- y_mat %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  # names(static_outcome_df)
  y_static <- static_outcome_df[which(static_outcome_df$op_id == cur_opid),4:ncol(static_outcome_df)]
  y_mask1 <- rbind(apply(y_static, 2, function(x) ifelse(is.na(x), 0, 1)))
  y_mask1 <- y_mask1 %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame
  y_static <- rbind(apply(y_static, 2, function(x) ifelse(is.na(x), 0, x)))
  y_static <- y_static %>% as.data.frame %>% lapply(., as.numeric) %>% as.data.frame

  return(list(lab_pre, mask_lab_pre, lab_post, mask_lab_post, 
              vit_pre, mask_vit_pre, vit_in, mask_vit_in, vit_post, mask_vit_post, 
              x_s, oper_info,
              t_list_ds, key_t_ds, y_mat, y_mask, y_static, y_mask1
              ))
}

# datas <- get_1opid_allAlign(which(ds_id$op_id==423695004))
# datas[[6]][1:2,]
# datas[[14]][1:2,]
# length(datas)
# ds_id[1:4,]

process_data <- function(k, root_path) {
    # k<-232
    id_k<-ds_id$op_id[k]
    folder_path<-file.path(root_path, id_k)
    create_dir(folder_path, F)
    
    datas <- get_1opid_allAlign(k)

    fwrite(datas[[1]], file=file.path(folder_path, "lab_pre.csv"), row.names=F)
    fwrite(datas[[2]], file=file.path(folder_path, "mask_lab_pre.csv"), row.names=F)
    fwrite(datas[[3]], file=file.path(folder_path, "lab_post.csv"), row.names=F)
    fwrite(datas[[4]], file=file.path(folder_path, "mask_lab_post.csv"), row.names=F)

    fwrite(datas[[5]], file=file.path(folder_path, "vit_pre.csv"), row.names=F)
    fwrite(datas[[6]], file=file.path(folder_path, "mask_vit_pre.csv"), row.names=F)
    fwrite(datas[[7]], file=file.path(folder_path, "vit_in.csv"), row.names=F)
    fwrite(datas[[8]], file=file.path(folder_path, "mask_vit_in.csv"), row.names=F)
    fwrite(datas[[9]], file=file.path(folder_path, "vit_post.csv"), row.names=F)
    fwrite(datas[[10]], file=file.path(folder_path, "mask_vit_post.csv"), row.names=F)

    fwrite(datas[[11]], file=file.path(folder_path, "x_s.csv"), row.names=F)
    fwrite(datas[[12]], file=file.path(folder_path, "oper_info.csv"), row.names=F)
    fwrite(datas[[13]], file=file.path(folder_path, "t_list.csv"), row.names=F)
    fwrite(datas[[14]], file=file.path(folder_path, "key_t.csv"), row.names=F)

    fwrite(datas[[15]], file=file.path(folder_path, "y_mat.csv"), row.names=F)
    fwrite(datas[[16]], file=file.path(folder_path, "y_mask.csv"), row.names=F)
    fwrite(datas[[17]], file=file.path(folder_path, "y_static.csv"), row.names=F)
    fwrite(datas[[18]], file=file.path(folder_path, "y_mask1.csv"), row.names=F)
}

names(operations_derived)
ds_id <- merge(ds_id, operations_derived[,c(1,11,12)],by="op_id", all.x=T)

ds_id[1:2,]
names(ds_id)
ds_id[ds_id$in_hospital_death == 1 & 
        apply(ds_id[,c(50:54)], 1, function(x) sum(x, na.rm=T)) >=1 & 
        ds_id$emop == 0 & 
        ds_id$AKI_2 == 1,][6:10,1:4]

root_path <- "/home/luojiawei/pengxiran_project_data/样本筛选/"
# create_dir(root_path, T)


k <- which(ds_id$op_id == 482689915)
process_data(k, root_path = root_path)






