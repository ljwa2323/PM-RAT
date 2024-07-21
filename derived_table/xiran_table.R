

#  新增变量 BMI, 麻醉持续时间，手术持续时间，手术室持续时间，ICU持续时间，CPB持续时间

setwd("/home/luojiawei/pengxiran_project/")

library(data.table)
library(dplyr)
library(magrittr)
# library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)
library(readxl)

operation <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/operations.csv",
                    header=T)

operation[1:4,]

lab <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/labs.csv",header=T)

lab[1:5,]



# 确保operation和lab都是data.table对象
setDT(operation)
setDT(lab)

# 定义处理单个手术记录的函数
process_operation <- function(op_info) {
  subject_id <- op_info$subject_id
  opstart_time <- op_info$opstart_time
  opend_time <- op_info$opend_time
  
  # 找到手术开始前的首次测量
  pre_op_idx <- lab[subject_id == subject_id & chart_time < opstart_time, .I[which.max(chart_time)]]
  pre_op <- if (length(pre_op_idx) > 0) {
    lab[pre_op_idx, .(subject_id, chart_time, item_name, value)]
  } else {
    NULL
  }
  
  # 找到手术结束后的第一次测量
  post_op_idx <- lab[subject_id == subject_id & chart_time > opend_time, .I[which.min(chart_time)]]
  post_op <- if (length(post_op_idx) > 0) {
    lab[post_op_idx, .(subject_id, chart_time, item_name, value)]
  } else {
    NULL
  }
  
  # 处理结果
  results <- list()
  if (!is.null(pre_op)) {
    time_difference_pre <- opstart_time - pre_op$chart_time
    if (time_difference_pre >= 0) {
      pre_op[, `:=`(op_id = op_info$op_id,
                    measurement_type = "术前最后一次",
                    time_difference = time_difference_pre,
                    opstart_time = op_info$opstart_time,
                    opend_time = op_info$opend_time)]
      results <- c(results, list(pre_op))
    }
  }
  
  if (!is.null(post_op)) {
    time_difference_post <- post_op$chart_time - opend_time
    if (time_difference_post >= 0) {
      post_op[, `:=`(op_id = op_info$op_id,
                     measurement_type = "术后第一次",
                     time_difference = time_difference_post,
                     opstart_time = op_info$opstart_time,
                     opend_time = op_info$opend_time)]
      results <- c(results, list(post_op))
    }
  }
  
  return(rbindlist(results))
}

# 定义块大小和输出路径
chunk_size <- 10000  # 根据你的内存大小调整这个值
output_path <- "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/lab_tables/"  # 修改为你的输出路径

# 计算块的数量
num_chunks <- ceiling(nrow(operation) / chunk_size)

# 对每个块进行处理
for (i in seq_len(num_chunks)) {
  # 计算当前块的索引
  idx <- ((i - 1) * chunk_size + 1):min(i * chunk_size, nrow(operation))
  
  # 提取当前块的operation数据
  operation_chunk <- operation[idx]
  
  # 使用mclapply并行处理当前块的数据
  results_list <- mclapply(1:nrow(operation_chunk), function(i) process_operation(operation_chunk[i,]), mc.cores = detectCores() - 1)
  
  # 合并结果
  results <- rbindlist(results_list, fill = TRUE)
  
  # 构建输出文件名，包含块的索引以保持顺序
  output_file_name <- sprintf("%s/results_chunk_%d.csv", output_path, i)
  
  # 将结果保存到CSV文件
  fwrite(results, output_file_name)
  
  # 触发垃圾回收
  gc()
  
  # 可选：打印当前进度
  print(paste("Chunk", i, "of", num_chunks, "processed and saved to", output_file_name))
}

# 设置文件夹路径
folder_path <- "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/lab_tables/"

# 获取文件夹内所有CSV文件的路径
files <- list.files(folder_path, full.names = TRUE, pattern = "\\.csv$")

# 读取并合并所有CSV文件
all_data <- rbindlist(lapply(files, fread))

# 查看合并后的数据的前几行
head(all_data)

all_data[1:4,]

all_data[all_data$time_difference <= 3 * 24 * 60, ]

write.csv(all_data, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/lab_summary.csv", row.names=F,
        fileEncoding='gbk')
write.csv(all_data, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/lab_summary_utf.csv", row.names=F)

all_data[1:10,]


# 假设 all_data 已经是一个 data.table 对象
# 筛选术前最后一次和术后第一次的行
pre_op_data <- all_data[measurement_type == "术前最后一次"]
post_op_data <- all_data[measurement_type == "术后第一次"]

# 对术前最后一次数据进行宽表格转换
wide_pre_op_data <- dcast(pre_op_data, op_id + subject_id + opstart_time + opend_time ~ item_name, value.var = "value")

# 对术后第一次数据进行宽表格转换
wide_post_op_data <- dcast(post_op_data, op_id + subject_id + opstart_time + opend_time ~ item_name, value.var = "value")

names(wide_pre_op_data)[5:41] <- paste0(names(wide_pre_op_data)[5:41],"_pre_op")
names(wide_post_op_data)[5:40] <- paste0(names(wide_post_op_data)[5:40],"_post_op")

lab_sum <- merge(operation[,c("op_id","opstart_time","opend_time")], wide_pre_op_data[,c(1,5:41)], by="op_id",all.x=T)
lab_sum <- merge(lab_sum, wide_post_op_data[,c(1,5:40)], by="op_id",all.x=T)

lab_sum[1:10,]
summary(lab_sum)

write.csv(lab_sum, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/lab_sum.csv", row.names=F,
                fileEncoding='gbk')



vit <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/vitals.csv",header=T)

vit[1:5,]


# 确保 vit 是 data.table 对象
setDT(vit)

# 调整 process_operation 函数以适应 vit 表格
process_vit <- function(op_info) {
  subject_id <- op_info$subject_id
  opstart_time <- op_info$opstart_time
  opend_time <- op_info$opend_time
  
  # 找到手术开始前的首次测量
  pre_op_idx <- vit[subject_id == subject_id & chart_time < opstart_time, .I[which.max(chart_time)]]
  pre_op <- if (length(pre_op_idx) > 0) {
    vit[pre_op_idx, .(subject_id, chart_time, item_name, value)]
  } else {
    NULL
  }
  
  # 找到手术结束后的第一次测量
  post_op_idx <- vit[subject_id == subject_id & chart_time > opend_time, .I[which.min(chart_time)]]
  post_op <- if (length(post_op_idx) > 0) {
    vit[post_op_idx, .(subject_id, chart_time, item_name, value)]
  } else {
    NULL
  }
  
  # 处理结果
  results <- list()
  if (!is.null(pre_op)) {
    time_difference_pre <- opstart_time - pre_op$chart_time
    if (time_difference_pre >= 0) {
      pre_op[, `:=`(op_id = op_info$op_id,
                    measurement_type = "术前最后一次",
                    time_difference = time_difference_pre,
                    opstart_time = op_info$opstart_time,
                    opend_time = op_info$opend_time)]
      results <- c(results, list(pre_op))
    }
  }
  
  if (!is.null(post_op)) {
    time_difference_post <- post_op$chart_time - opend_time
    if (time_difference_post >= 0) {
      post_op[, `:=`(op_id = op_info$op_id,
                     measurement_type = "术后第一次",
                     time_difference = time_difference_post,
                     opstart_time = op_info$opstart_time,
                     opend_time = op_info$opend_time)]
      results <- c(results, list(post_op))
    }
  }
  
  return(rbindlist(results))
}

# 使用相同的块处理逻辑处理 vit 表格
# 注意：确保调整 output_path 以避免覆盖 lab 的结果
output_path_vit <- "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/vit_tables/"

# 计算块的数量
num_chunks_vit <- ceiling(nrow(operation) / chunk_size)

# 对每个块进行处理
for (i in seq_len(num_chunks_vit)) {
  idx <- ((i - 1) * chunk_size + 1):min(i * chunk_size, nrow(operation))
  operation_chunk <- operation[idx]
  
  results_list_vit <- mclapply(1:nrow(operation_chunk), function(i) process_vit(operation_chunk[i,]), mc.cores = detectCores() - 1)
  
  results_vit <- rbindlist(results_list_vit, fill = TRUE)
  
  output_file_name_vit <- sprintf("%s/results_chunk_%d.csv", output_path_vit, i)
  
  fwrite(results_vit, output_file_name_vit)
  
  gc()
  
  print(paste("Chunk", i, "of", num_chunks_vit, "processed and saved to", output_file_name_vit))
}


# 设置 vit 文件夹路径
folder_path_vit <- "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/vit_tables/"

# 获取 vit 文件夹内所有CSV文件的路径
files_vit <- list.files(folder_path_vit, full.names = TRUE, pattern = "\\.csv$")

# 读取并合并所有 vit CSV文件
all_data_vit <- rbindlist(lapply(files_vit, fread))

all_data_vit[1:50,]

write.csv(all_data_vit, "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/all_data_vit.csv", row.names=F, 
fileEncoding = 'gbk')



# 假设 all_data_vit 已经是一个 data.table 对象
# 筛选术前最后一次和术后第一次的行
pre_op_data_vit <- all_data_vit[measurement_type == "术前最后一次"]
post_op_data_vit <- all_data_vit[measurement_type == "术后第一次"]

# 对术前最后一次数据进行宽表格转换
wide_pre_op_data_vit <- dcast(pre_op_data_vit, op_id + subject_id + opstart_time + opend_time ~ item_name, value.var = "value")

# 对术后第一次数据进行宽表格转换
wide_post_op_data_vit <- dcast(post_op_data_vit, op_id + subject_id + opstart_time + opend_time ~ item_name, value.var = "value")

# 调整列名以区分术前和术后
names(wide_pre_op_data_vit)[5:ncol(wide_pre_op_data_vit)] <- paste0(names(wide_pre_op_data_vit)[5:ncol(wide_pre_op_data_vit)],"_pre_op")
names(wide_post_op_data_vit)[5:ncol(wide_post_op_data_vit)] <- paste0(names(wide_post_op_data_vit)[5:ncol(wide_post_op_data_vit)],"_post_op")

wide_pre_op_data_vit[1:2,c(1,5)]
operation[1:2,]
operation[1:5,c("op_id","opstart_time","opend_time")]
wide_pre_op_data_vit[1:2,c(1,5:ncol(wide_pre_op_data_vit)),drop=F]

# 合并 operation 数据与转换后的 vit 数据
vit_sum <- merge(operation[,c("op_id","opstart_time","opend_time")], wide_pre_op_data_vit[,c(1,5:ncol(wide_pre_op_data_vit)),with=F], by="op_id",all.x=T)
vit_sum <- merge(vit_sum, wide_post_op_data_vit[,c(1,5:ncol(wide_post_op_data_vit)),with=F], by="op_id",all.x=T)


# 查看合并后的 vit 数据的前几行
head(vit_sum)

# 保存合并后的 vit 数据到 CSV 文件
write.csv(vit_sum, file="/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/xiran_tables/vit_sum.csv", row.names=F, fileEncoding='gbk')