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

operation[1:2,]

# operation$icd10_pcs

# 筛选特定的手术代码
selected_operations <- operation[grep("^0FB0", operation$icd10_pcs), ]

selected_operations[1:2,]

dim(selected_operations)

table(selected_operations$death_30d)

library(dplyr)

# 计算每个 type_name 分组的 death_30d 率
death_rate_by_type <- operation %>%
  group_by(type_name) %>%
  summarise(death_30d_rate = mean(death_30d, na.rm = TRUE))

# 查看结果
print(death_rate_by_type)

library(ggplot2)
# 创建横向柱形图
p <- ggplot(death_rate_by_type, aes(x = reorder(type_name, -death_30d_rate), y = death_30d_rate, fill = type_name)) +
  geom_bar(stat = "identity", position = position_dodge(), fill = "lightblue") +
  coord_flip() +  # Make the bar chart horizontal
  labs(title = "In-hospital Mortality Rate by Surgery Type", x = "Surgery Type", y = "In-hospital Mortality Rate") +
  theme_minimal(base_family = "sans") +  # Use a minimal theme
  theme(panel.background = element_rect(fill = "white"),  # Set the panel background to white
        plot.background = element_rect(fill = "white", color = "white"),  # Set the entire plot background to white
        axis.text.x = element_text(size = 14),  # Increase xtick label font size
        axis.text.y = element_text(size = 14),  # Increase ytick label font size
        axis.title.x = element_text(size = 16),  # Increase xlab font size
        axis.title.y = element_text(size = 16))  # Increase ylab font size
ggsave("/home/luojiawei/pengxiran_project/结果文件夹/surgery_type_death_rate.png", plot = p, width = 10, height = 8, units = "in")


names(operation)

ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv")
names(ds_id)
operation <- merge(operation, ds_id[,c(1,4,48:52)], by="op_id", all.x=T)

operation[operation$set==2 & operation$death_30d==0 & operation$type_name=="Heart_and_Great_Vessels", ][,c(1:4,12)]

operation$icd10_pcs[1:10]

names(operation)
unique(operation$type_name)
operation[operation$set==2 & 
            operation$death_30d==1 & 
            # operation$type_name=="Urinary_and_Reproductive_Systems" & 
            operation$asa %in% c(1,2) & 
            operation$emop ==0 & 
            operation$op_duration > 40, ][,c(1,31,37)]

operation[operation$set==2 & 
            grepl("^0FT", operation$icd10_pcs) & 
            apply(operation[,42,drop=F], 1, function(x) sum(x,na.rm=T)) > 0 &
            apply(operation[,c(40,41,43),drop=F], 1, function(x) sum(x,na.rm=T)) == 0 & 
            # operation$type_name=="Gastrointestinal_System" & 
            operation$op_duration > 40, ][,c(1,31,37)]

operation[operation$set==2 & 
            # grepl("^Z90", operation$icd10_pcs) & 
            apply(operation[,42,drop=F], 1, function(x) sum(x,na.rm=T)) > 0 &
            apply(operation[,c(40,41,43),drop=F], 1, function(x) sum(x,na.rm=T)) == 0 & 
            operation$type_name=="Central_Nervous_System_and_Cranial_Nerves" & 
            
            operation$op_duration > 40, ][,c(1,31,37)]


operation[1:4,39:43]

operation[operation$set==2 & operation$death_30d==1 & operation$emop==0, ][,c(1,31,37)]


tar_id <- operation$op_id[ !is.na(operation$AKI) & operation$set == 2 &
                             operation$death_30d == 0 & operation$asa >=4] %>% na.omit #  & operation$emop == 0 operation$AKI>=1 &
length(tar_id)

root_path <- "/home/luojiawei/pengxiran_project_data/all_op_id1/"


# names(x_s)

tar_id1 <- c()
for(i in 1:length(tar_id)){
  # i <- 7
  op_id <- tar_id[i]
  # x_s <- fread(file.path(root_path, op_id, "x_s.csv"), header = T)
  # lab_k <- fread(file.path(root_path, op_id, "lab_post.csv"), header = T)
  # lab_k1 <- fread(file.path(root_path, op_id, "lab_pre.csv"), header = T)
  tar_id1 <- c(tar_id1, op_id)
  # if (any(lab_k$creatinine >= 2.5) & all(lab_k1$creatinine <= 2) & x_s$Chronic_Kidney_Disease == 0) {tar_id1 <- c(tar_id1, op_id)}
}
tar_id1

names(operation)
operation[operation$op_id %in% tar_id1, c(1,11, 32,37,15)]



