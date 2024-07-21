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

operation[1:3,]

med <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/medications_derived.csv",header=T)

med[1:5,]

med$chart_time <- med$chart_time / 60



# 定义使用的药物列表
used_drugs_thrombosis <- c("iloprost", "urokinase", "kallidinogenase", "streptokinase/streptodornase")

# 定义使用的药物列表，针对呼吸系统并发症
used_drugs_respiratory <- c("isoproterenol", "ipratropium", "doxofylline", "salbutamol",
                            "tiotropium combipack", "ephedrine", "olodaterol/tiotropium",
                            "vilanterol/umeclidinium", "salmeterol/fluticasone", "bambuterol",
                            "salbutamol sulfate", "formoterol", "fenoterol", "acetylcysteine",
                            "acebrophylline", "bromhexine", "benproperine", "hederae helicis",
                            "erdosteine", "ambroxol", "beclomethasone/formoterol", "montelukast",
                            "aminophylline", "beclomethasone", "budesonide/formoterol",
                            "fluticasone furoate/vilanterol", "fluticasone furoate",
                            "indacaterol/glycopyrronium", "ciclesonide", "tiotropium refillpack",
                            "indacaterol", "acetyl cysteine")

# 定义使用的药物列表，针对术后精神神经异常
used_drugs_neurological <- c("gabapentin", "valproate", "valproic acid/valproate", "valproic acid",
                             "lacosamide", "topiramate", "clobazam", "phenytoin", "fosphenytoin",
                             "divalproex na", "oxcarbazepine", "perampanel", "zonisamide",
                             "carbamazepine", "lamotrigine", "vigabatrin", "pregabalin",
                             "levetiracetam", "rivastigmine", "galantamine", "donepezil",
                             "memantine", "theobromine", "buspirone", "etizolam",
                             "choline alfoscerate", "lithium carbonate", "quetiapine",
                             "haloperidol", "aripiprazole", "olanzapine", "risperidone",
                             "chlorpromazine", "amisulpride", "perphenazine", "mesalazine",
                             "methylphenidate", "modafinil", "amitriptyline", "fluoxetine",
                             "escitalopram", "sertraline", "nortriptyline", "mirtazapine",
                             "mirtazapine soltab", "duloxetine", "trazodone", "tianeptine",
                             "vortioxetine", "venlafaxine", "bupropion", "milnacipran",
                             "desvenlafaxine", "imipramine", "paroxetine", "fluvoxamine")


# 定义使用的药物列表，针对术后循环系统并发症
used_drugs_cardiovascular <- c("phenylephrine", "midodrine", "epinephrine", "norepinephrine",
                               "dopamine/dextrose", "calcium dobesilate", "teprenone",
                               "dopamine", "dobutamine/dextrose", "limaprost alfa-cyclodextrin clathrate",
                               "treprostinil", "alprostadil", "beraprost", "papaverine",
                               "nitroprusside", "nafronyl oxalate", "isosorbide dinitrate",
                               "isosorbide mononitrate", "nicorandil", "nitroglycerin",
                               "molsidomine", "ivabradine", "trimetazidine", "hydralazine",
                               "digoxin", "milrinone", "dobutamine", "amiodarone",
                               "adenosine", "propafenone", "flecainide", "sotalol")


# 定义使用的药物列表，针对术后恶心呕吐
used_drugs_ponv <- c("ramosetron", "palonosetron", "ondansetron", "metoclopramide",
                     "domperidone maleate", "aprepitant", "granisetron")



# 定义一个新的函数来检查所有并发症
check_all_conditions <- function(operation_row) {
  op_id <- operation_row$op_id
  orin_time <- as.numeric(operation_row$orin_time)
  subject_id <- operation_row$subject_id
  
  # 筛选出对应subject_id的med记录
  subject_med <- med[med$subject_id == subject_id, ]
  
  # 筛选出在手术开始后使用的药物记录
  post_op_med <- subject_med[as.numeric(subject_med$chart_time) > orin_time & as.numeric(subject_med$chart_time) < as.numeric(operation_row$discharge_time), ]
  
  # 检查是否使用了指定的药物，并返回一个命名向量
  conditions <- c(
    M_TE = any(used_drugs_thrombosis %in% post_op_med$drug_name),
    M_RS = any(used_drugs_respiratory %in% post_op_med$drug_name),
    M_NS = any(used_drugs_neurological %in% post_op_med$drug_name),
    M_CS = any(used_drugs_cardiovascular %in% post_op_med$drug_name),
    M_PONV = any(used_drugs_ponv %in% post_op_med$drug_name)
  )
  return(conditions)
}

# 使用mclapply并行处理每行，完成多个并发症的标注
results <- mclapply(1:nrow(operation), 
                      function(i) 
                        {
                          if (i %% 3000 == 0) {
                            print(i)
                          }
                          return(check_all_conditions(operation[i, ]))
                        }, 
                      mc.cores = detectCores() - 1)


# 将结果转换为适合添加到operation的格式
results_df <- do.call(rbind, results)
colnames(results_df) <- c("M_TE", "M_RS", "M_NS", "M_CS", "M_PONV")
results_df[1:4,]

operation[1:5,]
names(operation)

# 将结果添加到operation数据框中
med_outcome <- cbind(operation[,c(1,2,3,4)], results_df)

# 查看结果
head(med_outcome)

for(i in 5:9){
    med_outcome[[i]] <- ifelse(med_outcome[[i]], 1, 0)
}

table(med_outcome$M_TE)
table(med_outcome$M_RS)
table(med_outcome$M_NS)
table(med_outcome$M_CS)
table(med_outcome$M_PONV)

write.csv(med_outcome, file = "/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/med_outcome.csv", row.names = FALSE)

med_outcome <- fread("/home/luojiawei/pengxiran_project_data/inspire-a-publicly-available-research-dataset-for-perioperative-medicine-1.2/med_outcome.csv",header=T)

med_outcome[1:40,]
apply(med_outcome[,5:ncol(med_outcome)], 2, mean)




operation_row <- operation[1001,]
op_id <- operation_row$op_id
orin_time <- as.numeric(operation_row$orin_time)
subject_id <- operation_row$subject_id

# 筛选出对应subject_id的med记录
subject_med <- med[med$subject_id == subject_id, ]

# 筛选出在手术开始后使用的药物记录
post_op_med <- subject_med[as.numeric(subject_med$chart_time) > orin_time & as.numeric(subject_med$chart_time) < as.numeric(operation_row$discharge_time), ]

# 检查是否使用了指定的药物，并返回一个命名向量
conditions <- c(
  M_TE = any(used_drugs_thrombosis %in% post_op_med$drug_name),
  M_RS = any(used_drugs_respiratory %in% post_op_med$drug_name),
  M_NS = any(used_drugs_neurological %in% post_op_med$drug_name),
  M_CS = any(used_drugs_cardiovascular %in% post_op_med$drug_name),
  M_PONV = any(used_drugs_ponv %in% post_op_med$drug_name)
)

 conditions
