library(readxl)
library(pROC)
library(caret)


ds_nsqip <- read_excel("/home/luojiawei/pengxiran_project/结果文件夹/nsqip_241(2).xlsx",
                sheet=1, col_names = T)

names(ds_nsqip)

ds <- data.frame(op_id = ds_nsqip$op_id, 
                y_true = as.numeric(ds_nsqip[[6]]), 
                y_pred_prob_0 = as.numeric(ds_nsqip[[39]]),
                y_pred = as.numeric(ds_nsqip[[38]]))

ds <- data.frame(op_id = ds_nsqip$op_id, 
                y_true = ds_nsqip[[3]], 
                y_pred_prob_0 = as.numeric(ds_nsqip[[47]]),
                y_pred = factor(ifelse(as.numeric(ds_nsqip[[47]]) > 7, 1, 0)))
ds <- na.omit(ds)
roc(ds$y_true ~ ds$y_pred_prob_0)

write.csv(ds, file="/home/luojiawei/pengxiran_project/结果文件夹/模型性能/los_NSQIP.csv", row.names=F)


conf_mat <- caret::confusionMatrix(table(factor(ds$y_true),factor(ds$y_pred)),positive="1")
conf_mat$byClass[c(11,1,5,7)]
# names(conf_mat$byClass)

# 定义一个函数来计算所需的统计量
calc_stats <- function(ds_sample) {
  conf_mat <- caret::confusionMatrix(table(factor(ds_sample$y_true), factor(ds_sample$y_pred)), positive="1")
  return(conf_mat$byClass[c(11,1,5,7)])  # 返回平衡准确率、敏感性、精确度、F1 分数
}

# 手动实现 bootstrap
set.seed(123)
n <- nrow(ds)
R <- 50
results <- replicate(R, {
  indices <- sample(1:n, n, replace = TRUE)
  ds_sample <- ds[indices, ]
  calc_stats(ds_sample)
})

# 计算整个数据集的统计量
overall_stats <- calc_stats(ds)

# 计算 bootstrap 样本的 95% 置信区间
cis <- apply(results, 1, function(x) quantile(x, probs = c(0.025, 0.975)))

# 格式化输出结果，结合整个数据集的统计量和置信区间
formatted_results <- sapply(1:length(overall_stats), function(i) {
  sprintf("%.4f(%.4f,%.4f)", overall_stats[i], cis[1, i], cis[2, i])
})
names(formatted_results) <- c("Balanced Accuracy", "Sensitivity", "Precision", "F1")

# 打印格式化后的结果
print(formatted_results)


