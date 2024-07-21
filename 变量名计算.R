
library(data.table)
library(magrittr)

ds_id <- fread("/home/luojiawei/pengxiran_project_data/ds_id.csv") %>% as.data.frame

ds_id[1:4,]

folder <- paste0("/home/luojiawei/pengxiran_project_data/all_op_id1/", ds_id$op_id[1])

list.files(folder)

load("/home/luojiawei/pengxiran_project/stats.RData")

fn <- c()
ds <- read.csv(file.path(folder, "lab_pre.csv"))
fn <- c(fn, paste0(names(ds), "_pre"), paste0("mask_",names(ds), "_pre"))

ds <- read.csv(file.path(folder, "x_s.csv"))
fn <- c(fn, names(ds))
fn[85:97] <- paste0("Surgery_", stat_static$type_name[[4]])

write.csv(fn, file="fn1.csv", row.names=F)


fn <- c()
ds <- read.csv(file.path(folder, "vit_pre.csv"))
fn <- c(fn, paste0(names(ds)[-1], "_pre"), paste0("mask_",names(ds)[-1], "_pre"))

write.csv(fn, file="fn2.csv", row.names=F)

fn <- c()
ds <- read.csv(file.path(folder, "vit_in.csv"))
fn <- c(fn, paste0(names(ds)[-1], "_in"), paste0("mask_",names(ds)[-c(1, ncol(ds))], "_in"))

write.csv(fn, file="fn3.csv", row.names=F)

fn <- c()
ds <- read.csv(file.path(folder, "vit_post.csv"))
fn <- c(fn, paste0(names(ds)[-1], "_post"), paste0("mask_",names(ds)[-c(1, ncol(ds))], "_post"))

write.csv(fn, file="fn4.csv", row.names=F)


fn <- c()
ds <- read.csv(file.path(folder, "lab_post.csv"))
fn <- c(fn, paste0(names(ds), "_post"), paste0("mask_",names(ds), "_post"))

ds <- read.csv(file.path(folder, "oper_info.csv"))
fn <- c(fn, paste0(names(ds), "_post"), paste0("mask_",names(ds), "_post"))

write.csv(fn, file="fn5.csv", row.names=F)









