import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

from dataloader import INSPIRE
from model import *

from tqdm import tqdm
import numpy as np
import json
import random

from sklearn.metrics import accuracy_score, recall_score, \
                            precision_score, f1_score, roc_auc_score, \
                            average_precision_score

dataset_tr = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [1],
                    cls_col = None,
                    cls = None
                    )

dataset_va = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [3],
                    cls_col = None,
                    cls = None
                    )

# -------  模型参数 -----------
input_size_list = [112+111, 23]
hidden_size_list = [512, 32]
output_size_list = [3,3,4,1,1,1,3,3,1,4,4,4,3,3] + [1 for _ in range(34)] + [4, 6]
type_list = ["cat" for _ in range(len(output_size_list))]
model_list = ["lstm","mlp"]

# -------- 训练参数 ------------
EPOCHS = 100
lr = 0.001
weight_decay = 0.001
batch_size = 200
best_loss = float("inf")
no_improvement_count = 0
max_iter_train = 20
max_iter_val = 10
tol_count = 10

device = torch.device("cuda:1")

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
model = model.to(device)
model.load_state_dict(torch.load("/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_mt1.pth"))

optimizer = optim.Adam(model.parameters(), 
                       lr=lr, 
                       weight_decay=weight_decay)

with open('output_weight.json', 'r') as f:
    weight_list = json.load(f)

loss_functions = []
for output_size, data_type, weight in zip(output_size_list, type_list, weight_list):
    if output_size == 1:
        if data_type == "num":
            loss_functions.append(nn.MSELoss())
        elif data_type == "cat":
            loss_functions.append(nn.BCEWithLogitsLoss(weight=torch.tensor(weight[1:2]).to(device)))
    elif output_size > 1 and data_type == "cat":
        loss_functions.append(nn.CrossEntropyLoss(weight=torch.tensor(weight).to(device)))

print("开始训练...")

for epoch in range(EPOCHS):
    running_loss = 0.0
    data_loader = dataset_tr.iterate_batch(batch_size)
    
    counter = 1
    while True:
        # 如果 counter 超过了最大迭代次数，跳出循环，进入下一个 epoch
        if counter > max_iter_train:
            break
        try:
            data_batch, ids = next(data_loader)
        except StopIteration:
            # 当 data_loader 迭代完毕后，重置它
            data_loader = data_loader.iterate_batch(batch_size)
            data_batch, ids = next(data_loader)
            break

        running_loss = 0.0
        # 初始化包含n个空列表的大列表，n为任务数量
        loss_list_total = [[] for _ in range(len(loss_functions))]
        n = 0
        for i in tqdm(range(len(data_batch))):

            datas = data_batch[i]
            
            X, mask, t_list, time, x_s, y_mat, y_mask, y_static, y_mask1 = datas
            X = torch.cat([X, mask], dim=1)
            X = X.unsqueeze(0).to(device)
            x_s = x_s.to(device)
            y_mat = y_mat.to(device)
            y_mask = y_mask.to(device)
            y_static = y_static.to(device)
            y_mask1 = y_mask1.to(device)

            if t_list.shape[0] < 5:
                continue

            for j in range(1, t_list.shape[0]):
                
                if random.random() < 0.5:
                    continue

                ind = torch.where(time < t_list[j,0])[0]
                ind1 = torch.where(time == t_list[j,0])[0]
                if t_list[j,0] - time[ind[-1]] > 60 * 12 or len(ind1) == 0:
                    continue
                yhat_list = model([X[:,ind, ], x_s])
                for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[:9], loss_functions[:9], output_size_list[:9], type_list[:9])):
                    if type == "cat":
                        if output_size > 1:
                            loss = loss_fn(yhat, y_mat[ind1][0,k:(k+1)].to(torch.int64))
                        else:
                            loss = loss_fn(yhat, y_mat[ind1][:,k:(k+1)])
                    else:
                        loss = loss_fn(yhat, y_mat[ind1][:,k:(k+1)])
                    # 将loss添加到对应的列表中
                    loss_list_total[k].append(loss * y_mask[ind1][0, k])

            # 处理静态特征的loss
            ind = torch.where(time < t_list[-1,0])[0]
            yhat_list = model([X[:,ind, ], x_s])
            for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[9:], loss_functions[9:], output_size_list[9:], type_list[9:])):
                if type == "cat":
                    if output_size > 1:
                        loss = loss_fn(yhat, y_static[0,k:(k+1)].to(torch.int64))
                    else:
                        loss = loss_fn(yhat, y_static[:,k:(k+1)])
                else:
                    loss = loss_fn(yhat, y_static[:,k:(k+1)])
                # 将loss添加到对应的列表中
                loss_list_total[9 + k].append(loss * y_mask1[0, k])

        # 计算每个任务的平均loss并汇总
        loss_batch = [torch.stack(loss_list).mean() for loss_list in loss_list_total]
        loss = torch.stack(loss_batch).mean()
        running_loss += loss.cpu().item()
        
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        print_info = "Epoch: {}, counter: {}, Loss: {:.6f}".format(epoch, counter, running_loss)
        for i, loss in enumerate(loss_batch):
            print_info += ", Task {} Loss: {:.6f}".format(i, loss.cpu().item())
        print(print_info)

        counter += 1

    # 每轮 epoch 完了以后评估测试集表现，若 loss_val 有改善，则保存模型参数
    # 若连续两轮 loss_val 都没有改善，则终止训练
    # 在每轮 epoch 完成后，计算测试集上的 loss
    print("开始在验证集上测试...")

    with torch.no_grad():
        # 在循环开始前初始化列表
        data_val_loader = dataset_va.iterate_batch(batch_size)
        counter = 1
        while True:
            if counter > max_iter_val:
                break
            try:
                data_batch_val, ids_val = next(data_val_loader)
            except StopIteration:
                break
            for i in tqdm(range(len(data_batch_val))):
                datas_val = data_batch_val[i]

                X, mask, t_list, time, x_s, y_mat, y_mask, y_static, y_mask1 = datas_val
                X = torch.cat([X, mask], dim=1)
                X = X.unsqueeze(0).to(device)
                x_s = x_s.to(device)
                y_mat = y_mat.to(device)
                y_mask = y_mask.to(device)
                y_static = y_static.to(device)
                y_mask1 = y_mask1.to(device)

                if t_list.shape[0] < 5:
                    continue

                for j in range(3, t_list.shape[0]):
                    ind = torch.where(time < t_list[j,0])[0]
                    yhat_list = model([X[:,ind, ], x_s])
                    ind1 = torch.where(time == t_list[j,0])[0]
                    if t_list[j,0] - time[ind[-1]] > 60 * 12 or len(ind1) == 0:
                        continue
                    
                    for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[:9], loss_functions[:9], output_size_list[:9], type_list[:9])):
                        if type == "cat":
                            if output_size > 1:
                                loss = loss_fn(yhat, y_mat[ind1][0,k:(k+1)].to(torch.int64))
                            else:
                                loss = loss_fn(yhat, y_mat[ind1][:,k:(k+1)])
                        else:
                            loss = loss_fn(yhat, y_mat[ind1][:,k:(k+1)])
                        # 将loss添加到对应的列表中
                        if y_mask[ind1][0, k] == 1:
                            loss_list_total[k].append(loss)

                # 处理静态特征的loss
                ind = torch.where(time < t_list[-1,0])[0]
                yhat_list = model([X[:,ind, ], x_s])
                for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[9:], loss_functions[9:], output_size_list[9:], type_list[9:])):
                    if type == "cat":
                        if output_size > 1:
                            loss = loss_fn(yhat, y_static[0,k:(k+1)].to(torch.int64))
                        else:
                            loss = loss_fn(yhat, y_static[:,k:(k+1)])
                    else:
                        loss = loss_fn(yhat, y_static[:,k:(k+1)])
                    # 将loss添加到对应的列表中
                    if y_mask1[0, k] == 1:
                        loss_list_total[9 + k].append(loss)

            counter += 1

        # 计算每个任务的平均loss并汇总
        loss_batch = [torch.stack(loss_list).mean() for loss_list in loss_list_total]
        loss = torch.stack(loss_batch).mean()
        running_loss += loss.cpu().item()
        
        print("Valid loss: {:.6f}".format(running_loss))

        # 如果 AUC 有改善，则保存模型参数
        if loss.cpu().item() < best_loss:
            best_loss = loss.cpu().item()
            torch.save(model.state_dict(), "/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_mt1.pth")
            no_improvement_count = 0
            print("参数已更新")
        else:
            no_improvement_count += 1
        
        # 若连续两轮 AUC 都没有改善，则终止训练
        if no_improvement_count == tol_count:
            break