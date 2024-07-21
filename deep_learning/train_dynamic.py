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

dataset_tr = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [1],
                    cls_col = None,
                    cls = None
                    )


dataset_va = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [3],
                    cls_col = None,
                    cls = None
                    )


# -------  模型参数 -----------
input_size_list = [36, 59, 59, 9]
hidden_size_list = [128, 128, 128, 32]
output_size_list = [3]
type_list = ["cat"]
model_list = ["mlp","biattlstm", "biattlstm", "mlp"]

# -------- 训练参数 ------------
EPOCHS = 100
lr = 0.001
weight_decay = 0.001
batch_size = 100
# best_loss = float("inf")
best_auc = 0.5
no_improvement_count = 0
max_iter_train = 10
max_iter_val = 5
tol_count = 10

device = torch.device("cuda:1")

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
model = model.to(device)

optimizer = optim.Adam(model.parameters(), 
                       lr=lr, 
                       weight_decay=weight_decay)

# loss_fn = nn.BCEWithLogitsLoss(weight = torch.tensor([0.1381]).to(device)) # weight=torch.tensor([0.995]).to(device)
loss_fn = nn.CrossEntropyLoss(weight=torch.tensor([0.0955,0.7463,0.1582]).to(device))

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
            # break

        running_loss = 0.0

        y_true, y_pred = [], []
        for i in tqdm(range(len(data_batch))):

            datas = data_batch[i]
            
            lab, vit_pre, mask_pre, vit_in, mask_in, t_list, x_s, \
                y_mat, y_mask, y_static, y_mask1 = datas
            
            lab = lab.to(device)
            vit_pre = vit_pre.unsqueeze(0).to(device)
            vit_in = vit_in.unsqueeze(0).to(device)
            x_s = x_s.to(device)

            y_mat = y_mat.to(device)
            y_mask = y_mask.to(device)

            last_y_mat = None  # 初始化上一次的 y_mat 值
            for j in range(2, y_mat.shape[0]):
                
                # if random.random() < 0.5:
                #     continue
                if y_mask[j, 0] == 0:
                    continue

                # 如果当前的 y_mat[j,1:2] 与上一次的 y_mat[j-1,1:2] 相同，则跳过当前循环
                current_y_mat = y_mat[j, 1:2]
                if last_y_mat is not None and torch.equal(current_y_mat, last_y_mat):
                    continue

                vit_in_ = vit_in[:,:j]
                yhat_list = model([lab, vit_pre, vit_in_, x_s])
                last_y_mat = current_y_mat  # 更新 last_y_mat 为当前的 y_mat

                y_pred.append(yhat_list[0])
                y_true.append(y_mat[j:(j+1),1:2])

        y_pred = torch.cat(y_pred, dim=0)
        if output_size_list[0] == 1:
            y_true = torch.cat(y_true, dim=0)
        else:
            y_true = torch.cat(y_true, dim=0).to(torch.int64).reshape(-1)

        loss = loss_fn(y_pred, y_true)
        running_loss += loss.cpu().item()
        
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        print_info = "Epoch: {}, counter: {}, Loss: {:.6f}".format(epoch, counter, running_loss)
        # for i, loss in enumerate(loss_batch):
        #     print_info += ", Task {} Loss: {:.6f}".format(i, loss.cpu().item())
        print(print_info)

        counter += 1

    # 每轮 epoch 完了以后评估测试集表现，若 loss_val 有改善，则保存模型参数
    # 若连续两轮 loss_val 都没有改善，则终止训练
    # 在每轮 epoch 完成后，计算测试集上的 loss
    print("开始在验证集上测试...")
    running_loss = 0.0
    y_true, y_pred = [], []

    with torch.no_grad():
        # 在循环开始前初始化列表
        data_loader = dataset_va.iterate_batch(100)
        counter = 1
        while True:
            if counter > 10:
                break
            try:
                data_batch, ids_val = next(data_loader)
            except StopIteration:
                break
            for i in tqdm(range(len(data_batch))):
                datas = data_batch[i]

                lab, vit_pre, mask_pre, vit_in, mask_in, t_list, x_s, \
                    y_mat, y_mask, y_static, y_mask1 = datas

                lab = lab.to(device)
                vit_pre = vit_pre.unsqueeze(0).to(device)
                vit_in = vit_in.unsqueeze(0).to(device)
                x_s = x_s.to(device)

                y_mat = y_mat.to(device)
                y_mask = y_mask.to(device)

                last_y_mat = None  # 初始化上一次的 y_mat 值
                for j in range(2, y_mat.shape[0]):
                    
                    # if random.random() < 0.5:
                    #     continue
                    if y_mask[j, 0] == 0:
                        continue

                    # 如果当前的 y_mat[j,1:2] 与上一次的 y_mat[j-1,1:2] 相同，则跳过当前循环
                    current_y_mat = y_mat[j, 1:2]
                    if last_y_mat is not None and torch.equal(current_y_mat, last_y_mat):
                        continue

                    vit_in_ = vit_in[:,:j]
                    yhat_list = model([lab, vit_pre, vit_in_, x_s])
                    last_y_mat = current_y_mat  # 更新 last_y_mat 为当前的 y_mat
                    
                    y_pred.append(yhat_list[0])
                    y_true.append(y_mat[j:(j+1),1:2])
            counter += 1

        y_pred = torch.cat(y_pred, dim=0)
        if output_size_list[0] == 1:
            y_true = torch.cat(y_true, dim=0)
        else:
            y_true = torch.cat(y_true, dim=0).to(torch.int64).reshape(-1)

        loss = loss_fn(y_pred, y_true)
        running_loss += loss.cpu().item()

        # 将模型输出转换为概率
        y_pred_prob = y_pred.cpu().numpy()
        y_true_np = y_true.cpu().numpy()
        
        # 计算AUC
        if output_size_list[0] == 1:
            auc = roc_auc_score(y_true_np, y_pred_prob)
        else:
            auc = roc_auc_score(y_true_np, y_pred_prob, average='macro', multi_class='ovr')
        print("Valid loss: {:.6f}, AUC: {:.6f}".format(running_loss, auc))

        # 如果 AUC 有改善，则保存模型参数
        if auc > best_auc:
            best_auc = auc  # 更新最佳AUC
            torch.save(model.state_dict(), "/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_sbp.pth")
            no_improvement_count = 0
            print("参数已更新")
        else:
            no_improvement_count += 1
        
        # 若连续两轮 AUC 都没有改善，则终止训练
        if no_improvement_count == tol_count:
            break