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

# -------  环境变量 -----------
y_name = "ALI_2"
y_index = 49
stage = "post"
device = torch.device("cuda:2")
pre_param = None
# aux_info = [{"Y_name": "AKI_2",
#              "ind" : 1,
#             "y_index": 46,
#             "y_true":[],
#             "y_pred":[],
#             "weight":0.1,
#             "y_dim" : 1,
#             "loss_fn" : nn.BCEWithLogitsLoss()}]

# -------  模型参数 -----------
# lab_pre/post 36 + 36, vit_pre 79 + 78, vit_in 71 * 2, vit_post 79 + 78, x_s 48, oper 4
input_size_list = [36 + 36 + 48, 79 + 78, 71 * 2, 79 + 78, 36 + 36 + 4]#, 71 * 2, 79 + 78, 36 + 36 + 4
hidden_size_list = [256, 256, 256, 256, 256]#, 256, 256, 256
output_size_list = [1]
type_list = ["cat"]
model_list = ["mlp", "biattlstm", "biattlstm", "biattlstm", "mlp"]#, "biattlstm", "biattlstm", "mlp"


print(f"task: {y_name}, stage: {stage}")

dataset_tr_0 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [1],
                    cls_col = y_name,
                    cls = [0],
                    stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

dataset_tr_1 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [1],
                    cls_col = y_name,
                    cls = [1],
                    stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

# dataset_tr_2 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
#                     "/home/luojiawei/pengxiran_project_data/ds_id.csv",
#                     id_col="op_id",
#                     mod_col="set",
#                     mod = [1],
#                     cls_col = y_name,
#                     cls = [2],
#                     stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
#                     )

# dataset_tr_3 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
#                     "/home/luojiawei/pengxiran_project_data/ds_id.csv",
#                     id_col="op_id",
#                     mod_col="set",
#                     mod = [1],
#                     cls_col = y_name,
#                     cls = [3],
#                     stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
#                     )

dataset_va_0 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [3],
                    cls_col = y_name,
                    cls = [0],
                    stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

dataset_va_1 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [3],
                    cls_col = y_name,
                    cls = [1],
                    stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

# dataset_va_2 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
#                     "/home/luojiawei/pengxiran_project_data/ds_id.csv",
#                     id_col="op_id",
#                     mod_col="set",
#                     mod = [3],
#                     cls_col = y_name,
#                     cls = [2],
#                     stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
#                     )

# dataset_va_3 = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
#                     "/home/luojiawei/pengxiran_project_data/ds_id.csv",
#                     id_col="op_id",
#                     mod_col="set",
#                     mod = [3],
#                     cls_col = y_name,
#                     cls = [3],
#                     stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
#                     )


# -------- 训练参数 ------------
EPOCHS = 100
lr = 0.001
weight_decay = 0.001
batch_size = 300
batch_size_val = 300
# best_loss = float("inf")
best_auc = 0.5
no_improvement_count = 0
max_iter_train = 10
max_iter_val = 10
tol_count = 10

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
if pre_param is not None:
    model.load_state_dict(torch.load(pre_param, map_location=device))
    print("Loaded pre-trained model parameters from", pre_param)
model = model.to(device)

optimizer = optim.Adam(model.parameters(), 
                       lr=lr, 
                       weight_decay=weight_decay)


if output_size_list[0] > 1:
    loss_fn = nn.CrossEntropyLoss().to(device)
else:
    loss_fn = nn.BCEWithLogitsLoss()

print("开始训练...")

for epoch in range(EPOCHS):
    print(f"task: {y_name}, stage: {stage}")
    running_loss = 0.0
    data_loader_0 = dataset_tr_0.iterate_batch(batch_size, normalize=True)
    data_loader_1 = dataset_tr_1.iterate_batch(batch_size, normalize=True)
    # data_loader_2 = dataset_tr_2.iterate_batch(batch_size, normalize=True)
    # data_loader_3 = dataset_tr_3.iterate_batch(batch_size, normalize=True)
    
    counter = 1
    while True:
        # 如果 counter 超过了最大迭代次数，跳出循环，进入下一个 epoch
        if counter > max_iter_train:
            break
        try:
            data_batch_0, ids_0 = next(data_loader_0)
        except StopIteration:
            data_loader_0 = dataset_tr_0.iterate_batch(batch_size, normalize=True)
            data_batch_0, ids_0 = next(data_loader_0)

        try:
            data_batch_1, ids_1 = next(data_loader_1)
        except StopIteration:
            # 当 data_loader 迭代完毕后，重置它
            data_loader_1 = dataset_tr_1.iterate_batch(batch_size, normalize=True)
            data_batch_1, ids_1 = next(data_loader_1)

        # try:
        #     data_batch_2, ids_2 = next(data_loader_2)
        # except StopIteration:
        #     # 当 data_loader 迭代完毕后，重置它
        #     data_loader_2 = dataset_tr_2.iterate_batch(batch_size, normalize=True)
        #     data_batch_2, ids_2 = next(data_loader_2)

        # try:
        #     data_batch_3, ids_3 = next(data_loader_3)
        # except StopIteration:
        #     # 当 data_loader 迭代完毕后，重置它
        #     data_loader_3 = dataset_tr_3.iterate_batch(batch_size, normalize=True)
        #     data_batch_3, ids_3 = next(data_loader_3)

        running_loss = 0.0
        # 初始化包含n个空列表的大列表，n为任务数量
        y_true, y_pred = [], []
        # for i1 in range(len(aux_info)):
        #     aux_info[i1]["y_pred"], aux_info[i1]["y_true"] = [],[]
        for i in tqdm(range(len(data_batch_0))):  # 遍历第一个数据批次

            datas = data_batch_0[i]
            
            lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
                vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
                t_list, x_s, oper_info, \
                y_mat, y_mask, y_static, y_mask1 = datas
            
            lab_pre = lab_pre.to(device)
            mask_lab_pre = mask_lab_pre.to(device)
            lab_post = lab_post.to(device)
            mask_lab_post = mask_lab_post.to(device)

            vit_pre = vit_pre.unsqueeze(0).to(device)
            mask_pre = mask_pre.unsqueeze(0).to(device)
            vit_in = vit_in.unsqueeze(0).to(device)
            mask_in = mask_in.unsqueeze(0).to(device)  
            vit_post = vit_post.unsqueeze(0).to(device)
            mask_post = mask_post.unsqueeze(0).to(device)            

            x_s = x_s.to(device)
            oper_info = oper_info.to(device)

            x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
            x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
            x_3 = torch.cat([vit_in, mask_in], dim=-1)
            x_4 = torch.cat([vit_post, mask_post], dim=-1)
            x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
            
            yhat_list = model([x_1,x_2,x_3,x_4,x_5])
            y_pred.append(yhat_list[0])
            y_true.append(y_static[:,y_index:(y_index+1)])
            # for i1 in range(len(aux_info)):
            #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
            #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        for i in tqdm(range(len(data_batch_1))):  # 遍历第二个数据批次

            datas = data_batch_1[i]
            
            lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
                vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
                t_list, x_s, oper_info, \
                y_mat, y_mask, y_static, y_mask1 = datas
            
            lab_pre = lab_pre.to(device)
            mask_lab_pre = mask_lab_pre.to(device)
            lab_post = lab_post.to(device)
            mask_lab_post = mask_lab_post.to(device)

            vit_pre = vit_pre.unsqueeze(0).to(device)
            mask_pre = mask_pre.unsqueeze(0).to(device)
            vit_in = vit_in.unsqueeze(0).to(device)
            mask_in = mask_in.unsqueeze(0).to(device)  
            vit_post = vit_post.unsqueeze(0).to(device)
            mask_post = mask_post.unsqueeze(0).to(device)            

            x_s = x_s.to(device)
            oper_info = oper_info.to(device)

            x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
            x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
            x_3 = torch.cat([vit_in, mask_in], dim=-1)
            x_4 = torch.cat([vit_post, mask_post], dim=-1)
            x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
            
            yhat_list = model([x_1,x_2,x_3,x_4,x_5])
            y_pred.append(yhat_list[0])
            y_true.append(y_static[:,y_index:(y_index+1)])
            # for i1 in range(len(aux_info)):
            #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
            #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        # for i in tqdm(range(len(data_batch_2))):

        #     datas = data_batch_2[i]
            
        #     lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
        #         vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
        #         t_list, x_s, oper_info, \
        #         y_mat, y_mask, y_static, y_mask1 = datas
            
        #     lab_pre = lab_pre.to(device)
        #     mask_lab_pre = mask_lab_pre.to(device)
        #     lab_post = lab_post.to(device)
        #     mask_lab_post = mask_lab_post.to(device)

        #     vit_pre = vit_pre.unsqueeze(0).to(device)
        #     mask_pre = mask_pre.unsqueeze(0).to(device)
        #     vit_in = vit_in.unsqueeze(0).to(device)
        #     mask_in = mask_in.unsqueeze(0).to(device)  
        #     vit_post = vit_post.unsqueeze(0).to(device)
        #     mask_post = mask_post.unsqueeze(0).to(device)

        #     x_s = x_s.to(device)
        #     oper_info = oper_info.to(device)

        #     x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
        #     x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
        #     # x_3 = torch.cat([vit_in, mask_in], dim=-1)
        #     # x_4 = torch.cat([vit_post, mask_post], dim=-1)
        #     # x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
            
        #     yhat_list = model([x_1,x_2,x_3,x_4,x_5])
        #     y_pred.append(yhat_list[0])
        #     y_true.append(y_static[:,y_index:(y_index+1)])
        #     # for i1 in range(len(aux_info)):
        #     #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
        #     #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        # for i in tqdm(range(len(data_batch_3))):

        #     datas = data_batch_3[i]
            
        #     lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
        #         vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
        #         t_list, x_s, oper_info, \
        #         y_mat, y_mask, y_static, y_mask1 = datas
            
        #     lab_pre = lab_pre.to(device)
        #     mask_lab_pre = mask_lab_pre.to(device)
        #     lab_post = lab_post.to(device)
        #     mask_lab_post = mask_lab_post.to(device)

        #     vit_pre = vit_pre.unsqueeze(0).to(device)
        #     mask_pre = mask_pre.unsqueeze(0).to(device)
        #     vit_in = vit_in.unsqueeze(0).to(device)
        #     mask_in = mask_in.unsqueeze(0).to(device)  
        #     vit_post = vit_post.unsqueeze(0).to(device)
        #     mask_post = mask_post.unsqueeze(0).to(device)            

        #     x_s = x_s.to(device)
        #     oper_info = oper_info.to(device)

        #     x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
        #     x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
        #     # x_3 = torch.cat([vit_in, mask_in], dim=-1)
        #     # x_4 = torch.cat([vit_post, mask_post], dim=-1)
        #     # x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
            
        #     yhat_list = model([x_1,x_2,x_3,x_4,x_5])
        #     y_pred.append(yhat_list[0])
        #     y_true.append(y_static[:,y_index:(y_index+1)])
        #     # for i1 in range(len(aux_info)):
        #     #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
        #     #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        y_pred = torch.cat(y_pred, dim=0)
        y_true = torch.cat(y_true, dim=0)
        if output_size_list[0] == 1:
            y_true = (y_true > 0).float().to(device)
        else:
            y_true = y_true.to(torch.int64).to(device).reshape(-1)
        loss = loss_fn(y_pred, y_true)
        # for i1 in range(len(aux_info)):
        #     y_pred = torch.cat(aux_info[i1]["y_pred"], dim=0)
        #     y_true = torch.cat(aux_info[i1]["y_true"], dim=0)
        #     if aux_info[i1]["y_dim"] == 1:
        #         y_true = (y_true > 0).float().to(device)
        #     else:
        #         y_true = y_true.to(torch.int64).to(device).reshape(-1)
        #     loss += aux_info[i1]["weight"] * aux_info[i1]["loss_fn"](y_pred, y_true)
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
    # for i1 in range(len(aux_info)):
    #     aux_info[i1]["y_pred"], aux_info[i1]["y_true"] = [],[]
    with torch.no_grad():
        # 在循环开始前初始化列表
        data_loader_0 = dataset_va_0.iterate_batch(batch_size_val,normalize=True)
        data_loader_1 = dataset_va_1.iterate_batch(batch_size_val,normalize=True)
        # data_loader_2 = dataset_va_2.iterate_batch(batch_size_val,normalize=True)
        # data_loader_3 = dataset_va_3.iterate_batch(batch_size_val,normalize=True)
        counter = 1
        while True:
            if counter > max_iter_val:
                break
            try:
                data_batch_0, ids_0 = next(data_loader_0)
            except StopIteration:
                break

            for i in tqdm(range(len(data_batch_0))):

                datas = data_batch_0[i]
                
                lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
                    vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
                    t_list, x_s, oper_info, \
                    y_mat, y_mask, y_static, y_mask1 = datas
                
                lab_pre = lab_pre.to(device)
                mask_lab_pre = mask_lab_pre.to(device)
                lab_post = lab_post.to(device)
                mask_lab_post = mask_lab_post.to(device)

                vit_pre = vit_pre.unsqueeze(0).to(device)
                mask_pre = mask_pre.unsqueeze(0).to(device)
                vit_in = vit_in.unsqueeze(0).to(device)
                mask_in = mask_in.unsqueeze(0).to(device)  
                vit_post = vit_post.unsqueeze(0).to(device)
                mask_post = mask_post.unsqueeze(0).to(device)            

                x_s = x_s.to(device)
                oper_info = oper_info.to(device)

                x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
                x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
                x_3 = torch.cat([vit_in, mask_in], dim=-1)
                x_4 = torch.cat([vit_post, mask_post], dim=-1)
                x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
                
                yhat_list = model([x_1,x_2,x_3,x_4,x_5])
                y_pred.append(yhat_list[0])
                y_true.append(y_static[:,y_index:(y_index+1)])
                # for i1 in range(len(aux_info)):
                #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
                #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])
            counter += 1

        counter = 1
        while True:

            if counter > max_iter_val:
                break

            try:
                data_batch_1, ids = next(data_loader_1)
            except StopIteration:
                break

            for i in tqdm(range(len(data_batch_1))):
                datas = data_batch_1[i]
                
                lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
                    vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
                    t_list, x_s, oper_info, \
                    y_mat, y_mask, y_static, y_mask1 = datas
                
                lab_pre = lab_pre.to(device)
                mask_lab_pre = mask_lab_pre.to(device)
                lab_post = lab_post.to(device)
                mask_lab_post = mask_lab_post.to(device)

                vit_pre = vit_pre.unsqueeze(0).to(device)
                mask_pre = mask_pre.unsqueeze(0).to(device)
                vit_in = vit_in.unsqueeze(0).to(device)
                mask_in = mask_in.unsqueeze(0).to(device)  
                vit_post = vit_post.unsqueeze(0).to(device)
                mask_post = mask_post.unsqueeze(0).to(device)            

                x_s = x_s.to(device)
                oper_info = oper_info.to(device)

                x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
                x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
                x_3 = torch.cat([vit_in, mask_in], dim=-1)
                x_4 = torch.cat([vit_post, mask_post], dim=-1)
                x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
                
                yhat_list = model([x_1,x_2,x_3,x_4,x_5])
                y_pred.append(yhat_list[0])
                y_true.append(y_static[:,y_index:(y_index+1)])
                # for i1 in range(len(aux_info)):
                #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
                #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

            counter += 1

        # counter = 1
        # while True:

        #     if counter > max_iter_val:
        #         break

        #     try:
        #         data_batch_2, ids = next(data_loader_2)
        #     except StopIteration:
        #         break

        #     for i in tqdm(range(len(data_batch_2))):
        #         datas = data_batch_2[i]
                
        #         lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
        #             vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
        #             t_list, x_s, oper_info, \
        #             y_mat, y_mask, y_static, y_mask1 = datas
                
        #         lab_pre = lab_pre.to(device)
        #         mask_lab_pre = mask_lab_pre.to(device)
        #         lab_post = lab_post.to(device)
        #         mask_lab_post = mask_lab_post.to(device)

        #         vit_pre = vit_pre.unsqueeze(0).to(device)
        #         mask_pre = mask_pre.unsqueeze(0).to(device)
        #         vit_in = vit_in.unsqueeze(0).to(device)
        #         mask_in = mask_in.unsqueeze(0).to(device)  
        #         vit_post = vit_post.unsqueeze(0).to(device)
        #         mask_post = mask_post.unsqueeze(0).to(device)            

        #         x_s = x_s.to(device)
        #         oper_info = oper_info.to(device)

        #         x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
        #         x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
        #         # x_3 = torch.cat([vit_in, mask_in], dim=-1)
        #         # x_4 = torch.cat([vit_post, mask_post], dim=-1)
        #         # x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)

        #         yhat_list = model([x_1,x_2,x_3,x_4,x_5])
        #         y_pred.append(yhat_list[0])
        #         y_true.append(y_static[:,y_index:(y_index+1)])
        #         # for i1 in range(len(aux_info)):
        #         #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
        #         #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        #     counter += 1

        # counter = 1
        # while True:

        #     if counter > max_iter_val:
        #         break

        #     try:
        #         data_batch_3, ids = next(data_loader_3)
        #     except StopIteration:
        #         break

        #     for i in tqdm(range(len(data_batch_3))):
        #         datas = data_batch_3[i]
                
        #         lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
        #             vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
        #             t_list, x_s, oper_info, \
        #             y_mat, y_mask, y_static, y_mask1 = datas
                
        #         lab_pre = lab_pre.to(device)
        #         mask_lab_pre = mask_lab_pre.to(device)
        #         lab_post = lab_post.to(device)
        #         mask_lab_post = mask_lab_post.to(device)

        #         vit_pre = vit_pre.unsqueeze(0).to(device)
        #         mask_pre = mask_pre.unsqueeze(0).to(device)
        #         vit_in = vit_in.unsqueeze(0).to(device)
        #         mask_in = mask_in.unsqueeze(0).to(device)  
        #         vit_post = vit_post.unsqueeze(0).to(device)
        #         mask_post = mask_post.unsqueeze(0).to(device)            

        #         x_s = x_s.to(device)
        #         oper_info = oper_info.to(device)

        #         x_1 = torch.cat([lab_pre, mask_lab_pre, x_s], dim=-1)
        #         x_2 = torch.cat([vit_pre, mask_pre], dim=-1)
        #         x_3 = torch.cat([vit_in, mask_in], dim=-1)
        #         # x_4 = torch.cat([vit_post, mask_post], dim=-1)
        #         # x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
                
        #         yhat_list = model([x_1,x_2,x_3,x_4,x_5])
        #         y_pred.append(yhat_list[0])
        #         y_true.append(y_static[:,y_index:(y_index+1)])
        #         # for i1 in range(len(aux_info)):
        #         #     aux_info[i1]["y_pred"].append(yhat_list[aux_info[i1]["ind"]])
        #         #     aux_info[i1]["y_true"].append(y_static[:,aux_info[i1]["y_index"]:(aux_info[i1]["y_index"]+1)])

        #     counter += 1

        y_pred = torch.cat(y_pred, dim=0)
        y_true = torch.cat(y_true, dim=0)
        if output_size_list[0] == 1:
            y_true = (y_true > 0).float().to(device)
        else:
            y_true = y_true.to(torch.int64).to(device).reshape(-1)
        loss = loss_fn(y_pred, y_true)
        # for i1 in range(len(aux_info)):
        #     y_pred = torch.cat(aux_info[i1]["y_pred"], dim=0)
        #     y_true = torch.cat(aux_info[i1]["y_true"], dim=0)
        #     if aux_info[i1]["y_dim"] == 1:
        #         y_true = (y_true > 0).float().to(device)
        #     else:
        #         y_true = y_true.to(torch.int64).to(device).reshape(-1)
        #     loss += aux_info[i1]["weight"] * aux_info[i1]["loss_fn"](y_pred, y_true)
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
            torch.save(model.state_dict(), f"/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_{y_name}_{stage}" + ".pth")
            no_improvement_count = 0
            print("参数已更新")
        else:
            no_improvement_count += 1
        
        # 若连续两轮 AUC 都没有改善，则终止训练
        if no_improvement_count == tol_count:
            break