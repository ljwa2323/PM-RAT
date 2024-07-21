import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

import pandas as pd

from dataloader import INSPIRE
from model import *

from tqdm import tqdm
import numpy as np
import json

from sklearn.preprocessing import label_binarize

from sklearn.metrics import roc_curve
from sklearn.metrics import accuracy_score, recall_score, \
                            precision_score, f1_score, roc_auc_score, \
                            average_precision_score

from sklearn.metrics import confusion_matrix

# -------  环境变量 -----------
y_name = "in_hospital_death"
y_index = 49
stage = "pre"
device = torch.device("cuda:0")
file_para = f"/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_{y_name}_{stage}.pth"
file_out = f"/home/luojiawei/pengxiran_project/结果文件夹/模型性能/{y_name}_{stage}.csv"

# -------  模型参数 -----------
# lab_pre/post 36 + 36, vit_pre 79 + 78, vit_in 71 * 2, vit_post 79 + 78, x_s 48, oper 4
input_size_list = [36 + 36 + 48, 79 + 78]# , 71 * 2 , 79 + 78, 36 + 36 + 4
hidden_size_list = [256, 256] # ,256, 256, 256
output_size_list = [1]
type_list = ["cat"]
model_list = ["mlp","biattlstm"]#,"biattlstm","biattlstm","mlp"

print(f"task: {y_name}, stage: {stage}")

dataset_te = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [2],
                    cls_col = None,
                    cls = None,
                    stat_path="/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

# -------- 训练参数 ------------
EPOCHS = 100
lr = 0.001
weight_decay = 0.001
tol_count = 10000000

if output_size_list[0] > 1:
    loss_fn = nn.CrossEntropyLoss()
else:
    loss_fn = nn.BCEWithLogitsLoss()

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
pretrained_para = torch.load(file_para)
# for param_tensor in pretrained_para:
#     print(f"{param_tensor}: {pretrained_para[param_tensor].size()}")
model.load_state_dict(pretrained_para)
model = model.to(device)

running_loss = 0.0
y_true, y_pred = [], []
with torch.no_grad():

    for i in tqdm(range(dataset_te.len())):
        datas = dataset_te.get_1data(i,normalize=True)

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
        # x_3 = torch.cat([vit_in, mask_in], dim=-1)
        # x_4 = torch.cat([vit_post, mask_post], dim=-1)
        # x_5 = torch.cat([lab_post, mask_lab_post, oper_info], dim=-1)
        
        yhat_list = model([x_1,x_2])
        y_pred.append(yhat_list[0])
        y_true.append(y_static[:,y_index:(y_index+1)])


    y_pred = torch.cat(y_pred, dim=0)
    y_true = torch.cat(y_true, dim=0)
    if output_size_list[0] == 1:
        y_true = (y_true > 0).float().to(device)
    else:
        y_true = y_true.to(torch.int64).reshape(-1).to(device)
    loss = loss_fn(y_pred, y_true)
    running_loss += loss.cpu().item()

# 将模型输出转换为概率
y_pred_prob = y_pred.cpu().numpy()
y_true_np = y_true.cpu().numpy()

# 整理成 DataFrame
df = pd.DataFrame(y_pred_prob, columns=[f'y_pred_prob_{i}' for i in range(y_pred_prob.shape[1])])
df['y_true'] = y_true_np.flatten()
df.insert(0, 'all_id', dataset_te.all_id)

# 输出到文件
df.to_csv(file_out, index=False)