import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
import textwrap

import random

from dataloader import INSPIRE
from model import *

from tqdm import tqdm
import numpy as np
import json
import copy

from sklearn.metrics import roc_curve
from sklearn.metrics import accuracy_score, recall_score, \
                            precision_score, f1_score, roc_auc_score, \
                            average_precision_score

from sklearn.metrics import confusion_matrix

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils import calculate_integrated_gradients

import matplotlib.pyplot as plt

# -------  环境变量 -----------
y_name = "icu_duration_1d"
y_index = 0
stage = "post"
n_samples = 500
device = torch.device("cuda:2")
file_para = f"/home/luojiawei/pengxiran_project/deep_learning/saved_param/model_{y_name}_{stage}.pth"
file_out = f"/home/luojiawei/pengxiran_project/结果文件夹/模型性能/{y_name}_{stage}.csv"

# -------  模型参数 -----------
# lab_pre/post 36 + 36, vit_pre 76 + 75, vit_in 75 + 75, vit_post 75 + 75, x_s 48, oper 4
input_size_list = [36 + 36 + 48, 79 + 78, 71 * 2, 79 + 78, 36 + 36 + 4]#, 71 * 2 , 79 + 78, 36 + 36 + 4
hidden_size_list = [256, 256, 256, 256, 256] #, 256, 256, 256
output_size_list = [1]
type_list = ["cat"]
model_list = ["mlp","biattlstm", "biattlstm", "biattlstm","mlp"]#, "biattlstm","mlp"

print(f"变量重要度:  task: {y_name}, stage: {stage}")

dataset_te = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                    "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                    id_col="op_id",
                    mod_col="set",
                    mod = [2],
                    cls_col = None,
                    cls = None,
                    stat_path="/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                    )

# 设置绘图区域背景为白色
# plt.rcParams['figure.facecolor'] = 'white'

model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)
pretrained_para = torch.load(file_para)
# for param_tensor in pretrained_para:
#     print(f"{param_tensor}: {pretrained_para[param_tensor].size()}")
model.load_state_dict(pretrained_para)
model = model.to(device)

# 使用 pandas 读取 CSV 文件
blv1 = pd.read_csv("/home/luojiawei/pengxiran_project/blv1.csv", header=0)
blv2 = pd.read_csv("/home/luojiawei/pengxiran_project/blv2.csv", header=0)
blv3 = pd.read_csv("/home/luojiawei/pengxiran_project/blv3.csv", header=0)
blv4 = pd.read_csv("/home/luojiawei/pengxiran_project/blv4.csv", header=0)
blv5 = pd.read_csv("/home/luojiawei/pengxiran_project/blv5.csv", header=0)

baseline_list = [torch.tensor(blv1.iloc[:,1].values).to(torch.float32), 
                 torch.tensor(blv2.iloc[:,1].values).to(torch.float32), 
                 torch.tensor(blv3.iloc[:,1].values).to(torch.float32), 
                 torch.tensor(blv4.iloc[:,1].values).to(torch.float32), 
                 torch.tensor(blv5.iloc[:,1].values).to(torch.float32)]

fea_imp = []

random.seed(42)
for i in tqdm(random.sample(range(dataset_te.len()), n_samples), desc="处理进度"): 
    lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
        vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
        t_list, x_s, oper_info, \
        y_mat, y_mask, y_static, y_mask1 = dataset_te.get_1data(i, normalize=True)

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
    
    grads = calculate_integrated_gradients([x_1,x_2,x_3,x_4,x_5], model, y_index, steps=50, device=device, baseline_list=baseline_list)
    grads[1] = grads[1].sum((0,1))
    grads[2] = grads[2].sum((0,1))
    grads[3] = grads[3].sum((0,1))
    # fea_imp.append(np.concatenate([grads[0][0], grads[1]], axis=0))
    # fea_imp.append(np.concatenate([grads[0][0], grads[1], grads[2]], axis=0))
    fea_imp.append(np.concatenate([grads[0][0], grads[1], grads[2],grads[3],grads[4][0]], axis=0))


fea_imp = np.stack(fea_imp, axis=0)
np.save(f'/home/luojiawei/pengxiran_project/结果文件夹/fea_imp_folder/fea_imp_{y_name}_{stage}.npy', fea_imp)
print(f"特征重要性文件已保存至: /home/luojiawei/pengxiran_project/结果文件夹/fea_imp_folder/fea_imp_{y_name}_{stage}.npy")
