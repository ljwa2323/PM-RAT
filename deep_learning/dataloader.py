import os
import pandas as pd
import numpy as np
import torch
import torch.nn as nn

# class INSPIRE(object):

#     def __init__(self, root, id_file, id_col=None, 
#                  mod_col=None, mod=None, cls_col=None, cls=None):

#         super().__init__()
#         assert id_col is not None, "id_col 不能为 None"
#         self.root = root
#         ds = pd.read_csv(id_file, header=0)
#         if mod is not None:
#             ds = ds[ds.loc[:, mod_col].isin(mod)]
#         if cls is not None:
#             ds = ds[ds.loc[:, cls_col] == cls]
#         print("all_id 的数量:", len(ds))

#         self.all_id = ds.loc[:, id_col].tolist()

#     def len(self):
#         return len(self.all_id)

#     def get_1data(self, ind):

#         folder_path = os.path.join(self.root, str(self.all_id[ind]))

#         ds_X = pd.read_csv(os.path.join(folder_path, "X1.csv"), header=0)
#         ds_mask = pd.read_csv(os.path.join(folder_path, "mask.csv"), header=0)
#         ds_t_list = pd.read_csv(os.path.join(folder_path, "t_list.csv"), header=0)
#         ds_x_s = pd.read_csv(os.path.join(folder_path, "x_s.csv"), header=0)

#         ds_y_mat = pd.read_csv(os.path.join(folder_path, "y_mat.csv"), header=0)
#         ds_y_mask = pd.read_csv(os.path.join(folder_path, "y_mask.csv"), header=0)
#         ds_y_static = pd.read_csv(os.path.join(folder_path, "y_static.csv"), header=0)
#         ds_y_mask1 = pd.read_csv(os.path.join(folder_path, "y_mask1.csv"), header=0)

#         X = torch.tensor(ds_X.iloc[:,1:].values).float()
#         mask = torch.tensor(ds_mask.iloc[:,1:].values).float()
#         time = torch.tensor(ds_X.iloc[:,0:1].values).float()
#         t_list = torch.tensor(ds_t_list.iloc[:,0:1].values).float()
#         x_s =  torch.tensor(ds_x_s.values).float()
#         y_mat =  torch.tensor(ds_y_mat.values).float()
#         y_mask =  torch.tensor(ds_y_mask.values).float()
#         y_static =  torch.tensor(ds_y_static.values).float()
#         y_mask1 =  torch.tensor(ds_y_mask1.values).float()
        
#         return X, mask, t_list, time, x_s, y_mat, \
#                 y_mask, y_static, y_mask1

#     def get_batch_data(self, inds):

#         batches = []
#         ids1 = []
#         for i in range(len(inds)):
#             data = self.get_1data(inds[i])
#             if data is None:
#                 continue
#             else:
#                 batches.append(data)
#                 ids1.append(inds[i])
#         return batches, ids1

#     def iterate_batch(self, size, shuffle=True):

#         if size > self.len():
#             raise ValueError("batch size 大于了总样本数")

#         if shuffle:
#             all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
#         else:
#             all_ids = list(range(self.len()))

#         if self.len() % size == 0:
#             n = self.len() // size
#         else:
#             n = self.len() // size + 1

#         for i in range(n):
#             if i == n:
#                 yield self.get_batch_data(all_ids[(size * i):])
#             else:
#                 yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))])
#         return

class INSPIRE(object):

    def __init__(self, root, id_file, id_col=None, 
                 mod_col=None, mod=None, cls_col=None, cls=None, stat_path=None):

        super().__init__()
        assert id_col is not None, "id_col 不能为 None"
        self.root = root
        ds = pd.read_csv(id_file, header=0)
        if mod is not None:
            ds = ds[ds.loc[:, mod_col].isin(mod)]
        if cls is not None:
            ds = ds[ds.loc[:, cls_col].isin(cls)]
        print("all_id 的数量:", len(ds))

        self.all_id = ds.loc[:, id_col].tolist()
        if stat_path:
            self.stat_lab, self.stat_vit_in, self.stat_vit_out, self.stat_x_s, self.stat_oper_info = self.get_stat_info(stat_path)

    def get_stat_info(self, path):
        stat_lab = torch.tensor(pd.read_csv(os.path.join(path, "lab_data.csv"), header=0).iloc[:,1:].values).float()
        stat_vit_in = torch.tensor(pd.read_csv(os.path.join(path, "vit_in_data.csv"), header=0).iloc[:,1:].values).float()
        stat_vit_out = torch.tensor(pd.read_csv(os.path.join(path, "vit_out_data.csv"), header=0).iloc[:,1:].values).float()
        stat_x_s = torch.tensor(pd.read_csv(os.path.join(path, "x_s_data.csv"), header=0).iloc[:,1:].values).float()
        stat_oper_info = torch.tensor(pd.read_csv(os.path.join(path, "oper_info_data.csv"), header=0).iloc[:,1:].values).float()
        return stat_lab, stat_vit_in, stat_vit_out, stat_x_s, stat_oper_info

    def normalize(self, x, m, s):
        return (x - m) / s

    def unnormalize(self, x, m, s):
        return (x * s) + m

    def len(self):
        return len(self.all_id)

    def get_1data(self, ind, normalize=False):

        folder_path = os.path.join(self.root, str(self.all_id[ind]))

        ds_x_s = pd.read_csv(os.path.join(folder_path, "x_s.csv"), header=0)
        ds_t_list = pd.read_csv(os.path.join(folder_path, "t_list.csv"), header=0)
        ds_oper_info = pd.read_csv(os.path.join(folder_path, "oper_info.csv"), header=0)

        ds_lab_pre = pd.read_csv(os.path.join(folder_path, "lab_pre.csv"), header=0)
        ds_mask_lab_pre = pd.read_csv(os.path.join(folder_path, "mask_lab_pre.csv"), header=0)
        ds_lab_post = pd.read_csv(os.path.join(folder_path, "lab_post.csv"), header=0)
        ds_mask_lab_post = pd.read_csv(os.path.join(folder_path, "mask_lab_post.csv"), header=0)

        ds_vit_pre = pd.read_csv(os.path.join(folder_path, "vit_pre.csv"), header=0)
        ds_vit_in = pd.read_csv(os.path.join(folder_path, "vit_in.csv"), header=0)
        ds_vit_post = pd.read_csv(os.path.join(folder_path, "vit_post.csv"), header=0)
        ds_mask_vit_pre = pd.read_csv(os.path.join(folder_path, "mask_vit_pre.csv"), header=0)
        ds_mask_vit_in = pd.read_csv(os.path.join(folder_path, "mask_vit_in.csv"), header=0)
        ds_mask_vit_post = pd.read_csv(os.path.join(folder_path, "mask_vit_post.csv"), header=0)

        ds_y_mat = pd.read_csv(os.path.join(folder_path, "y_mat.csv"), header=0)
        ds_y_mask = pd.read_csv(os.path.join(folder_path, "y_mask.csv"), header=0)
        ds_y_static = pd.read_csv(os.path.join(folder_path, "y_static.csv"), header=0)
        ds_y_mask1 = pd.read_csv(os.path.join(folder_path, "y_mask1.csv"), header=0)

        lab_pre = torch.tensor(ds_lab_pre.values).float()
        mask_lab_pre = torch.tensor(ds_mask_lab_pre.values).float()
        lab_post = torch.tensor(ds_lab_post.values).float()
        mask_lab_post = torch.tensor(ds_mask_lab_post.values).float()

        vit_pre = torch.tensor(ds_vit_pre.iloc[:,1:].values).float()
        mask_pre = torch.tensor(ds_mask_vit_pre.iloc[:,1:].values).float()
        vit_in = torch.tensor(ds_vit_in.iloc[:,1:-1].values).float()
        mask_in = torch.tensor(ds_mask_vit_in.iloc[:,1:].values).float()
        vit_post = torch.tensor(ds_vit_post.iloc[:,1:].values).float()
        mask_post = torch.tensor(ds_mask_vit_post.iloc[:,1:].values).float()

        t_list = torch.tensor(ds_t_list.iloc[:,0:1].values).float()
        x_s =  torch.tensor(ds_x_s.values).float()
        oper_info =  torch.tensor(ds_oper_info.values).float()
        y_mat =  torch.tensor(ds_y_mat.values).float()
        y_mask =  torch.tensor(ds_y_mask.values).float()
        y_static =  torch.tensor(ds_y_static.values).float()
        y_mask1 =  torch.tensor(ds_y_mask1.values).float()

        if normalize:
            x_s = self.normalize(x_s, self.stat_x_s[:, 0], self.stat_x_s[:, 1])
            oper_info = self.normalize(oper_info, self.stat_oper_info[:, 0], self.stat_oper_info[:, 1])
            lab_pre = self.normalize(lab_pre, self.stat_lab[:, 0], self.stat_lab[:, 1])
            lab_post = self.normalize(lab_post, self.stat_lab[:, 0], self.stat_lab[:, 1])
            vit_pre = self.normalize(vit_pre, self.stat_vit_out[:, 0], self.stat_vit_out[:, 1])
            vit_in = self.normalize(vit_in, self.stat_vit_in[:-1, 0], self.stat_vit_in[:-1, 1])
            vit_post = self.normalize(vit_post, self.stat_vit_out[:, 0], self.stat_vit_out[:, 1])

        return lab_pre, mask_lab_pre, lab_post, mask_lab_post, \
                vit_pre, mask_pre, vit_in, mask_in, vit_post, mask_post, \
                t_list, x_s, oper_info, \
                y_mat, y_mask, y_static, y_mask1

    def get_batch_data(self, inds, normalize=False):

        batches = []
        ids1 = []
        for i in range(len(inds)):
            data = self.get_1data(inds[i], normalize)
            if data is None:
                continue
            else:
                batches.append(data)
                ids1.append(inds[i])
        return batches, ids1

    def iterate_batch(self, size, shuffle=True, normalize=False):

        if size > self.len():
            size = self.len()  # 如果batch size大于总样本数，设置为总样本数

        if shuffle:
            all_ids = np.random.choice(self.len(), size=self.len(), replace=False)
        else:
            all_ids = list(range(self.len()))

        if self.len() % size == 0:
            n = self.len() // size
        else:
            n = self.len() // size + 1

        for i in range(n):
            if i == n - 1:  # 修改条件，确保最后一个batch正确处理
                yield self.get_batch_data(all_ids[(size * i):], normalize)
            else:
                yield self.get_batch_data(all_ids[(size * i): (size * (i + 1))], normalize)
        return

if __name__ == "__main__":
    
    from model import *
    dataset = INSPIRE("/home/luojiawei/pengxiran_project_data/all_op_id1/",
                      "/home/luojiawei/pengxiran_project_data/ds_id.csv",
                      id_col="op_id",
                      mod_col="set",
                      mod = [1,2,3],
                      cls_col = None,
                      cls = None,
                      stat_path = "/home/luojiawei/pengxiran_project/deep_learning/stat_info"
                      )
    print("--")
    datas = dataset.get_1data(1900, normalize=True)
    # dataset.all_id.index(400198221)

    # input_size_list = [36, 56, 56, 27]
    # hidden_size_list = [128, 128, 128, 64]
    # output_size_list = [1]
    # type_list = ["cat" for _ in range(len(output_size_list))]
    # model_list = ["mlp","biattlstm", "biattlstm","mlp"]
    # loss_functions = []
    # for output_size, data_type in zip(output_size_list, type_list):
    #     if output_size == 1:
    #         if data_type == "num":
    #             loss_functions.append(nn.MSELoss())
    #         elif data_type == "cat":
    #             loss_functions.append(nn.BCEWithLogitsLoss())
    #     elif output_size > 1 and data_type == "cat":
    #         loss_functions.append(nn.CrossEntropyLoss())


    # model = PredModel(input_size_list, hidden_size_list, model_list, output_size_list, type_list)

    # lab, mask_lab, vit_pre, mask_pre, vit_in, mask_in, t_list, x_s, \
    #             y_mat, y_mask, y_static, y_mask1 = datas

    # j = 4
    # ind = torch.where(time < t_list[j,0])[0]
    # yhat_list = model([lab,vit_pre.unsqueeze(0), vit_in.unsqueeze(0), x_s])

    # ind1 = torch.where(time == t_list[j,0])[0]
    # # y_mat[ind1]
    # # y_mask[ind1]

    # loss_list_total = [[] for _ in range(len(loss_functions))]
    # yhat_list = model([X[:,ind, ], x_s])
    # for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[:9], loss_functions[:9], output_size_list[:9], type_list[:9])):
    #     if type == "cat":
    #         if output_size > 1:
    #             loss = loss_fn(yhat, y_mat[ind1][0,k:(k+1)].to(torch.int64))
    #         else:
    #             loss = loss_fn(yhat, y_mat[ind1][:,k:(k+1)])
    #     else:
    #         loss = loss_fn(yhat, y_mat[ind1][:,k])
    #     # 将loss添加到对应的列表中
    #     loss_list_total[k].append(loss * y_mask[ind1][0, k])

    # # 处理静态特征的loss
    # ind = torch.where(time < t_list[-1,0])[0]
    # yhat_list = model([X[:,ind, ], x_s])
    # for k, (yhat, loss_fn, output_size, type) in enumerate(zip(yhat_list[9:], loss_functions[9:], output_size_list[9:], type_list[9:])):
    #     if type == "cat":
    #         if output_size > 1:
    #             loss = loss_fn(yhat, y_static[0,k:(k+1)].to(torch.int64))
    #         else:
    #             loss = loss_fn(yhat, y_static[:,k:(k+1)])
    #     else:
    #         loss = loss_fn(yhat, y_static[:,k])
    #     # 将loss添加到对应的列表中
    #     loss_list_total[9 + k].append(loss * y_mask1[0, k])

    # # 计算每个任务的平均loss并汇总
    # loss_batch = [torch.stack(loss_list).mean() for loss_list in loss_list_total]
    # loss = torch.stack(loss_batch).mean()

    # for j in range(1, t_list.shape[0]):
    #     j = 2
    #     ind = torch.where(time < t_list[j,0])[0]
    #     if t_list[j,0] - time[ind[-1]] > 60 * 12:
    #         continue
    #     X[ind, ]

