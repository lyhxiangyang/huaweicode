import string
import time
from typing import List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection


def time_to_num(timestamp: string, form: string) -> float:
    return time.mktime(time.strptime(timestamp, form))


def num_to_time(num: float, form: string) -> string:
    return time.strftime(form, time.gmtime(num))


def split_by_core(df: pd.DataFrame) -> dict:
    core_list = list(set(df['cpu_affinity']))
    core_dict = {}
    for core in core_list:
        core_dict[core] = df.loc[df['cpu_affinity'] == core]
    return core_dict


def plot_change_server(files: List, features: List) -> None:
    print("Reading files...".center(40, "*"))
    df_list = [pd.read_csv(file) for file in files]

    print("Plotting...".center(40, "*"))
    for df in df_list:
        colors = []
        for flag in df['faultFlag']:
            if flag == 0:
                colors.append('b')
            else:
                colors.append('r')
        for feature in features:
            x = [i for i in range(len(df))]
            y = df[feature].tolist()
            points = np.array([x, y]).T.reshape(-1, 1, 2)
            segments = np.concatenate([points[:-1], points[1:]], axis=1)
            lc = LineCollection(segments, colors=colors)
            ax = plt.axes()
            ax.add_collection(lc)
            ax.set_xlim(min(x), max(x))
            ax.set_ylim(min(y), max(y))
            plt.title(feature)
            plt.show()


def plot_change_node(files: List, nodes: List, features: List) -> None:
    time_form = '%Y-%m-%d %H:%M:%S'

    print("Reading files...".center(40, "*"))
    df_list = [pd.read_csv(file) for file in files]
    df = pd.concat(df_list, ignore_index=True)
    print(df.head())

    print("Splitting...".center(40, "*"))
    core_dict = split_by_core(df)

    print("Plotting...".center(40, "*"))
    for node in nodes:
        colors = []
        for flag in df['faultFlag']:
            if flag == 0:
                colors.append('b')
            else:
                colors.append('r')
        for feature in features:
            x = [i for i in range(core_dict[node])]
            y = core_dict[node][feature].tolist()
            points = np.array([x, y]).T.reshape(-1, 1, 2)
            segments = np.concatenate([points[:-1], points[1:]], axis=1)
            lc = LineCollection(segments, colors=colors)
            ax = plt.axes()
            ax.add_collection(lc)
            ax.set_xlim(min(x), max(x))
            ax.set_ylim(min(y), max(y))
            plt.title('node%d: %s' % (node, feature))
            plt.show()


if __name__ == '__main__':
    files_node = [
        'D:/HuaweiMachine/测试数据/wrfrst单机版e5/wrfrst单机版e5/wrfrst-7.16-1/wrf_rst_e5-43_process_cpuall-10.csv',
    ]
    files_server = [
        # 'D:/HuaweiMachine/测试数据/wrfrst单机版e5/wrfrst单机版e5/wrfrst-7.16-1/wrf_rst_e5-43_server.csv',
        'D:/HuaweiMachine/测试数据/wrfrst_normal_e5/result/normal_single/wrfrst_e5-43_server.csv',
        'D:/HuaweiMachine/测试数据/grapes_normal-e5/result/normal_single/grapes_e5-43_server.csv',
    ]
    plot_change_server(files_server, ['load1', 'cached'])
