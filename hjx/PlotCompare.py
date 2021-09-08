import string
from typing import List

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt


def plot_compare(y1: List, y2: List, title: string) -> None:
    x = [i for i in range(len(y1))]
    plt.plot(x, y1, 'b', label='normal')
    plt.plot(x, y2, 'r', label='anomalous')
    plt.title(title)
    plt.legend()
    plt.show()


if __name__ == '__main__':
    file_normal = 'D:/HuaweiMachine/测试数据/wrfrst_normal_e5/result/normal_single/wrfrst_e5-43_server.csv'
    file_anomalous = 'D:/HuaweiMachine/测试数据/wrfrst单机版e5/wrfrst单机版e5/wrfrst-7.16-1/wrf_rst_e5-43_server.csv'
    feature = 'load1'
    list_normal = pd.read_csv(file_normal)[feature].tolist()
    list_anomalous = pd.read_csv(file_anomalous)[feature].tolist()
    matplotlib.rc('font', family='FangSong', weight='bold')

    y1 = list_normal[:15]
    y2 = list_anomalous[:15]
    plot_compare(y1, y2, '%s - %d' % (feature, 81))

    y3 = list_anomalous[84:99]
    plot_compare(y1, y3, '%s - %d' % (feature, 85))
