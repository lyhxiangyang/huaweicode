from collections import defaultdict
from typing import List

"""
# --函数功能：
传入实际值列表和预测值列表，输出准确率、召回率等信息

返回值：
TP: 将正样本预测为正样本的数量
FN：将正样本预测为负样本的数量
FP：将负样本预测为正样本的数量
TN：将负样本预测为副样本的数量
precision: TP / (TP + FP)   正确预测正样本 占 全部预测正样本的比值   精确率
recall: TP / (TP + FN) 正确预测正样本 占 正样本总数的比值           召回率
accuracy: 准确率(accuracy) = 预测对的/所有 = (TP+TN)/(TP+FN+FP+TN) = 70%

"""


def get_metrics(reallist: List, prelist: List, label: int):
    """
    Get Statistical metrics
    :param reallist: Label list of samples
    :param prelist: Predicted label list
    :param label: Label to be selected
    :return: tp, fp, fn and tn etc
    """
    true_pos, true_neg = 0, 0
    false_pos, false_neg = 0, 0
    rightnumber = 0
    for i in range(len(reallist)):
        if reallist[i] == prelist[i]:
            rightnumber += 1
        if prelist[i] == label:
            if reallist[i] == prelist[i]:
                # 正-正
                true_pos += 1
            else:
                # 负-正
                false_pos += 1
        else:
            if reallist[i] != label:
                # 负-负
                true_neg += 1
            else:
                # 正-负
                false_neg += 1
    precision = float('nan') if true_pos + false_pos == 0 else true_pos / (true_pos + false_pos)
    recall = float('nan') if true_pos + false_neg == 0 else true_pos / (true_pos + false_neg)
    accuracy = rightnumber / len(reallist)
    metrics = dict(tp=true_pos, tn=true_neg, fp=false_pos, fn=false_neg, precision=precision, recall=recall,
                   accuracy=accuracy)
    # 添加数量数据，即各个实际标签对应的数量
    realnums = {} # 总数量
    numDict = {}
    for i in reallist:
        if i not in realnums.keys():
            realnums[i] = 0
        if i not in numDict.keys():
            numDict[i] = {}
            numDict[i]["pre_num"] = 0
            numDict[i]["pre_normal_percentage"] = 0

        realnums[i] += 1
        # 正常的数量
        # if prelist[i] not in numDict[i].keys():
        #     numDict[i]

    metrics["realnums"] = realnums


    return metrics
