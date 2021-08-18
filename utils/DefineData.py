from enum import Enum, unique, IntEnum


@unique
class NodeState(IntEnum):
    pass



WINDOWS_SIZE= 5
FAULT_FLAG = "faultFlag"
DEBUG = True

# 定义固定的文件名字
FDR = 0.01



# 机器学习用到的常数
# 模型类型
MODEL_TYPE = ['decision tree', 'random forest', 'adaptive boosting']
