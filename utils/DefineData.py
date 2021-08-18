from enum import Enum, unique, IntEnum


@unique
class NodeState(IntEnum):
    pass



WINDOWS_SIZE= 5
FAULT_FLAG = "faultFlag"
TIME_COLUMN_NAME = "time"

DEBUG = True

# 定义固定的文件名字
FDR = 0.01

# 需要排除的特征名字
EXCLUDE_FEATURE_NAME = ["time", FAULT_FLAG]

# 机器学习用到的常数
# 模型类型
MODEL_TYPE = ['decision tree', 'random forest', 'adaptive boosting']
