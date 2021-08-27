def get_metrics(x, y, label):
    """
    Get Statistical metrics
    :param x: Label list of samples
    :param y: Predicted label list
    :param label: Label column name
    :return: tp, fp, fn and tn
    """
    true_pos, true_neg = 0, 0
    false_pos, false_neg = 0, 0
    for i in range(len(x)):
        if x[i] == label:
            if x[i] == y[i]:
                true_pos += 1
            else:
                false_pos += 1
        else:
            if y[i] != label:
                true_neg += 1
            else:
                false_neg += 1
    precision = float('nan') if true_pos + false_pos == 0 else true_pos / (true_pos + false_pos)
    recall = float('nan') if true_pos + false_neg == 0 else true_pos / (true_pos + false_neg)
    accuracy = (true_pos + true_neg) / len(x)
    metrics = {'tp': true_pos, 'tn': true_neg, 'fp': false_pos, 'fn': false_neg, 'precision': precision, 'recall': recall, 'accuracy': accuracy }
    return metrics

