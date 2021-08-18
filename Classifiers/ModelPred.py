import joblib


def model_pred(x_pred, model_type):
    """
    Use trained model to predict
    :param x_pred: Samples to be predicted
    :param model_type: The type of saved model to be used
    :return y_pred: Class labels for samples in x_pred
    """
    # Load saved model
    model = joblib.load('Data/saved_model/%s.pkl' % model_type)

    y_pred = model.predict(x_pred)
    return y_pred


def select_and_pred(df, model_type):
    # Select needed features
    f = open('Data/saved_model/header.txt', 'r')
    features = f.read().splitlines()
    df_selected = df[features]

    # Use trained model to predict
    y_pred = model_pred(df_selected, model_type)
    return y_pred
