from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn import metrics
import joblib
from utils.DefineData import *


def model_train(df, model_type):
    """
    Train the model of selected type
    :param df: Dataframe of selected features and labels
    :param model_type: The type of model to be trained
    """
    # Remove column "Intensity" if exists
    header = list(df.columns)

    # 如果有Intensity这个 就使用Intensity
    # if header.count('Intensity'):
    #     header.remove('Intensity')
    # df = df[header]

    model = DecisionTreeClassifier(random_state=0)
    if model_type == 'random forest':
        # Numbers of decision trees is 100
        model = RandomForestClassifier(n_estimators=100, random_state=0)
    elif model_type == 'adaptive boosting':
        # Numbers of decision trees is 100 and the maximum tree depth is 5
        estimator_cart = DecisionTreeClassifier(max_depth=5)
        model = AdaBoostClassifier(base_estimator=estimator_cart, n_estimators=100, random_state=0)

    # Split the data into train and test set
    x = df.drop(FAULT_FLAG, axis=1)
    y = df[FAULT_FLAG]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.28, random_state=0)

    # Train the model
    model.fit(x_train, y_train)

    # Test the accuracy of the model
    y_eval = model.predict(x_test)
    accuracy = metrics.accuracy_score(y_test, y_eval)
    print('Accuracy of %s classifier: %f' % (model_type, accuracy))

    # Save model
    joblib.dump(model, '%s/%s.pkl' % (SaveModelPath ,model_type))

    # Save the header without label
    header = list(df.columns)
    header.remove(FAULT_FLAG)
    f = open('%s/header.txt' % SaveModelPath, 'w')
    f.write('\n'.join(header))
    f.close()
