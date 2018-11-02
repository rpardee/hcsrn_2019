import pyodbc
import numpy as np
import pandas as pd
from collections import defaultdict
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.externals import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import sys

HOME_DIR = r"//bibbity/bobbity/boo/pqi_hospitalizations/ml/"

vec         = joblib.load(HOME_DIR +"binarizer.pkl")
feature_set = set(vec.classes_)   # or vec.classes_
cdf         = joblib.load(HOME_DIR +"RandmForest.pkl")

feature_importances = pd.DataFrame(cdf.feature_importances_,
                                   index = feature_set,
                                    columns=['importance']).sort_values('importance', ascending=False)

print(feature_importances.head())

feature_importances.to_csv(HOME_DIR + "feature_importances.csv")

print("Finished!")
