#!/usr/bin/python

import pickle
import sys
from sklearn.model_selection import train_test_split, KFold

from sklearn.linear_model import LinearRegression

sys.path.append("./tools/")
from feature_format import featureFormat, targetFeatureSplit

file='MyDataFile.pkl'

### read in data dictionary, convert to numpy array
data_dict = pickle.load( open(file, "r") )

#features = ["race_speed", "race_distance", 'workout_count', 'workout_distance', 'workout_duration', 'velocity_*', 'cadence_*' ]
features = ["race_speed", "race_distance", 'workout_count', 'workout_distance', 'workout_duration', 'velocity_*']
data_array = featureFormat( data_dict, features )
target, features = targetFeatureSplit( data_array )

feature_train, feature_test, target_train, target_test = train_test_split(features, target, test_size=0.25, random_state=42)

print 'Feature Train:'
print feature_train
print
print 'Target Train:'
print target_train

import numpy as np

reg = LinearRegression()
reg.fit( feature_train,  target_train )

accuracy = reg.score( feature_test, target_test )

print accuracy