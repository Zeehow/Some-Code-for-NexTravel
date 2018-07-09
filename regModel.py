# -*- coding: utf-8 -*-
"""
Created on Tue Jun 12 17:02:57 2018

@author: zhehaoca
"""

import pandas as pd
import numpy as np
import scipy as sp
import sklearn as skl
import mysql.connector as con
import scipy.cluster.hierarchy as sch
from scipy.cluster.vq import vq, kmeans, whiten
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import datetime as dt
from dateutil import parser
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

def varTest (trainbase, iterations):
    percDf = pd.DataFrame()
    for i in trainbase:
        percArr = []
        traindata = trainbase[[i]]
        for j in range(iterations):
            train, test, trainTar, testTar = train_test_split(traindata, target, test_size = 0.2)
            gbmodel = GradientBoostingClassifier(n_estimators=100)
            gbmodel.fit(train, trainTar)
            predictions = gbmodel.predict_proba(test)
            result = []
            for prediction in predictions[:,0]:
                if prediction < 0.5:
                    result.append(1)
                else:
                    result.append(0)
            fault = 0
            for k in range(len(testTar)):
                 if testTar[k] != result[k]:
                 #if testTar[k] == 0:
                     fault = fault + 1
            perc = fault/len(testTar)
            percArr.append(perc)
        percDf[str(i)] = (percArr)
    return percDf
    
#def genVar (varSeries):
#    varSeries.fillna(0)
    

base = pd.read_csv("E:/NexTravel/withGeo.csv")
res = pd.read_csv("E:/NexTravel/churnVal.csv")
truF = base['name'].isin(res['name'])
cnt = 0
dropArr = []
for i in truF:
    if i == False:
        dropArr.append(cnt)
    cnt = cnt + 1
base = base.drop(base.index[dropArr])
base = base.reset_index()
base = base.drop(['index'], axis = 1)
base = base.drop_duplicates(subset = 'name', keep = 'first')
truF = res['name'].isin(base['name'])
cnt = 0
dropArr = []
for i in truF:
    if i == False:
        dropArr.append(cnt)
    cnt = cnt + 1
res = res.drop(res.index[dropArr])
res = res.drop_duplicates(subset = 'name', keep = 'first')
cat = 1
rec = []
res = res.sort_values(['name'])
res = res.reset_index()
base = base.sort_values(['name'])
base = base.reset_index()
base = base.drop(['index'], axis = 1)
res = res.drop(['index'], axis = 1)
base['Y2'] = res['Y2'].map({'start':2,'good': 1, 'churn': 0})
base['Y3'] = res['Y3'].map({'start':2,'good': 1, 'churn': 0})
base['Y4'] = res['Y4'].map({'start':2,'good': 1, 'churn': 0})
base['Y5'] = res['Y5'].map({'start':2,'good': 1, 'churn': 0})
base = base[base['Y5'] != 2]
base = base.sort_values(['industryLevel0', 'industryLevel1'])
rec0 = 0
rec1 = 0
arr0 = []
arr1 = []
cnt0 = 0
cnt1 = 0
pos = 0
base = base.reset_index()
base = base.drop(['index'], axis = 1)

for i in base['industryLevel1']:
    if not pd.isnull(i):
        if i != rec1:
            cnt1 = cnt1 + 1
        arr1.append(cnt1)
        rec1 = i
    else:
        arr1.append(0)
    if not pd.isnull(base.loc[pos, 'industryLevel0']):
        if base.loc[pos, 'industryLevel0'] != rec0:
            cnt0 = cnt0 + 1
        arr0.append(cnt0)
        rec0 = base.loc[pos, 'industryLevel0']
        pos = pos + 1
    else:
        arr0.append(0)
        
base['size'] = base['size'].fillna(0)
base['emails'] = base['emails'].fillna(0)
base['i0'] = arr0
base['i1'] = arr1

base = base.sort_values(['geoCountryCode'])
gccNumber = 1
gccRec = -1
gccArr = []
for i in base['geoCountryCode']:
    if not pd.isnull(i):
        if i != gccRec:
            gccNumber = gccNumber + 1
        gccArr.append(gccNumber)
    else:
        gccArr.append(0)
base['gccNum'] = gccArr
base = base.reset_index()
base = base.drop(['index'], axis = 1)

base = base.sort_values(['geoCity'])
gcNumber = 1
gcRec = -1
gcArr = []
for i in base['geoCity']:
    if not pd.isnull(i):
        if i != gcRec:
            gcNumber = gcNumber + 1
        gcArr.append(gcNumber)
    else:
        gcArr.append(0)
base['gcNum'] = gcArr
base = base.reset_index()
base = base.drop(['index'], axis = 1)

base['cpero'] = base['calls']/base['orders']
base['epero'] = base['emails']/base['orders']
target = []
traindata = base[['size', 'i0','i1','cpero','epero','gccNum']]
for rows in base.itertuples():
    if rows.Y2 == 1 or rows.Y2 == 2 or rows.Y2 == 3:
        if rows.Y3 == 0:
            target.append(0)
        else:
            target.append(1)
    elif rows.Y2 == 0:
        target.append(0)
    elif rows.Y3 == 2:
        if rows.Y4 == 0:
            target.append(0)
        else:
            target.append(1)
    elif rows.Y4 == 2:
        if rows.Y5 == 0:
            target.append(0)
        else:
            target.append(1)
            
#Get list of churn Companies
listChurn = []
for ite in range(len(target)):
    if target[ite] == 0:
        listChurn.append(base.loc[ite, 'name'])
            
train, test, trainTar, testTar = train_test_split(traindata, target, test_size = 0.2)
gbmodel = GradientBoostingClassifier(n_estimators=100)
gbmodel.fit(train, trainTar)
predictions = gbmodel.predict_proba(test)
result = []
for prediction in predictions[:,0]:
    if prediction < 0.5:
        result.append(1)
    else:
        result.append(0)
fault = 0

for i in range(len(testTar)):
     if testTar[i] != result[i]:
         fault = fault + 1

base = base
churnY2 = (base[base['Y2'] == 0]['ci'])
churnY3 = (base[base['Y3'] == 0]['ci'])
churnY4 = (base[base['Y4'] == 0]['ci'])
x_coord = []
for i in range(len(testTar)):
    x_coord.append(i/len(testTar))

plt.scatter(x_coord, predictions[:,1], c = testTar)
plt.fill([0,1,1,0],[0,0,0.5,0.5], c='#ff00ff80')
plt.fill([0,1,1,0],[0.5,0.5,1,1], c='#ffff0080')
plt.show()

perc = fault/len(testTar)
testperc = 0
for i in target:
    if i != 1:
        testperc = testperc + 1
testperc = testperc/len(target)

testRes = varTest(traindata, 10)
