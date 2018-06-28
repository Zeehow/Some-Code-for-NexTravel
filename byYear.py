# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 14:16:44 2018

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


def deleteOutliers (array):
    stdf = np.std(array[:,0])
    avgf = np.average(array[:,0])
    stds = np.std(array[:,1])
    avgs = np.average(array[:,1])
    ret = array
    idx = 0
    delarr = []
    for i in ret:
        if (abs(i[0] - avgf) >= 2*stdf) or (abs(i[1] - avgs) >= 2*stds):
            delarr.append(idx)
        idx = idx + 1
        #if (i[0] >= 60):
        #    delarr.append(idx)
        #idx = idx + 1
    ret = np.delete(ret, delarr, 0)
    #print (ret.size)
    return ret

def findCenter (array):
    return np.mean(array, axis = 0)
    
cnx = con.connect(user='readwrite', password = 't8seQkRzqTf5EnAW',
                  host='nex-stage-12.cyq7akijzgow.us-west-1.rds.amazonaws.com',
                  database='nex')
cursor = cnx.cursor()
sql = "SELECT C.companyId, C.name, O.orderId, O.dateAdded FROM nex.order AS O, nex.company AS C WHERE O.companyId = C.companyId ORDER BY O.companyId"
base = pd.read_sql(sql, cnx)
#base = pd.read_csv("E:/NexTravel/try.csv")
base = base[base["companyId"] != -1]
base = base[base['companyId'] != 1]
#base = base.drop_duplicates(subset = 'name', keep = 'first')
tot = []
grandTot = []
arr = []
year = []
#for i in base['dateAdded']:
#    base.loc[cnt, 'dateAdded'] = parser.parse(i)
#    cnt = cnt+1
cnt = 0
tempId = 0
rec = 0
cids = []
namarr = []
ori = []
for names in base['name']:
    if rec != names:
        namarr.append(names)
        rec = names
        
base = base.set_index('dateAdded')
ori.append(base['2014'])
ori.append(base['2015'])
ori.append(base['2016'])
ori.append(base['2017'])
ori.append(base['2018'])
checkisin = pd.Series()
totnam = [] 


for idx in ori:
    idx = idx.reset_index()
    idx = idx.sort_values(['name', 'dateAdded'], ascending=[True, True])
    idx = idx.reset_index()
    if idx.loc[1, 'dateAdded'].year != 2014:
        namser = pd.Series(namarr)
        totnam.append(namser)
        checkisin = idx['name'].isin(namser)
    cnt = 0
    arr = []
    tot = []
    for ids in idx['name']:
        Idrec = ids
        #if cnt == idx['companyId'].size:
        #    break
        if cnt == 0:
            if (not checkisin.empty):
                if checkisin[cnt] == True:
                    if idx.loc[cnt, 'dateAdded'].month != 1:
                        for it in range(idx.loc[cnt, 'dateAdded'].month - 1):
                            arr.append(0)
            rec = 1
            tempId = ids
            cnt = cnt + 1
            continue
        if tempId == ids:
            if idx.loc[cnt, 'dateAdded'].month == idx.loc[cnt - 1, 'dateAdded'].month:
                rec = rec + 1
            elif idx.loc[cnt, 'dateAdded'].month == idx.loc[cnt - 1, 'dateAdded'].month + 1:
                arr.append(rec)
                rec = 1
            else:
                arr.append(rec)
                rec = 1
                for x in range(idx.loc[cnt, 'dateAdded'].month - idx.loc[cnt - 1, 'dateAdded'].month - 1):
                    arr.append(0)
        else:
            cids.append(Idrec)
            Idrec = ids
            arr.append(rec)
            if (idx.loc[cnt - 1, 'dateAdded'].year == 2018):
                if (idx.loc[cnt - 1, 'dateAdded'].month != 6) :
                    mos = (5 - idx.loc[cnt - 1, 'dateAdded'].month)
                    for y in range(mos):
                        arr.append(0)
            else:
                mos = (12 - idx.loc[cnt - 1, 'dateAdded'].month)
                for y in range(mos):
                    arr.append(0) 
            tot.append(arr)
            arr = []
            if (not checkisin.empty):
                if checkisin[cnt] == True:
                    if idx.loc[cnt, 'dateAdded'].month != 1:
                        for it in range(idx.loc[cnt, 'dateAdded'].month - 1):
                            arr.append(0)
            rec = 1

        tempId = ids
        cnt = cnt + 1
    
    if not arr:
        arr.append(rec)
    if (idx.loc[idx['companyId'].size - 1, 'dateAdded'].year == 2018):
        if (idx.loc[idx['companyId'].size - 1, 'dateAdded'].month != 6) :
            mos = (6 - idx.loc[idx['companyId'].size - 1, 'dateAdded'].month)
            for y in range(mos):
                arr.append(0)
    else:
        mos = (12 - idx.loc[idx['companyId'].size - 1, 'dateAdded'].month)
        for y in range(mos):
            arr.append(0)
    tot.append(arr)
    
    namarr = idx['name'].agg(pd.unique)
    grandTot.append(tot)
    tot = []

totnam.append(pd.Series(namarr))    
    
k_source = []
labelArr = []
labelArrs = []
for arr in grandTot:
    stdev = []
    avg = []
    for arrs in arr:    
        stdev.append(np.std(arrs))
        avg.append(np.average(arrs))
    k_source.append(np.column_stack((avg,stdev)))
    
cnt = 1
for yrs in k_source:
    label = []
    cents = kmeans(yrs, 3)[0]
    label = vq(yrs, cents)[0] + 1
    labelArrs.append(label)
    plt.scatter(yrs[:,0], yrs[:,1], c=label)
    plt.show()
    cnt = cnt + 1
cnt = 1

for arr in totnam:
    d = {'Y' + str(cnt) + 'name' : arr, 'Y' + str(cnt) : labelArrs[cnt - 1]}
    labelArr.append(pd.DataFrame(data = d))
    cnt = cnt + 1

allName = pd.unique(base['name']).tolist()
finalData = pd.DataFrame()
finalData['name'] = allName
finalData = finalData.set_index('name')

cnt = 1
for year in labelArr:
    cnter = 0
    for entry in year['Y' + str(cnt) + 'name']:
        finalData.loc[entry, 'Y'+str(cnt)] = year.loc[cnter, 'Y'+str(cnt)]
        cnter = cnter + 1
    cnt = cnt + 1

finalData = finalData.fillna(0)
cnt = 0
yr = 1
finalData = finalData.reset_index()
churnTable = pd.DataFrame()
churnTable['name'] = allName
for col in finalData:
    if col == 'name':
        continue;
    if yr == 1:
        yr = yr + 1
        continue;
    cnt = 0
    for ents in finalData[col]:
        if finalData.loc[cnt, 'Y'+str(yr - 1)] != 0:
            if ents == 0:
                churnTable.loc[cnt, 'Y'+str(yr)] = 'churn'
            elif ents < (finalData.loc[cnt, 'Y'+str(yr - 1)] - 2):
                churnTable.loc[cnt, 'Y'+str(yr)] = 'churn'
            else:
                churnTable.loc[cnt, 'Y'+str(yr)] = 'good'
        else:
            if ents != 0:
                churnTable.loc[cnt, 'Y'+str(yr)] = 'start'
        cnt = cnt + 1
    yr = yr + 1

cRate = []
for years in churnTable:
    valid = churnTable[years].dropna()
    valid = valid[valid != 'start']
    cCount = 0
    for val in valid:
        if val == 'churn':
            cCount = cCount + 1
    cRate.append(cCount/len(valid))
    
churnTable.to_csv('churnVal.csv', encoding = 'utf-8')