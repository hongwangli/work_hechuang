# -*- coding:utf-8 -*-
from pymongo import MongoClient
from numpy import *
import MySQLdb
import pandas as pd
class StoreMongo(object):
    def __init__(self, host, port, database, collection):
        self.client = MongoClient(host, port)
        self.database = database
        self.collection = collection
    # 获得一个游标
    def get_cursor(self):
        db = self.client[self.database]
        collection = db[self.collection]
        return collection
    # 存储
    def store_data(self, dataset):
        collection = self.get_cursor()
        for data in dataset:
            collection.save(data)
    # 更新
    def upsert(self, con, dic):
        collection = self.get_cursor()
        collection.update(con, dic, True)
    # 查询结果数量
    def results_count(self, query):
        collection = self.get_cursor()
        return collection.find(query).count()
    # 查询数据
    def search_data(self, query,no_cursor_timeout=True,limit={"_id": 0}):
        rs = self.client[self.database][self.collection].find(query, )
        return rs
    # 查询表里总数据量
    def collection_count(self):
        return self.client[self.database][self.collection].count()
    # 获取地址经纬度
    def get_distance(self,i):
        for j in types:
            rs = obj.search_data({'$and':[{'userId':i},{'type':j}]})
            if rs.count() == 0:
                locationlist[j]=[float(0),float(0)]
            else:
                rsl = list(rs)
                locationlist[j]=[float(rsl[0]['location']['coordinates'][0]),float(rsl[0]['location']['coordinates'][1])]
        juzhumat=mat(locationlist['2'])
        gongzuomat=mat(locationlist['4'])
        renzhengmat = mat(locationlist['5'])
        return juzhumat,gongzuomat,renzhengmat
if __name__ == '__main__':
    host = '172.16.51.11'
    port = 27017
    database = 'risk'
    collection = 'risk_user_lbs'
    userids=[485369]
    obj = StoreMongo(host, port, database, collection)
    conn = MySQLdb.connect(host="172.16.51.60", user="test", passwd="test", db="cui", charset="utf8")
    # '1':u'现居住地(手填)','2':u'现居住地(GPS)','3':u'工作地址(手填)','4':u'工作地址(GPS)','5':u'提交认证地址(GPS)'
    types =['2','4','5']
    locdislist=[]
    for i in userids:
        locationlist = {}
        juzhumat,gongzuomat,renzhengmat=obj.get_distance(i)
        jg=sqrt((juzhumat-gongzuomat)*(juzhumat-gongzuomat).T)
        jr=sqrt((juzhumat-renzhengmat)*(juzhumat-renzhengmat).T)
        gr=sqrt((gongzuomat-renzhengmat)*(gongzuomat-renzhengmat).T)
        # print jg,jr,gr


