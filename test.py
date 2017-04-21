# -*- coding:utf-8 -*-
from pymongo import MongoClient
import pandas as pd
import time


class StoreMongo(object):
    def __init__(self, host, port, database, collection, user, password):
        self.client = MongoClient(host, port)
        self.database = database
        print host, port, database
        self.client[database].authenticate(user, password)
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
    def search_data(self, query, no_cursor_timeout=True, limit={"_id": 0}):
        rs = self.client[self.database][self.collection].find(query, )
        return rs

    # 查询表里总数据量
    def collection_count(self):
        return self.client[self.database][self.collection].count()


if __name__ == '__main__':
    host = '172.16.51.13'
    port = 3717
    user = 'xuyonglong'
    password = 'MDkoWEYN3YhBNJpNLyVksdZA'
    database = 'risk'
    collection = 'risk_auth_tongdun'
    obj = StoreMongo(host, port, database, collection, user, password)
    exceldf = pd.read_csv('/Users/ufenqi/Desktop/tongdun.csv', names=['id', 'userid'])
    # exceldf=exceldf.tail(100)
    excelid = exceldf['id'].values.tolist()
    exceluserid = exceldf['userid'].values.tolist()
    mongdict = []

    # rs = obj.search_data({'_id': '18708747620|APP'})
    # print rs.count()
    for i in range(0,exceldf.__len__()):
        # print exceldf['id'][i]
        rs = obj.search_data({'_id':str(excelid[i])})
        if rs.count() == 0:
            continue
        rsl = list(rs)
        # print exceldf['id'][i]
        try:
            mongdict.append([excelid[i], exceluserid[i], rsl[0]['idCardCity'], rsl[0]['idCardCounty'],
                             rsl[0]['idCardProvince'], rsl[0]['mobileAddressCity'], rsl[0]['mobileAddressProvince']])
        except:
            print exceldf['id'][i]
    mongdf = pd.DataFrame(mongdict,
                          columns=['id', 'userid', 'idCardCity', 'idCardCounty', 'idCardProvince', 'mobileAddressCity',
                                   'mobileAddressProvince'])
    print mongdf.head()
    mongdf.to_excel('/Users/ufenqi/Desktop/tongdunmongodb.xlsx',index=False)

