# -*- coding:utf-8 -*-
import xlrd
from numpy import *
import MySQLdb
import pandas as pd
import sys
import os
import time
import datetime
import numpy as np
import math
import json
import traceback
from pymongo import MongoClient
from math import*
reload(sys)
sys.setdefaultencoding('utf-8')
class StoreMongo(object):
    def __init__(self, host, port, database, collection,user,password):
        self.client = MongoClient(host, port)
        self.database = database
#         print host,port,database
        self.client[database].authenticate(user,password)
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

def phoneguifan(phnelist):
    molist = []
    for p in phnelist:
        try:
            p = str(p)
            p = p.replace(' ', '').replace('+86', '').replace('-', '')
            if len(p) == 13 and p[:2] == '86':
                p = p[2:]
            molist.append(p)
        except Exception, e:
            sys.stderr.write('exp: ' + str(e) + '\t' + str(p) + '\n')
            continue
    return molist
def contactphone(bankmolist,cuismolist,uc):
    bankmocnt = 0
    cuishoumocnt = 0
    for c in uc:
        try:
            n = c['name']
            plist = c['phone']
            new_plist = []
            for p in plist:
                p = p.replace(' ', '').replace('+86', '').replace('-', '')
                if len(p) == 13 and p[:2] == '86':
                    p = p[2:]
                if p in bankmolist:
                    bankmocnt += 1
                if p in cuismolist:
                    cuishoumocnt += 1
                    # 规范化后的电话号码
                    #                             new_plist.append(p.encode('utf-8'))
        except Exception, e:
            sys.stderr.write('exc1 ' + str(e) + '\n')
            continue
    return bankmocnt,cuishoumocnt
def e2_wecash(bankmolist,cuismolist,ud):
    callinfo = ud['data']['transportation'][0]['origin']['callInfo']['data']
    bankmocnt = 0
    cuishoumocnt = 0
    bankmaxcnt={}
    cuishoumaxcnt={}
    for c in callinfo:
        if c['code'] != 'E000000':
            continue
        for cd in c['details']:
            p = cd['anotherNm']

            if p in bankmolist:
                bankmocnt += 1
                st = cd['startTime']
                if st == '':
                    st = '2016-11-27 9:40:03'
                elif st.find('-') == 2:
                    st = '2016-' + st
                elif st.find('-') > 0 and st.find('/') > 0:
                    st = st.split('-')[1]
                elif len(st.split('-')) == 4:
                    st = '-'.join(st.split('-')[1:])
                stime=st[0:7]
                if stime in bankmaxcnt.keys():
                    bankmaxcnt[stime]+=1
                else:
                    bankmaxcnt.setdefault(stime,1)
                # print bankmocnt
            if p in cuismolist:
                cuishoumocnt += 1
                st = cd['startTime']
                if st == '':
                    st = '2016-11-27 9:40:03'
                elif st.find('-') == 2:
                    st = '2016-' + st
                elif st.find('-') > 0 and st.find('/') > 0:
                    st = st.split('-')[1]
                elif len(st.split('-')) == 4:
                    st = '-'.join(st.split('-')[1:])
                stime = st[0:7]
                if stime in cuishoumaxcnt.keys():
                    cuishoumaxcnt[stime] += 1
                else:
                    cuishoumaxcnt.setdefault(stime, 1)
    if len(bankmaxcnt)>0:
        bankmaxcnts = max(bankmaxcnt.values())
    else:
        bankmaxcnts=0
    if len(cuishoumaxcnt)>0:
        cuishoumaxcnts=max(cuishoumaxcnt.values())
    else:
        cuishoumaxcnts=0
    # print bankmaxcnts
    # print bankmocnt
    return bankmocnt,cuishoumocnt,bankmaxcnts,cuishoumaxcnts
def e2_lhp(bankmolist,cuismolist,ud):
    callinfo = ud['phoneList'][0]['telData']
    bankmocnt = 0
    cuishoumocnt = 0
    bankmaxcnt = {}
    cuishoumaxcnt = {}
    for c in callinfo:
        p = c['receiverPhone']
        if p in bankmolist:
            bankmocnt += 1
            st = c['cTime']
            if st == '':
                st = '2016-11-27 9:40:03'
            elif st.find('-') == 2:
                st = '2016-' + st
            elif st.find('-') > 0 and st.find('/') > 0:
                st = st.split('-')[1]
            elif len(st.split('-')) == 4:
                st = '-'.join(st.split('-')[1:])
            stime = st[0:7]
            if stime in bankmaxcnt.keys():
                bankmaxcnt[stime] += 1
            else:
                bankmaxcnt.setdefault(stime, 1)
        if p in cuismolist:
            cuishoumocnt += 1
            st = c['cTime']
            if st == '':
                st = '2016-11-27 9:40:03'
            elif st.find('-') == 2:
                st = '2016-' + st
            elif st.find('-') > 0 and st.find('/') > 0:
                st = st.split('-')[1]
            elif len(st.split('-')) == 4:
                st = '-'.join(st.split('-')[1:])
            stime = st[0:7]
            if stime in cuishoumaxcnt.keys():
                cuishoumaxcnt[stime] += 1
            else:
                cuishoumaxcnt.setdefault(stime, 1)
    if len(bankmaxcnt)>0:
        bankmaxcnts = max(bankmaxcnt.values())
    else:
        bankmaxcnts=0
    if len(cuishoumaxcnt)>0:
        cuishoumaxcnts=max(cuishoumaxcnt.values())
    else:
        cuishoumaxcnts=0
    # print bankmocnt
    return bankmocnt, cuishoumocnt, bankmaxcnts, cuishoumaxcnts
def lianxi(u,c_dir,d_dir,c_dic,d_dic,bankmolist,cuismolist):
            if (u in c_dic) and (u in d_dic):
                with open(d_dir + '/' + u, 'r') as fp:
                    ud = fp.readline()
                with open(c_dir + '/' + u, 'r') as fp:
                    uc = fp.readline()
                try:
                    ud = json.loads(ud)
                    uc = json.loads(uc)
                # 处理联系人
                    bankmocnt, cuishoumocnt=contactphone(bankmolist,cuismolist,uc)
                    if 'code' in ud:
                        # wecash
                        bankmocnts, cuishoumocnts, bankmaxcnts, cuishoumaxcnts = e2_wecash(bankmolist,cuismolist,ud)
                    else:
                        # lhp
                        # 以最大城市作为本地
                        bankmocnts, cuishoumocnts, bankmaxcnts, cuishoumaxcnts = e2_lhp(bankmolist,cuismolist,ud)
                    lianxiinfor=[bankmocnt,cuishoumocnt,bankmocnts, cuishoumocnts,bankmaxcnts, cuishoumaxcnts]
                    return lianxiinfor
                except Exception, e:
                    sys.stderr.write('exp: ' + str(e) + '\t' + u + '\n')
                    print 'exec: ',  traceback.print_exc()
                    return [-1.0,-1.0,-1.0,-1.0,-1.0,-1.0]
            else:
                return [-1.0,-1.0,-1.0,-1.0,-1.0,-1.0]
                #break
                # print bankmocnt,cuishoumocnt
                #     print bankcontactcnt
def Distance2(lat1,lng1,lat2,lng2):# 第二种计算方法
    radlat1=radians(lat1)
    radlat2=radians(lat2)
    a=radlat1-radlat2
    b=radians(lng1)-radians(lng2)
    s=2*asin(sqrt(pow(sin(a/2),2)+cos(radlat1)*cos(radlat2)*pow(sin(b/2),2)))
    earth_radius=6378.137
    s=s*earth_radius
    if s<0:
        return -s
    else:
        return s
def get_distance(obj,ti,moblie):
    types = ['1', '3', '5']
    locationlist = {}
    for j in types:
        # rs = obj.search_data({'$and':[{'userId':i},{'type':j}]})
        rs = obj.search_data({'_id': moblie+'|'+str(ti)+'|'+str(j)})
        if rs.count() == 0:
            locationlist[j]=[float(0),float(0)]
        else:
            rsl = list(rs)
            locationlist[j]=[float(rsl[0]['location']['coordinates'][0]),float(rsl[0]['location']['coordinates'][1])]
    juzhumat=locationlist['1']
    gongzuomat=locationlist['3']
    renzhengmat = locationlist['5']
    return juzhumat,gongzuomat,renzhengmat
def f():
    host = '172.16.51.13'
    port = 3717
    user = 'xuyonglong'
    password = 'MDkoWEYN3YhBNJpNLyVksdZA'
    database = 'risk'
    collection = 'risk_user_lbs'
    #     output_file = '/home/xuyonglong/feature/working/experiment/20170320/feature/fea_lianxi'
    #     userids=[26982]
    userids = [1471877]
    # 样本
    s_file = '/home/xuyonglong/feature/working/experiment/20170320/sample/s_30_new'
    # 联系人
    c_dir = '/home/xuyonglong/feature/working/experiment/20170320/raw/contacts'
    # 通讯详情
    d_dir = '/home/xuyonglong/feature/working/experiment/20170320/raw/details'
    output_file = '/home/xuyonglong/feature/working/experiment/20170320/feature/fea_huangyeAndjuli'
    bankyellowdf = pd.read_excel('/home/xuyonglong/银行黄页.xlsx')
    cuishouyellowdf = pd.read_excel('/home/xuyonglong/cuihsouhuangye.xlsx')
    bankyellowdf.columns = ['source', 'phone']
    cuishouyellowdf.columns = ['source', 'phone']
    bankmobile = bankyellowdf['phone'].values.tolist()
    cuishoumobile = cuishouyellowdf['phone'].values.tolist()
    #     银行黄页
    bankmolist = phoneguifan(bankmobile)
    #      催收及同行黄页
    cuismolist = phoneguifan(cuishoumobile)

    obj = StoreMongo(host, port, database, collection, user, password)
    conn = MySQLdb.connect(host="172.16.51.13", port=65000, user="xuyonglong", passwd="MDkoWEYN3YhBNJpNLyVksdZA",
                           db="risk", charset="utf8")
    cursor = conn.cursor()
    # '1':u'现居住地(手填)','2':u'现居住地(GPS)','3':u'工作地址(手填)','4':u'工作地址(GPS)','5':u'提交认证地址(GPS)'
    locdislist = []
    #     wfp = open(output_file, 'w')
    uid_dic = {}
    wfp = open(output_file, 'w')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            uid_dic[linelist[0]] = 0
        c_files = os.listdir(c_dir)
        d_files = os.listdir(d_dir)
        # 联系人数组
        c_dic = {}
        # 通讯详单数组
        d_dic = {}
        for i in range(len(c_files)):
            c_dic[c_files[i]] = 0
        for i in range(len(d_files)):
            d_dic[d_files[i]] = 0
        # wfp = open(output_file, 'w')
        for i in uid_dic:
            print 'userid:'+str(i)
            lianxireninformation=lianxi(i,c_dir,d_dir,c_dic,d_dic,bankmolist,cuismolist)
            print "lianxireninformation"
            print lianxireninformation
            sql = 'select t.id, t.userName from risk_auth_credit_task t where t.userId =' + str(i)
            tfn = cursor.execute(sql)
            if tfn == 0:
                juzhumat, gongzuomat, renzhengmat=[0.0,0.0],[0.0,0.0],[0.0,0.0]
            ul=cursor.fetchall()[0]
            juzhumat, gongzuomat, renzhengmat = get_distance(obj,ul[0],ul[1])
            jg=Distance2(juzhumat[0],juzhumat[1],gongzuomat[0],gongzuomat[1])
            jr = Distance2(juzhumat[0], juzhumat[1], renzhengmat[0], renzhengmat[1])
            gr = Distance2(gongzuomat[0], gongzuomat[1], renzhengmat[0], renzhengmat[1])
            print '距离'
            print jg, jr, gr
            #         重要联系人认证人数
            renzhengcnt = 0
            #     重要联系人作为系统已认证人的重要联系人次数
            zhongyaocnt = 0

            juzhurz=0.0
            gongzuorz=0.0
            sql = 'select contactMobile1,contactMobile2 from risk_auth_contact_person where userId=' + str(i)
            n = cursor.execute(sql)
            if n == 0:
                continue
            mobiles = cursor.fetchall()
            #         print mobiles
            zhongyaoinformation = [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0]
            userinfromiation=[]
            for r in mobiles[0]:
                if r == None:
                    lianxireninformation.extend(zhongyaoinformation)
                    userinfromiation = [jg, jr, gr, renzhengcnt, zhongyaocnt, juzhurz, gongzuorz]
                    userinfromiation.extend(lianxireninformation)
                    for index, item in enumerate(userinfromiation):
                        userinfromiation[index] = str(item)
                    print "信息"
                    print userinfromiation
                    wfp.write(str(i) + '\t' + '\t'.join(userinfromiation) + '\n')
                    continue
                    #             print r
                sql = 'select t.userId from risk_auth_credit_task t where t.authType like "%basic%" AND t.status like "%pass%" and t.userName ="' + r + '"'
                tfn = cursor.execute(sql)
                if tfn > 0:
                    renzhengcnt += 1
                    zhongyaouserid = cursor.fetchall()
                    # 使用第一个重要联系人的
                    if zhongyaouserid[0][0]!=None and renzhengcnt==0:
                        zhongyaoinformation=lianxi(zhongyaouserid, c_dir, d_dir, c_dic, d_dic, bankmolist, cuismolist)
                sql = 'SELECT userId from risk_auth_contact_person where userId!=' + str(
                    i) + ' and (contactMobile1="' + r + '" or contactMobile2="' + r + '")'
                n = cursor.execute(sql)
                if n == 0:
                    continue
                ui = cursor.fetchall()
                for u in ui[0]:
                    sql = 'select t.userId from risk_auth_credit_task t where t.authType like "%basic%" AND t.status like "%pass%" and t.userId =' + str(
                        u) + ''
                    n = cursor.execute(sql)
                    if n > 0:
                        zhongyaocnt += 1
                        #         print renzhengcnt
                        #         print zhongyaocnt
                        #         重要联系人1的单位距离与认证人的单位距离
            print 'zhongyaoinformation'
            print zhongyaoinformation
            lianxireninformation.extend(zhongyaoinformation)
            sql = 'select userId from risk_auth_credit_task where userName ="' + mobiles[0][0] + '"'
            n = cursor.execute(sql)
            if n == 0:
                userinfromiation = [ jg, jr, gr, renzhengcnt, zhongyaocnt, juzhurz, gongzuorz]
                userinfromiation.extend(lianxireninformation)
                for index, item in enumerate(userinfromiation):
                    userinfromiation[index] = str(item)
                print "信息"
                print userinfromiation
                wfp.write(str(i) + '\t' + '\t'.join(userinfromiation) + '\n')
                continue
            sql = 'select t.id,t.userName from risk_auth_credit_task t where t.userId =' + str(i)
            tfn = cursor.execute(sql)
            if tfn == 0:
                juzhumat, gongzuomat, renzhengmat = [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]
            ul=cursor.fetchall()[0]
            juzhumatzhongyao, gongzuomatzhongyao, renzhengmatzhongyao = get_distance(obj, ul[0],ul[1])
            juzhurz = Distance2(juzhumat[0], juzhumat[1], juzhumatzhongyao[0], juzhumatzhongyao[1])
            gongzuorz = Distance2(gongzuomat[0], gongzuomat[1], gongzuomatzhongyao[0], gongzuomatzhongyao[1])
            # print juzhurz, gongzuorz
            userinfromiation=[jg, jr, gr, renzhengcnt, zhongyaocnt, juzhurz, gongzuorz]
            userinfromiation.extend(lianxireninformation)
            for index, item in enumerate(userinfromiation):
                userinfromiation[index] = str(item)
            print "信息"
            print userinfromiation
            wfp.write(str(i) + '\t' +'\t'.join(userinfromiation) + '\n')
    wfp.close()
            # locdislist.append(userinfromiation)
if __name__ == '__main__':
    f()
    # lianxi()
    # zhongyao()


