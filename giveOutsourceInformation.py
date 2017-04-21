# -*- coding:utf-8 -*-
from pymongo import MongoClient
import pandas as pd
import time
import MySQLdb
import xlrd
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
if __name__ == '__main__':
    host = '172.16.51.11'
    port = 27017
    database = 'risk'
    collection = 'risk_user_lbs'
    obj = StoreMongo(host, port, database, collection)

    conn=MySQLdb.connect(host="172.16.51.60",user="test",passwd="test",db="cui",charset="utf8")


    # sql="select * from cui_task limit 2"
    sql="SELECT ct.userId,u.`name` AS '经办人',ct.`id` AS '任务号',CONCAT('#',b.`Id`) AS '账单号',a.`realName` AS '真实姓名',CONCAT('#',a.`cardId`) AS '身份证号',rac.`sex` AS '性别',rac.`age` AS '年龄',a.`mobile` AS '手机号',acb.`mobile` AS '银行预留',rac.`address` AS '身份证地址',bus.`addressInfo` AS '购物地址',bus.invoiceInfo AS '发票抬头',t.`createDate` AS '借款时间',b.`repayEndDate` AS '还款日期',t.`loanDays` AS '借款周期',b.`overdueDays` AS '逾期天数',t.`amount` AS '借款金额',t.feeAmount AS '服务费',(b.lateFeeAmount + b.penaltyAmount) AS '滞纳金+违约金',(b.principalAmount + b.interestAmount + b.lateFeeAmount + b.penaltyAmount) AS '总欠款',b.cuiLateFeeDiscount AS '减免',t.`buyerBankName` AS '开户行',CONCAT(LEFT(t.`buyerBankCard`,4),'****',RIGHT(t.buyerBankCard,4)) AS '收款账号' FROM cui.`cui_task` ct LEFT JOIN cui.`cui_task_bill` ctb ON ctb.`taskId` = ct.`id` LEFT JOIN sudaibear.`bc_bill` b ON b.`Id` = ctb.`billId` LEFT JOIN sudaibear.`ac_account` a ON a.`userId` = ct.`userId` LEFT JOIN risk.`risk_auth_cardid` rac ON rac.`userId` = ct.`userId` LEFT JOIN sudaibear.`tc_trade` t ON t.`id` = b.`tradeId` LEFT JOIN sudaibear.`ac_account_bindcard` acb ON acb.`id` = t.`buyerBindCardId` LEFT JOIN risk.risk_auth_rongjd_bus bus ON bus.`userId` = ct.`userId` AND bus.`isValid` = 1 LEFT JOIN sudaibear.`uc_users` u ON u.`id` = ct.`assignUserId` WHERE ct.`status` = 475 AND b.`status` = 340 and ct.assignDate BETWEEN DATE(SYSDATE()) and DATE(DATE_ADD(SYSDATE(),INTERVAL 1 day))"
    # sql="SELECT ct.userId,u.`name` AS '经办人',ct.`id` AS '任务号',CONCAT('#',b.`Id`) AS '账单号',a.`realName` AS '真实姓名',CONCAT('#',a.`cardId`) AS '身份证号',rac.`sex` AS '性别',rac.`age` AS '年龄',a.`mobile` AS '手机号',acb.`mobile` AS '银行预留',rac.`address` AS '身份证地址',bus.`addressInfo` AS '购物地址',bus.invoiceInfo AS '发票抬头',t.`createDate` AS '借款时间',b.`repayEndDate` AS '还款日期',t.`loanDays` AS '借款周期',b.`overdueDays` AS '逾期天数',t.`amount` AS '借款金额',t.feeAmount AS '服务费',(b.lateFeeAmount + b.penaltyAmount) AS '滞纳金+违约金',(b.principalAmount + b.interestAmount + b.lateFeeAmount + b.penaltyAmount) AS '总欠款',b.cuiLateFeeDiscount AS '减免',t.`buyerBankName` AS '开户行',CONCAT(LEFT(t.`buyerBankCard`,4),'****',RIGHT(t.buyerBankCard,4)) AS '收款账号' FROM cui.`cui_task` ct LEFT JOIN cui.`cui_task_bill` ctb ON ctb.`taskId` = ct.`id` LEFT JOIN sudaibear.`bc_bill` b ON b.`Id` = ctb.`billId` LEFT JOIN sudaibear.`ac_account` a ON a.`userId` = ct.`userId` LEFT JOIN risk.`risk_auth_cardid` rac ON rac.`userId` = ct.`userId` LEFT JOIN sudaibear.`tc_trade` t ON t.`id` = b.`tradeId` LEFT JOIN sudaibear.`ac_account_bindcard` acb ON acb.`id` = t.`buyerBindCardId` LEFT JOIN risk.risk_auth_rongjd_bus bus ON bus.`userId` = ct.`userId` AND bus.`isValid` = 1 LEFT JOIN sudaibear.`uc_users` u ON u.`id` = ct.`assignUserId`"
    testdf=pd.read_sql(sql,conn)
    testdf=testdf.head()
    userids = testdf['userId'].values.tolist()
    types={'1':u'现居住地(手填)','2':u'现居住地(GPS)','3':u'工作地址(手填)','4':u'工作地址(GPS)','5':u'提交认证地址(GPS)'}
    # typename=[u'现居住地(手填)',u'现居住地(GPS)',u'工作地址(手填)',u'工作地址(GPS)',u'提交认证地址(GPS)']
    for i in types:
        mongdict=[]
        for userid in userids:
            rs = obj.search_data({'$and':[{'userId':userid},{'type':i}]})
            if rs.count()==0:
                continue
            rsl=list(rs)
            try:
                mongdict.append([userid,rsl['address']])
            except:
                print userid
        mongdf=pd.DataFrame(mongdict,columns=['userid',types[i]])
        testdf=pd.merge(left=testdf,right=mongdf,how='left',left_on='userId',right_on='userid')
        del testdf['userid']
        print testdf.head()
    testdf.to_excel('yuqu.xlsx')
    # print rs.count()
    # rsl = list(rs)
    # print len(rsl)


