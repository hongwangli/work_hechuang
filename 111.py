#-*- coding: utf-8 -*-
import sys
import json
import requests
import time
import pandas as pd
def g(pii):
    r = requests.get('http://jdjk.test.ufenqi.com/user/getUser?userId=%s' % pii)
    a = r.text
    a = json.loads(a)
    if a['code'] == '200':
        return  a['data']
def getuserid(excelpath):
    exdf = pd.read_excel(excelpath, index_col=None)
    # print exdf.head()
    aa=exdf.head(10)
    useridlist = aa['user_id'].tolist()
    return useridlist


if __name__ == '__main__':
    # useridlist = getuserid('/Users/ufenqi/Desktop/优分期成人进件USERID.xlsx')
    exdf = pd.read_excel('/Users/ufenqi/Desktop/优分期成人进件USERID.xlsx', index_col=None)
    useridlist = exdf['user_id'].tolist()
    mobiles = []
    credateDates = []
    cardIds = []
    for i in useridlist:
        data = g(i)
        mobiles.append(data['mobile'])
        credateDates.append(data['credateDate'])
        cardIds.append(data['cardId'])
        if len(mobiles)>=5:
            datas = {'mobiles': mobiles, 'credateDates': credateDates, 'cardIds': cardIds}
            df = pd.DataFrame(datas)
            df.to_csv('users.csv', mode='a', sep=',', header=False,index=None)
            mobiles = []
            credateDates = []
            cardIds = []
    datas = {'mobiles': mobiles, 'credateDates': credateDates, 'cardIds': cardIds}
    df = pd.DataFrame(datas)
    df.to_csv('users.csv', mode='a',sep=',',header=False,index=None)