#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import create_engine
import time
import cx_Oracle as cx;


# In[7]:


oracle_test_host= host
oracle_test_port='1521'
oracle_test_serviceName=serviceName
oracle_test_conn_User=user
oracle_test_conn_Pass=password


# In[8]:


oracle_conn_test=f"oracle+cx_oracle://{oracle_test_conn_User}:{oracle_test_conn_Pass}@{oracle_test_host}:{oracle_test_port}/?service_name={oracle_test_serviceName}"


# In[9]:


order_sql_test='''select distinct
campaign_subscriber_id,
CAMPAIGN_INDV_CUSTOMER_ID,
contract_type_id,
subscriber_Status_id,
P2P_VOICE_DAY_COUNT,
SUBSCRIBER_TENURE_DAYS,
subs_postpaid_tenure_months
from campaign.EOM_CAMPAIGN_SUBS_HISTORY
where monthly_calendar_id = TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYYMM')) and
CHARGING_METHOD_ID = 1 AND ---faturalÄ±
subscriber_Status_id = 2 AND --- aktif
SUBSCRIBER_TYPE_GROUP_ID = 158 AND --- abone tipi: bireysel
CONTRACT_TYPE_ID in (1,41) AND --- ses ve data
CAMPAIGN_INDV_CUSTOMER_ID <> -1 AND CAMPAIGN_INDV_CUSTOMER_ID IS NOT null and
campaign_subscriber_id <> -1 AND campaign_subscriber_id IS NOT NULL AND rownum <= 1000000
order by campaign_subscriber_id'''

order_train_test='''SELECT * FROM TCRBAYRAKTAR.RISK_SCORE_202001_TRA 
where rownum <=1000'''


# In[10]:


def sqlConnection(order_train_test,oracle_conn_test,rownum_init,rownum_finish):
    order_train_test=f'''SELECT * FROM {test_table}
    where rownum > {rownum_init} and rownum< {rownum_finish}'''
    start=time.time()
    orders=pd.read_sql_query(order_train_test,oracle_conn_test)
    finish=time.time()
    result=finish-start
    print(f'DB read time : {result}')
    return result,orders


# In[11]:


result,orders=sqlConnection(order_train_test,oracle_conn_test,0,100000)


# In[12]:


orders


# In[123]:


import threading  # threading modülünü import ettik
t1 = threading.Thread(target=sqlConnection, args=(order_train_test,oracle_conn_test,0,1000))  # threadi tanımladık ve f fonksiyonunu hedef gösterdik
t1.start()  # threadi çalıştırdık
t2 = threading.Thread(target=sqlConnection, args=(order_train_test,oracle_conn_test,1000,2000))
t2.start()


# In[88]:


def loopthreading(x,y,z):
    orders_total=pd.DataFrame()
    for i in range(0,10):
        i= threading.Thread(target=sqlConnection, args=(order_train_test,oracle_conn_test,x,y))  # threadi tanımladık ve f fonksiyonunu hedef gösterdik
        result,orders=i.start()
        print(orders.result)
        x+=z
        y+=z
        orders_total+=orders.result
    return orders


# In[119]:


start=time.time()
orders=loopthreding(0,10000,10000)
print(orders)
finish=time.time()
result=finish-start
print(f'total loop time : {result}')


# In[1]:


import multiprocessing

multiprocessing.cpu_count() # or os.cpu_count()


# In[ ]:




