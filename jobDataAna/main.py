
# %%
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import sqlalchemy
import re
#plot输出可以显示中文
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei'] 
mpl.rcParams['axes.unicode_minus'] = False 

settings = {'MYSQL_HOST': '127.0.0.1',
            'MYSQL_PORT': '3306',
            'MYSQL_USER': 'root',
            'MYSQL_PASSWORD': 'lnkyzhang',
            'MYSQL_DATABASE': 'job'}

tables = {'zhilian': 'zhilian', 'zhipin': 'zhipin', '51job': '51job'}

mydb = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
    settings['MYSQL_USER'], settings['MYSQL_PASSWORD'], settings['MYSQL_HOST'], settings['MYSQL_PORT'], settings['MYSQL_DATABASE']))

sql = 'select * from tablename'

df_original_zhilian = pd.read_sql_query(sql.replace('tablename',tables['zhilian']), mydb)
df_original_zhipin = pd.read_sql_query(sql.replace('tablename',tables['zhipin']), mydb)
df_original_51job = pd.read_sql_query(sql.replace('tablename',tables['51job']), mydb)

city_list = ['北京','上海','广州','深圳','杭州','成都','武汉','沈阳']
query_list = ['c%2B%2B', 'java', 'python', 'Hadoop', 'golang','html5', 'javascript', '机器学习', '图像处理', '机器视觉', '运维']

print("Init complete!")

# %%
#数据清洗：取消每个平台中每个城市的不同关键字下条目工资区间中位数排名的前后各5%
df_zhilian = df_original_zhilian
df_zhipin = df_original_zhipin
df_51job = df_original_51job

def clean_salary_zhilian(x):
    if re.search(r'\d+K-\d+K',x):
        salary_l = re.search(r'\d+(?=K-\d+K)',x).group(0)
        salary_h = re.search(r'(?<=-)\d+(?=K)',x).group(0)
        salary_m = (int(salary_l) + int(salary_h))/2
        return salary_m

def clean_salary_zhipin(x):
    if re.search(r'\d+k-\d+k',x):
        salary_l = re.search(r'\d+(?=k-\d+k)',x).group(0)
        salary_h = re.search(r'(?<=-)\d+(?=k)',x).group(0)
        salary_m = (float(salary_l) + float(salary_h))/2
        return salary_m

def clean_salary_51job(x):
    if x:
        if re.search(r'万/月',x):
            salary_l = re.search(r'[0-9]+([.]{1}[0-9]+){0,1}(?=-[0-9]+([.]{1}[0-9]+){0,1}万/月)',x).group(0)
            salary_h = re.search(r'(?<=-)[0-9]+([.]{1}[0-9]+){0,1}(?=万/月)',x).group(0)
            salary_m = ((float(salary_l) + float(salary_h))/2)*10
            return salary_m
        if re.search(r'万/年',x):
            salary_l = re.search(r'[0-9]+([.]{1}[0-9]+){0,1}(?=-[0-9]+([.]{1}[0-9]+){0,1}万/年)',x).group(0)
            salary_h = re.search(r'(?<=-)[0-9]+([.]{1}[0-9]+){0,1}(?=万/年)',x).group(0)
            salary_m = ((float(salary_l) + float(salary_h))/2)*10/12
            return salary_m
        return np.nan

def clean_location_51job(x):
    if x:
        if re.search(r'(北京-?\w*)|(上海-?\w*)|(广州-?\w+)|(深圳-?\w+)|(杭州-?\w+)|(成都-?\w+)|(武汉-?\w+)|(沈阳-?\w+)',x):
            location_sim = re.search(r'(北京(?=-?\w*))|(上海(?=-?\w*))|(广州(?=-?\w*))|(深圳(?=-?\w*))|(杭州(?=-?\w*))|(成都(?=-?\w*))|(武汉(?=-?\w*))|(沈阳(?=-?\w*))',x).group(0)
            return location_sim
        


def clean_zhilian():
    global df_zhilian
    df_zhilian['salary'] = df_original_zhilian['salary'].apply(clean_salary_zhilian)
    df_zhilian = df_zhilian.dropna(subset=['salary']).reset_index(drop = True)


def clean_zhipin():
    global df_zhipin
    df_zhipin['salary'] = df_original_zhipin['salary'].apply(clean_salary_zhipin)
    df_zhipin = df_zhipin.dropna(subset=['salary']).reset_index(drop = True)


def clean_51job():
    global df_51job
    df_51job['salary'] = df_original_51job['salary'].apply(clean_salary_51job)
    df_51job['location'] = df_original_51job['location'].apply(clean_location_51job)
    df_51job = df_51job[True ^ df_51job['location'].isin(['异地招聘'])].reset_index(drop = True)
    df_51job = df_51job.dropna(subset=['salary','location']).reset_index(drop = True)


clean_zhilian()
clean_zhipin()
clean_51job()

print('Clean complete!')

# %%

def delete_min_max_salary(input_df):

    global city_list
    global query_list

    d = pd.DataFrame()

    for i in city_list:
        list_temp_city = []
        list_temp_city.append(i)
        
        for j in query_list:
            list_temp_query = []
            list_temp_query.append(j)

            a = input_df[input_df['location'].isin(list_temp_city)].sort_values(by=['salary']).reset_index(drop = True)
            b = a[a['key_word'].isin(list_temp_query)].sort_values(by=['salary']).reset_index(drop = True)
            c = b.iloc[int(b.shape[0] * 0.1):int(b.shape[0] * 0.9)].reset_index(drop = True)
            d = d.append(c)
    return d.reset_index(drop=True)

df_zhilian_sim = delete_min_max_salary(df_zhilian)
df_zhipin_sim = delete_min_max_salary(df_zhipin)
df_51job_sim = delete_min_max_salary(df_51job)

# %%
#不同平台计算，并将结果保存到dataframe

#统计工资
def statistics_salary(input_df):

    global city_list
    global query_list

    d = pd.DataFrame(index = query_list,columns = city_list)

    for i in city_list:

        list_temp_city = []
        list_temp_city.append(i)

        for j in query_list:
            list_temp_query = []
            list_temp_query.append(j)

            d.iloc[query_list.index(j),city_list.index(i)] = input_df[input_df['location'].isin(list_temp_city) & input_df['key_word'].isin(list_temp_query)]['salary'].mean()
    return d

#统计工作数量
def statistics_jobcount(input_df):

    global city_list
    global query_list

    d = pd.DataFrame(index = query_list,columns = city_list)

    for i in city_list:

        list_temp_city = []
        list_temp_city.append(i)

        for j in query_list:
            list_temp_query = []
            list_temp_query.append(j)

            d.iloc[query_list.index(j),city_list.index(i)] = input_df[input_df['location'].isin(list_temp_city) & input_df['key_word'].isin(list_temp_query)]['salary'].shape[0]
    return d

statistics_salary(df_zhilian_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title="zhilian_salary")
statistics_salary(df_zhipin_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title="zhipin_salary")
statistics_salary(df_51job_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title="51job_salary")

statistics_jobcount(df_zhilian_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title="zhilian_count")
statistics_jobcount(df_zhipin_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title='zhipin_count')
statistics_jobcount(df_51job_sim).plot(xticks=[0,1,2,3,4,5,6,7,8,9,10],figsize=(10,10),title = '51job_count')