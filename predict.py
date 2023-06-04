import pymysql
from keras.models import load_model
import requests
import time
import execjs
import keras
from keras.models import Sequential
from keras.layers import Dense, LSTM, Dropout
from numpy import array
import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt


def getUrl(fscode):
    head = 'http://fund.eastmoney.com/pingzhongdata/'
    tail = '.js?v=' + time.strftime("%Y%m%d%H%M%S", time.localtime())
    return str(head + fscode + tail)


def getWorth(fscode):
    # 用requests获取到对应的文件
    content = requests.get(getUrl(fscode))

    print(getUrl(fscode))
    # 使用execjs获取到相应的数据
    jsContent = execjs.compile(content.text)

    name = jsContent.eval('fS_name')
    code = jsContent.eval('fS_code')
    # 单位净值走势
    netWorthTrend = jsContent.eval('Data_netWorthTrend')
    # 累计净值走势
    ACWorthTrend = jsContent.eval('Data_ACWorthTrend')

    netWorth = []
    ACWorth = []

    # 提取出里面的净值
    for dayWorth in netWorthTrend[::-1]:
        netWorth.append(dayWorth['y'])

    for dayACWorth in ACWorthTrend[::-1]:
        ACWorth.append(dayACWorth[1])
    print(name, code)
    return netWorth, ACWorth


# 将预测结果保存至数据库
def save_result(netWorth, growth, product_id):
    # 打开数据库连接
    db = pymysql.connect(host='localhost', port=3306, user='root', password='2514632453',
                         db='fundtrans', charset='utf8mb4')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    delete_prediction = "delete from prediction where product_id='"+product_id+"'"
    cursor.execute(delete_prediction)
    insert_value = "INSERT INTO prediction (product_id, time, net_worth, growth) VALUES (%s, %s, %s,%s)"
    cursor.execute(insert_value, tuple([product_id, '未来1天', netWorth[0], growth[0]]))
    cursor.execute(insert_value, tuple([product_id, '未来2天', netWorth[1], growth[1]]))
    cursor.execute(insert_value, tuple([product_id, '未来3天', netWorth[2], growth[2]]))
    db.commit()
    cursor.close()  # 关闭游标
    db.close()  # 关闭数据库连接


def getGrowth(netWorth):
    growth = []
    for i in range(0, len(netWorth)-1):
        nw1 = netWorth[i]
        nw2 = netWorth[i+1]
        g = (nw2-nw1)/nw1 * 100
        growth.append(g)
    return growth


fundCode = "000006"
# back_days = -5
netWorth, ACWorth = getWorth(fundCode)
print("所获得的基金净值格式如下所示（按时间由近到远排序）：")
print(netWorth[:10])
print("共有"+str(len(netWorth))+"个数据")
mydata = netWorth[::-1]

# data = array(mydata)
# data = data.reshape(1,len(mydata),1)
data = mydata
i = len(data) - 96
x = []
y = []
while i < len(data) - 16:
    x.append(data[i:i + 15])
    y.append(data[i + 16])
    i = i + 1

features_set, labels = np.array(x), np.array(y)
# back_labels = labels[back_days:]
# labels = labels[:back_days]
features_set = np.reshape(features_set, (features_set.shape[0], features_set.shape[1], 1))
model = load_model('model/model_' + fundCode + '.h5')
predictY = model.predict(features_set)
print(features_set.shape)
last = features_set[len(features_set) - 1].reshape(1, 15, 1)
print(last.shape)

list = []
i = 0
while i < 3:
    templabel = model.predict(last)
    list.append(float(templabel))
    last = last.reshape(15)
    last = np.delete(last, 0)

    last = np.append(last, templabel)

    last = last.reshape(1, 15, 1)
    i = i + 1

list = np.array(list)
list.dtype = "float"

list = np.reshape(list, (len(list), 1))
predictY = np.vstack((predictY, list))
predictY = predictY[2:]
# labels = np.hstack((labels, back_labels))
labels = labels[:-2]
predictY = predictY.T[0]
print("最近5天净值预测值：")
print(predictY[-5:])
print("最近5天净值真实值：")
print(np.hstack((labels[-2:], [None, None, None])))

plt.rcParams['font.sans-serif']='SimHei'
plt.rcParams['axes.unicode_minus']=False

plt.figure(figsize=(10, 5))
plt.plot(labels, color='blue', label='基金每日净值')
plt.plot(predictY, color='red', label='预测每日净值')
plt.legend(loc='upper left')
plt.title("净值预测 基金代码：{}".format(fundCode))
plt.show()

growthLables = getGrowth(labels)
growthPredictY = getGrowth(predictY)
print("最近5天增长幅预测值：")
print(growthPredictY[-5:])
print("最近5天增长幅真实值：")
print(np.hstack((growthLables[-2:], [None, None, None])))
save_result(predictY[-3:], growthPredictY[-3:], fundCode)
plt.figure(figsize=(10, 5))
plt.plot(growthLables, color='blue', label='基金每日增长幅')
plt.plot(growthPredictY, color='red', label='预测每日增长幅')
plt.legend(loc='upper left')
plt.title("增长幅预测 基金代码：{}".format(fundCode))
plt.show()
