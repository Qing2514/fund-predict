import requests
import pymysql
import time
import re
import json
import execjs
import pandas as pd

# 建立数据库连接
cnx = pymysql.connect(host='localhost', port=3306, user='root', password='2514632453',
                      db='fundtrans', charset='utf8mb4')

# 创建游标
cursor = cnx.cursor()


class FundCrawler:
    def __init__(self,
                 fund_code: int,
                 page_range: int = None,
                 file_name=None):
        """
        :param fund_code:  基金代码
        :param page_range:  获取最大页码数，每页包含20天的数据
        """
        self.root_url = 'http://api.fund.eastmoney.com/f10/lsjz'
        self.fund_code = fund_code
        self.session = requests.session()
        self.page_range = page_range
        self.file_name = file_name if file_name else '{}.csv'.format(self.fund_code)
        self.headers = {
            'Host': 'api.fund.eastmoney.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Referer': 'http://fundf10.eastmoney.com/jjjz_%s.html' % self.fund_code,
        }

    @staticmethod
    def content_formatter(content):
        params = re.compile('jQuery.+?\((.*)\)')
        data = json.loads(params.findall(content)[0])
        return data

    def page_data(self,
                  page_index):
        params = {
            'callback': 'jQuery18308909743577296265_1618718938738',
            'fundCode': self.fund_code,
            'pageIndex': page_index,
            'pageSize': 20,
        }
        res = self.session.get(url=self.root_url, headers=self.headers, params=params)
        content = self.content_formatter(res.text)
        return content

    def page_iter(self):
        for page_index in range(self.page_range):
            item = self.page_data(page_index + 1)
            yield item

    def get_all(self):
        total_count = float('inf')
        page_index = 0
        while page_index * 20 <= total_count:
            item = self.page_data(page_index + 1)
            page_index += 1
            total_count = item['TotalCount']
            yield item

    def run(self):
        if self.page_range:
            for data in self.page_iter():
                for fund in data['Data']['LSJZList']:
                    if fund['JZZZL'] == '':
                        fund['JZZZL'] = 0.00
                    fund_data = {
                        'date': fund['FSRQ'],
                        'net_worth': fund['DWJZ'],
                        'growth': fund['JZZZL'],
                    }
                    data_tuple = tuple(fund_data.values())
                    add_data = "INSERT INTO `trend_{}` (date,net_worth,growth) VALUES (%s,%s,%s)".format(self.fund_code)
                    cursor.execute(add_data, data_tuple)
                    cnx.commit()
        else:
            for data in self.get_all():
                for fund in data['Data']['LSJZList']:
                    if fund['JZZZL'] == '':
                        fund['JZZZL'] = 0.00
                    fund_data = {
                        'date': fund['FSRQ'],
                        'net_worth': fund['DWJZ'],
                        'growth': fund['JZZZL'],
                    }
                    data_tuple = tuple(fund_data.values())
                    add_data = "INSERT INTO `trend_{}` (date,net_worth,growth) VALUES (%s,%s,%s)".format(self.fund_code)
                    cursor.execute(add_data, data_tuple)
                    cnx.commit()


def creat_product():
    # 删除表
    drop_table_product = '''
        drop table if exists product;
    '''
    cursor.execute(drop_table_product)

    # 创建表
    create_table_product = '''
        CREATE TABLE product (
        id VARCHAR(50) NOT NULL,
        name VARCHAR(50) NOT NULL,
        type VARCHAR(50) NOT NULL,
        security SMALLINT(2) NOT NULL,
        net_worth DECIMAL(10,4) NOT NULL,
        growth DECIMAL(10,2),
        manager VARCHAR(50) NOT NULL,
        state SMALLINT(2) NOT NULL,
        PRIMARY KEY(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    '''
    cursor.execute(create_table_product)


def get_security(security):
    if security == '001':
        return 4
    elif security == '002' or security == '003':
        return 3
    elif security == '004' or security == '005':
        return 2
    else:
        return 1


for j in range(1, 40):
    url = f'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page={j},200&dt=1597126258333&atfc=&onlySale=0'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'
    }
    resp = requests.get(url, headers=headers).text
    str_ = resp[106:]
    fund_list = eval(str_.split(",count")[0])
    print(f'\n正在爬取第{j}页')

    session = requests.session()
    search_url = 'https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx'
    # creat_product()
    for i in range(len(fund_list)):
        # 爬取该基金详细信息
        fund_date = fund_list[i]
        print('产品代码：' + fund_date[0])
        headers = {
            'Host': 'api.fund.eastmoney.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Referer': 'http://fundf10.eastmoney.com/jjjz_%s.html' % fund_date[0],
        }
        params = {
            "callback": "jQuery18309325043269513131_1618730779404",
            "m": 1,
            "key": fund_date[0],
        }
        res = session.get(search_url, params=params)
        params = re.compile('jQuery.+?\((.*)\)')
        content = json.loads(params.findall(res.text)[0])
        fund = content['Datas'][0]['FundBaseInfo']

        # if fund_date[3] == '':
        #     fund_date[3] = 0.00
        # if fund_date[8] == '':
        #     fund_date[8] = 0.00
        # security = get_security(fund['FUNDTYPE'])
        # data = {
        #     'id': fund['FCODE'],
        #     'name': fund['SHORTNAME'],
        #     'type': fund['FTYPE'],
        #     'security': security,
        #     'net_worth': fund_date[3],
        #     'growth': fund_date[8],
        #     'manager': fund['JJJL'],
        #     'state': 0
        # }
        # data_tuple = tuple(data.values())
        # add_data = "INSERT INTO product (id,name,type,security,net_worth,growth,manager,state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        # cursor.execute(add_data, data_tuple)
        # cnx.commit()

        # # 爬取该基金净值数据
        # # 删除表
        # drop_table = '''
        #     drop table if exists trend_`%s`;
        # ''' % (fund_date[0])
        # cursor.execute(drop_table)

        fund_date[0] = '007669'

        # 以基金代码为表名创建表
        create_table = '''
            CREATE TABLE `trend_%s` (
            date DATE NOT NULL,
            net_worth DECIMAL(10,4) NOT NULL,
            growth DECIMAL(10,2),
            PRIMARY KEY(date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        ''' % (fund_date[0])
        cursor.execute(create_table)

        c = FundCrawler(fund_date[0])
        c.run()
