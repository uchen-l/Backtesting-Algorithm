
from single_snowball import Snowball
import os
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime,timedelta
import warnings
from get_data import get_data
from tqdm import tqdm

warnings.simplefilter(action='ignore', category=Warning)


os.chdir('C:\\Users\\tongyu\\Desktop\\雪球回测\\')





class SnowballBacktest:
    def __init__(self,und_code,data, knock_in_price, knock_out_price_rule,strike_price, ob_freq, ob_freq_ki, tenure, lock_period, start_date, 
                 end_date,coupon_rate,allo_period, knock_in_out_days,eu_ki_switch, last_knock_out_price = None):
       
        #单雪球属性
        self.und_code=und_code   # 新增了und_code,data,coupon_rate变量 （吴伟）
        self.data=data
        self.price=self.data[self.und_code]
        self.knock_in_price = knock_in_price
        self.knock_out_price_rule = knock_out_price_rule  # 吴伟加
        self.strike_price = strike_price # 2024-09-10: 新增strike_price, strike_price为行权价，填入百分数。
        self.ob_freq = ob_freq
        self.ob_freq_ki = ob_freq_ki # Yuchen 20240712
        self.tenure = tenure
        self.lock_period = lock_period
        self.last_knock_out_price = last_knock_out_price
        self.eu_ki_switch = eu_ki_switch
        #回测属性
        self.start_date = start_date
        self.end_date = end_date
        self.coupon_rate=coupon_rate
        if end_date < self.price.index[0]:
            raise ValueError('请重新初始化一个小于今天的结束日期')
        self.allo_period = allo_period #加仓周期 天 1就是1天买一笔
        self.knock_in_out_days = knock_in_out_days
        self.knock_in_snowball=[]
        self.not_knock_in_snowball=[]
        self.ave_end_time=0                  #所有结束合约的平均存续时间
        self.backtest_snowballs()
    def _create_snowballs(self):
        open_days = []
        open_day = self.start_date
        while open_day < self.end_date:
            temp = open_day
            while temp not in self.price.index:
                temp = temp + timedelta(days = 1)
            if temp not in open_days:
                open_days.append(temp)
            open_day = open_day + timedelta(days = self.allo_period)
                    
        snowball_list = []
        for i in tqdm(open_days):
            snowball = Snowball(self.und_code, self.data, self.knock_in_price, self.knock_out_price_rule, i, \
                                self.tenure, self.lock_period, self.ob_freq, self.ob_freq_ki, self.coupon_rate, self.eu_ki_switch)
            # snowball.calc_coupon_rule() 在这里调用 calc_coupon_rule 方法
            snowball_list.append(snowball)
        self.snowballs = snowball_list
        return snowball_list
    def backtest_snowballs(self):
        snowballs = self._create_snowballs()
        #结束组
        self.in_and_out = []
        self.not_in_and_out = []
        self.not_in_not_out = []
        self.in_not_out = []
        self.payoffs = []  # 存储收益的列表    （吴伟）
        self.end_time=[]  # 存储结束时间的列表  （吴伟）

        #未结束组
        self.not_end_in = []
        self.not_end_not_in = []
        self.knock_out_times = []
        self.knock_in_times = []
        self.knock_out_ntimes = []
        self.knock_in_ntimes = []
        self.knock_out_1y = 0
        self.profit = 0
        self.profit1y =0
        self.knock_in = 0
        self.knock_in_1y_out =0
        for snowball in tqdm(snowballs):

            if snowball.is_end: #结束合约
                self.payoffs.append(snowball.calc_payoff())  # 吴伟
                self.end_time.append((snowball.end_date-snowball.start_date).days)  # 吴伟

                if snowball.is_knock_in & snowball.is_knock_out:
                    self.in_and_out.append(snowball)
                    self.knock_out_times.append(len(self.price[(self.price.index <= snowball.knock_out_date) & (self.price.index >= snowball.start_date)]))
                    self.knock_in_times.append(len(self.price[(self.price.index <= snowball.knock_in_date) & (self.price.index >= snowball.start_date)]))
                    
                    self.knock_out_ntimes.append((snowball.knock_out_date-snowball.start_date).days)
                    self.knock_in_ntimes.append((snowball.knock_in_date-snowball.start_date).days)
                elif snowball.is_knock_in & (not snowball.is_knock_out):
                    self.knock_in_times.append(len(self.price[(self.price.index <= snowball.knock_in_date) & (self.price.index >= snowball.start_date)]))
                    self.knock_in_ntimes.append((snowball.knock_in_date-snowball.start_date).days)
                    self.in_not_out.append(snowball)
                elif (not snowball.is_knock_in) & snowball.is_knock_out:
                    self.knock_out_times.append(len(self.price[(self.price.index <= snowball.knock_out_date) & (self.price.index >= snowball.start_date)]))
                    self.knock_out_ntimes.append((snowball.knock_out_date-snowball.start_date).days)
                    self.not_in_and_out.append(snowball)
                else:
                    self.not_in_not_out.append(snowball)
                    
            else: #未结束合约
                if snowball.is_knock_in:
                    self.not_end_in.append(snowball)
                else:
                    self.not_end_not_in.append(snowball)
        
        for snowball in snowballs:
            if  snowball.is_end: #结束合约
                if (not (snowball.is_knock_in & (not snowball.is_knock_out))):#盈利合约
                    self.profit += 1
                    if snowball.is_knock_out:
                        if (snowball.knock_out_date-snowball.start_date).days <= 365:
                            self.profit1y +=1
        for snowball in snowballs:
            if  snowball.is_end: #结束合约
                if snowball.is_knock_in:#敲入合约
                    self.knock_in += 1
                    if snowball.is_knock_out:
                        if (snowball.knock_out_date-snowball.knock_in_date).days <= self.knock_in_out_days:
                            self.knock_in_1y_out +=1

        self.end_ttl = len(self.in_and_out) + len(self.not_in_and_out) + len(self.not_in_not_out) + len(self.in_not_out)
        self.io_ratio = len(self.in_and_out)/self.end_ttl
        self.nio_ratio = len(self.not_in_and_out)/self.end_ttl
        self.nino_ratio = len(self.not_in_not_out)/self.end_ttl
        self.ino_ratio = len(self.in_not_out)/self.end_ttl
        self.not_end_ttl = len(self.not_end_in) + len(self.not_end_not_in)
        self.nei_ratio = len(self.not_end_in)/self.not_end_ttl
        self.neni_ratio = len(self.not_end_not_in)/self.not_end_ttl
        self.profit1y_ratio = self.profit1y/self.profit
        self.knock_in_1y_out_ratio = self.knock_in_1y_out/self.knock_in

        self.ave_end_time=round(np.mean(self.end_time),2)
        
        for out in self.in_and_out:
            # 为什么这里是交易日年（252）而不是自然日年（365）？
            if len(self.price.loc[out.start_date:out.knock_out_date]) <= 252:
                self.knock_out_1y += 1
        for out in self.not_in_and_out:
            if len(self.price.loc[out.start_date:out.knock_out_date]) <= 252:
                self.knock_out_1y += 1
        self.out_1y_ratio = self.knock_out_1y/len(self.snowballs)
    def __repr__(self):
        io = round(self.io_ratio*100,2)
        nio = round(self.nio_ratio*100,2)
        nino = round(self.nino_ratio*100,2)
        ino = round(self.ino_ratio*100,2)
        nei = round(self.nei_ratio*100,2)
        neni = round(self.neni_ratio*100,2)
        # out_1y未被调用
        out_1y = round(self.out_1y_ratio*100,2)
        profit1y = round(self.profit1y_ratio*100,2)
        knock_in_1y_out = round(self.knock_in_1y_out_ratio*100,2)
        str1 = f'结束合约:{self.end_ttl}\n敲入后敲出：{len(self.in_and_out)} {io}%\n未敲入敲出:{len(self.not_in_and_out)} {nio}%\n未敲入 未敲出:{len(self.not_in_not_out)} {nino}%\n敲入 未敲出：{len(self.in_not_out)} {ino}%'
        str3 = f'平均敲入交易日：{np.mean(self.knock_in_times)}\n平均敲出交易日:{np.mean(self.knock_out_times)}'
        str5 = f'平均敲入自然日：{np.mean(self.knock_in_ntimes)}\n平均敲出自然日:{np.mean(self.knock_out_ntimes)}'
        str2 = f'存续合约:{self.not_end_ttl}\n未敲入:{len(self.not_end_not_in)} {neni}%\n已敲入:{len(self.not_end_in)} {nei}%\n'
        str4 = f'一年内敲出概率:{profit1y}%'
        str6 = f'敲入后一年内敲出概率:{knock_in_1y_out}%'
        return str1 +'\n' + str3 +'\n' +str5  +'\n' + str4 +'\n' +str6  +'\n\n' + str2

if __name__ == '__main__':
    
    # 修改雪球回测参数
    und_code='002230.SZ'
    data=get_data([und_code])
    # print(data)
    print(data[und_code])
    low = datetime(2016,1,4)
    high = datetime(2024,7,26)
    
    bt1 = SnowballBacktest(und_code,data,knock_in_price=80, 
                           knock_out_price_rule='23M-100-0.5,1M-81-0',
                           strike_price=100,
                           ob_freq=1, 
                           ob_freq_ki='1D',
                           tenure=24, 
                           lock_period=3, 
                           start_date=low,
                           end_date=high,
                           # coupon_rate='6M-15.2-0,6M-6.8-0,12M-2.9-0',
                           coupon_rate='12M-10-0,12M-3-0',
                           allo_period=1,
                           eu_ki_switch=False,
                           knock_in_out_days=365,
                           last_knock_out_price = None)
    print(f"标的 {und_code} 回测结果如下： \n{bt1}")
    # bt1_start_dates = {sb.start_date for sb in bt1.not_in_and_out}
    # bt2_start_dates = {sb.start_date for sb in bt2.not_in_and_out}
    # diff_bt1 = bt2_start_dates - bt1_start_dates
    # diff_bt2 = bt1_start_dates - bt2_start_dates
    # diff_csv = pd.DataFrame(diff_bt1, columns = ['start_date'])
    # diff_csv1 = pd.DataFrame(diff_bt2, columns = ['start_date'])
    # diff_csv.to_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\diff_bt1.csv')
    # diff_csv1.to_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\diff_bt2.csv')
    
    # ----------输出中间表代码----------
    date_pd = data[(data.index >= low)&(data.index <= high)]
    snowball_pd = pd.DataFrame(index = date_pd.index,columns = ['end_date','status','is_knock_in','knock_in_date','is_knock_out','knock_out_date','payoff_status'])
    for i in range(len(bt1.snowballs)):
        single = []
        snowball = bt1.snowballs[i]
        end_date = snowball.end_date
        status = 'CLOSED' if snowball.is_end else 'ACTIVE'
        is_knock_out = snowball.is_knock_out
        knock_out_date = snowball.knock_out_date
        is_knock_in = snowball.is_knock_in
        knock_in_date = snowball.knock_in_date
        

        payoff_status = None
        if snowball.payoff != None:
            payoff_status = 'LOSS'
        else:
            if snowball.is_end:
                payoff_status = 'PROFIT'
        attr_list = [end_date, status, is_knock_in, knock_in_date, is_knock_out, knock_out_date,payoff_status]
        snowball_pd.loc[snowball.start_date] = attr_list
        #print(snowball_pd.loc[snowball.start_date])
    #输出中间表
    snowball_pd.to_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\大盘雪球打分回测\\002230.SZ - 早利+降敲+降落伞+24M+锁3M+80KI+100KO+降敲0.5+末次观察81+追保+100%+第二段票息3.csv')
    
