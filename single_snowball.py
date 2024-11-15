import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime,timedelta
import os
import re
import matplotlib.pyplot as plt
from get_data import get_data
os.chdir('C:\\Users\\tongyu\\Desktop\\雪球回测\\')

from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env


"""
data里面是标的的收盘价数据，可在get_data文件里面选择所需的标的
und_code：标的代码
knock_in_price:敲入价，填入百分数，如75,80

strike_price:行权价，填入百分数(2024-09-09更新)
knock_out_price_rule：格式：'24M-100-0'，或'12M-100-0,12M-100-0.5',三个参数分别是期限，敲出价，降敲步长
start_date:开始日期，datetime格式
tenure：期限，填入整数月份，如24
lock_period：锁定期
ob_freq：观察频率
coupon_rate：票息，填入百分数
"""

class Snowball:
    # 2024-07-09: 新增开关式结构(eu_switch, boolean value, True or False), 用于判断是否为欧式结构，默认为True，如果为欧式结构则boolean value为False。
    # 2024-07-09: 新增敲入观察频率(ob_freq_ki, int value)。
    # 2024-07-09: 票息coupon_rate更新分段式（def calc_coupon_rule）。
    # 2024-09-10: 新增strike_price, strike_price为行权价，填入百分数。
    def __init__(self, und_code, data, knock_in_price, knock_out_price_rule, strike_price,start_date,tenure, lock_period, ob_freq,ob_freq_ki, coupon_rate,eu_ki_switch):
        """_summary_

        Args:
            und_code (_type_): _description_
            data (_type_): _description_
            knock_in_price (_type_): _description_
            knock_out_price_rule (_type_): _description_
            strike_price (_type_): _description_
            start_date (_type_): _description_
            tenure (_type_): _description_
            lock_period (_type_): _description_
            ob_freq (_type_): _description_
            ob_freq_ki (_type_): _description_
            coupon_rate (_type_): _description_
            eu_ki_switch (_type_): _description_

        Raises:
            ValueError: _description_
        """
        self.und_code=und_code
        self.data=data
        self.price=self.data[self.und_code]
        self.knock_in_price = knock_in_price/100 # 填入百分数
        self.strike_price = strike_price/100 # 填入百分数
        self.knock_out_price_rule=knock_out_price_rule  
        self.start_date = start_date
        self.eu_ki_switch = eu_ki_switch
        # 增加了没有数据情形判断 （吴伟）
        if self.start_date not in self.price.index:
            if self.start_date > self.price.index[-1]:
                raise ValueError('选择的日期暂无数据') 
            else:
                while self.start_date not in self.price.index:
                    self.start_date+=timedelta(days=1) #顺延到下一个交易日
        self.tenure = tenure 
        self.lock_period = lock_period #月 

        # 考虑到现实中锁定期不可能为0，有些记录的锁定期为0其实是为1，故做次修改（吴伟）  
        if self.lock_period==0: 
            self.lock_period=1 
        
        self.ob_freq = ob_freq
        self.ob_freq_ki = ob_freq_ki
        self.coupon_rate=coupon_rate

        self.knock_out_price=[]       #存储敲出价格的列表 （吴伟）
        self.ob_prices=[]             #存储敲出观察日的收盘价列表  （吴伟）

        self.calc_knock_out_price()
        self.calc_coupon_rule() # 新增coupon_rate计算分段票息
        self.is_knock_in = False
        self.is_knock_out = False
        self._knock_out_date = None
        self._knock_in_date = None
        self.reach_tenure = False #是否到期
        self.is_end = False #合约是否结束
        self.payoff = 0
        self._end_date = None
        self.check_life()
        self.knock_out_date
        self.knock_in_date
        # 新增欧式结构判断
        self.check_end()
         # 放在了checkend后面，因为checkend里面有可能会改变end_date的值
        self.end_date
        self.check_same_date()
        self.calc_payoff()
        
    def calc_knock_out_price(self):
        # 将规则字符串分割成单独的规则列表
        rules = self.knock_out_price_rule.split(',')
        self.knock_out_price = []
        for rule in rules:
            # 对每个规则进行分割以获取具体的参数
            items = re.split(r'[-]', rule)
            period, start, step = items[0], float(items[1]), float(items[2])            
            # 计算每个规则的敲出价并添加到列表中
            # for i in range(0, int(re.search(r'\d+', period).group()), self.ob_freq):
            #     price = (start - i * step) / 100 * self.price.loc[self.start_date]
            #     self.knock_out_price.append(price)
            for i in range(0, int(re.search(r'\d+', period).group()), self.ob_freq):
                if i < self.lock_period:
                    # 在锁定期内，敲出价保持初始值
                    price = start / 100 * self.price.loc[self.start_date]
                else:
                    # 锁定期结束后，敲出价开始递减
                    price = (start - (i - self.lock_period + 1 ) * step) / 100 * self.price.loc[self.start_date]
                self.knock_out_price.append(price)
        # print(f'敲出价格列表：{self.knock_out_price}')
        # print(f'敲出价格列表length：{len(self.knock_out_price)}')
            
        
    # 2024-07-09:新增coupon_rule 获取分段票息 Yuchen
    def calc_coupon_rule(self):
        rules = self.coupon_rate.split(',')
        self.coupon_list = []
        for rule in rules:
            items = re.split(r'[-]', rule)
            period, start, step = items[0], float(items[1]), float(items[2]) 
            for i in range(0, int(re.search(r'\d+', period).group()), self.ob_freq):
                coupon = (start - i * step) / 100
                self.coupon_list.append(coupon)
        self.coupon_list = self.coupon_list[(self.lock_period-1):]
        
    def check_life(self):
        # 检查有没有到期，如果开始日+期限>最后一天的日期，到期
        if self.start_date + relativedelta(months = self.tenure) <= self.price.index[-1]:
            self.reach_tenure = True
    
    def _check_knock_out(self):
        ob_days_raw = [self.start_date + relativedelta(months=i * self.ob_freq) for i in range(1, (self.tenure // self.ob_freq) + 1)]
        self.ob_days_full = []
        # 观察天数的范围是tenure之内且现有数据之内
        for day in ob_days_raw:
            if day > self.price.index[-1]:  # 如果日期超出了price.index的范围，停止循环
                # raise Exception('入参日期超出数据范围')
                self.ob_days_full.append(day) 
            elif day < self.price.index[0]:
                raise Exception('入参日期超出数据范围')
            else: 
                while day not in self.price.index:
                    day += timedelta(days=1)  # 顺延到第一个交易日
                self.ob_days_full.append(day)     
        self.ob_days = self.ob_days_full[self.lock_period-1:] # Yuchen: 从锁定期开始计算敲出观察日 
        #print(f'敲出观察日：{self.ob_days}')
        ob_prices = self.price.reindex(self.ob_days)
        self.ob_prices=ob_prices
        #print(f'敲出观察日价格：{ob_prices}')
        ob_knock_out = self.knock_out_price[self.lock_period-1:self.lock_period-1+len(self.ob_days)]
        ob_knock_out = pd.Series(ob_knock_out,index = ob_prices.index) #观察日的敲出价  
        knock_out_prices = ob_prices[ob_prices >= ob_knock_out]#knock_out_prices is a Series
        knock_out_date = None
        # 未增加欧式结构的判断
        if len(knock_out_prices) > 0:
            if (self.lock_period == 0) & (self.knock_out_price == 1): #锁定期为0时，第一个knock_out_prices是买入这一天
                if len(knock_out_prices) > 1:
                    self.is_knock_out = True
                    knock_out_date = knock_out_prices.index[1]
            else:
                self.is_knock_out = True
                knock_out_date = knock_out_prices.index[0]
        return knock_out_date
       
            
    @property
    def knock_out_date(self): #属性化函数，为self._knock_out_date赋值，查看私有属性_knock_out_date
        if self._knock_out_date is None:
            self._knock_out_date = self._check_knock_out()
        return self._knock_out_date
    
    # 2024-07-11: 修改敲入观察频率的计算方式，增加敲入观察频率ob_freq_ki，用于计算敲入观察日（Yuchen）
    def _check_knock_in(self):
        # 敲入观察日从开始日期开始，每隔 ob_freq_ki 月检查一次
        temp_last_day = self.start_date + relativedelta(months = self.tenure)
        if temp_last_day not in self.price.index and temp_last_day < self.price.index[-1]: 
            while temp_last_day not in self.price.index:
                temp_last_day += timedelta(days=1)
                
        knock_in_date = None 
                
        if self.eu_ki_switch == False:
            if re.search(r'M',self.ob_freq_ki):
                num_freq_ki = int(re.search(r'\d+',self.ob_freq_ki).group())

                # ---------------------------------修改---------------------------------
                ob_days_raw = [self.start_date + relativedelta(months=i * num_freq_ki) for i in range((self.tenure // num_freq_ki) + 1)]
                self.ob_days_ki = []
                # 观察天数的范围是tenure之内且现有数据之内
                for day in ob_days_raw:
                    if day > self.price.index[-1]:  # 如果日期超出了price.index的范围，停止循环
                        # raise Exception('入参日期超出数据范围')
                        self.ob_days_ki.append(day) 
                    elif day < self.price.index[0]:
                        raise Exception('入参日期超出数据范围')
                    else:    
                        while day not in self.price.index:
                            day += timedelta(days=1)  # 顺延到第一个交易日
                        self.ob_days_ki.append(day)  
                self.ob_days_ki = self.ob_days_ki[self.lock_period:] # Yuchen: 从锁定期开始计算敲入观察日  
                # 获取敲入观察日的价格
                ob_prices = self.price.reindex(self.ob_days_ki)
                knock_in_dates = []
                for date, price in zip(self.ob_days_ki, ob_prices):
                    if pd.notna(price) and price < self.knock_in_price * self.price.loc[self.start_date]:
                        knock_in_dates.append(date)
                # 检查是否敲入   
                if len(knock_in_dates) != 0: 
                    if self.is_knock_out:
                        if knock_in_dates[0] < self._knock_out_date:
                            knock_in_date = knock_in_dates[0]
                            self.is_knock_in = True
                    else:
                        knock_in_date = knock_in_dates[0]
                        self.is_knock_in = True
            elif re.search(r'D',self.ob_freq_ki):
                # num_freq_ki = int(re.search(r'\d+',self.ob_freq_ki).group())
                # 这个地方写死了，后期需要改，本来应该是生成一个列表，记录所有观察日，但是我只取了第一天和最后一天的
                self.ob_days_ki = [self.start_date,  temp_last_day]
                prices = self.price[(self.price.index>= self.start_date) & (self.price.index< temp_last_day)]
                knock_in_prices = prices[prices < self.knock_in_price * self.price.loc[self.start_date]]
                #敲入一定在敲出之前 或者没敲出
                if len(knock_in_prices) != 0:
                    if self.is_knock_out:
                        if knock_in_prices.index[0] < self.knock_out_date:
                            knock_in_date = knock_in_prices.index[0]
                            self.is_knock_in = True   
                    else:
                        knock_in_date = knock_in_prices.index[0]
                        self.is_knock_in = True

        # 欧式结构，只观察一次，且为最后一天()
        else: 
            self.ob_days_ki = [temp_last_day]
            if self.is_knock_out:
                self.is_knock_in = False
                self.is_knock_out = True

            else:
                if temp_last_day in self.price.index:
                    spot_price = self.price.loc[temp_last_day]
                    if spot_price < self.knock_in_price * self.price.loc[self.start_date]:
                        knock_in_date = temp_last_day
                        self.is_knock_in = True
                        #最后一天敲入以后，不可能还会敲出，所以这里直接赋值
                        self.is_knock_out = False
        return knock_in_date   
    
    @property
    def knock_in_date(self):
        if self._knock_in_date is None:
            self._knock_in_date = self._check_knock_in()
        return self._knock_in_date
            
    def check_end(self):
        if self.reach_tenure:
            self.is_end = True #到存续期肯定结束
        else:
            if self.is_knock_out:
                self.is_end = True #没到存续期敲出结束

    def calc_payoff(self):   # 新增coupon_rate计算收益（吴伟）
        # 未存续 已结束
        if self.is_end:
            # 产品已经触发了敲入条件而没有触发敲出条件
            # 0910更新：增加了strike_price的判断
            if self.is_knock_in & (not self.is_knock_out):
                self.payoff+=(self.price.loc[self.end_date] - (self.price.loc[self.start_date]*self.strike_price)) / self.price.loc[self.start_date]
            # 检测coupon_rate是否为分段式，如果是的话进行额外处理 - Yuchen
            else:
                days_num=len(self.price[(self.price.index <= self.end_date) & (self.price.index >= self.start_date)])
                date_index = self.ob_days.index(self.end_date)
                self.payoff=days_num/252*self.coupon_list[date_index]

    def _get_end_date(self):
        end_date = None
        if self.is_end:
            if self.is_knock_out:
                end_date = self._knock_out_date
            else:
                end_date = self.start_date + relativedelta(months = int(self.tenure)) # 更改成所有敲入/敲出观察日最后一天
                while end_date not in self.price.index: # 以免结束时不是交易日
                    end_date += timedelta(days = 1)
            return end_date
        
    @property
    def end_date(self):
        if self._end_date == None:
            self._end_date = self._get_end_date()
        return self._end_date
    
    def check_same_date(self):
        if self.is_end:
            if self.ob_days_ki[-1] != self.ob_days[-1]: 
                raise Exception(f'最后一个敲出观察日和最后一个敲入观察日不相等\n,敲入观察日最后一天{self.ob_days_ki[-1]}, 敲出观察日最后一天{self.ob_days[-1]} ')
        else:
            pass
        
    def __repr__(self):
        return f'Snowball 开始日期{self.start_date}|敲入 {self._knock_in_date}|敲出 {self._knock_out_date}|存续 {not self.is_end}|收益 {self.payoff}|是否敲入{self.is_knock_in}|是否敲出{self.is_knock_out}'
# 入参：
#(self, und_code, data, knock_in_price, knock_out_price_rule, strike_price,start_date,tenure, lock_period, ob_freq,ob_freq_ki, coupon_rate,eu_ki_switch)
if __name__ == '__main__':
    und_code='600519.SH'
    data=get_data([und_code])  # 先提取标的的收盘价数据
    # print(data)
    date_data = pd.read_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\date.csv')
    date_data['start_date'] = pd.to_datetime(date_data['start_date'])
    print(date_data['start_date'])
    print(Snowball(und_code,data,75,'24M-100-1',100, datetime(2015,3,30),24,3,1,'1D','24M-10-0',False))
    
    for date in date_data['start_date']:
        print(Snowball(und_code,data,75,'24M-100-1',100, date,24,3,1,'1D','24M-10-0',False))
    