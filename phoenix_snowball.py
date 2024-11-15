from single_snowball import Snowball
import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from statistics import mean
from datetime import datetime,timedelta
from tqdm import tqdm
import warnings
# warnings.simplefilter(action='ignore', category=Warning)
# print(pd.__version__)

# 2024-07-08 PS：该代码需要在pandas 1.5.2 环境下运行，否则会报错 pandas._config.config.OptionError: No such keys(s): 'mode.use_inf_as_null' 
# 解决方法是在terminal中输入 'pip install pandas==1.5.2'

# 2024-09-09 加入对FCN结构支持(fcn_switch)，基于凤凰结构进行修改，区别在于FCN为欧式观察（所以欧式开关打开），且固定拿到派息（所以eu_ki_switch为True）
# 同时区分了knock_in_price（敲入价）和strike_price（行权价）


class phoenix_snowball(Snowball):
    """
    """
    def __init__(self ,und_code ,data,knock_in_price, knock_out_price_rule,strike_price, start_date,tenure,lock_period, ob_freq,ob_freq_ki, coupon_rate,eu_ki_switch,fcn_switch):
        self.fcn_switch=fcn_switch
        # 新增strike_price属性
        # self.strike_price=strike_price / 100
        self.get_dividend_months=0
        self.knock_in_but_get_dividend=0  # 月中敲入但当月拿到派息=首次敲入至产品敲出期间拿到的派息次数/首次敲入至产品敲出期间的敲出观察日的数量
        self.knock_in_not_out_but_win=0  # 累计派息可覆盖期末亏损，是为1，否则为0
        super().__init__(und_code,data,knock_in_price, knock_out_price_rule,strike_price, start_date,tenure,lock_period, ob_freq,ob_freq_ki, coupon_rate,eu_ki_switch)
        # 防止出现不合理的输入，如果传入fcn_switch=True，那么eu_ki_switch必须为True,因为fcn仅作欧式观察
        if self.fcn_switch:
            self.eu_ki_switch=True
        self.calc_get_dividend_months()  
        self.calc_payoff()
        self.calc_knock_in_not_out_but_win()


    def calc_get_dividend_months(self):
        """
        获取得到派息的月份数
        """
        if self.is_end:
            self.dividend_ob_days = [x for x in self.ob_days_full if x <= self.end_date]  # 派息观察日列表
            if self.eu_ki_switch:
                if self.fcn_switch == False:
                    if self.is_knock_in:
                        self.get_dividend_months = len(self.dividend_ob_days) - 1
                    else:
                        self.get_dividend_months = len(self.dividend_ob_days)
                    self.knock_in_but_get_dividend = 0
                else:
                    self.get_dividend_months = len(self.dividend_ob_days)
            else:
                if self.is_knock_in:
                    numerator = 0  # 首次敲入至产品结束期间拿到的派息次数
                    denominator = len([x for x in self.dividend_ob_days if x >= self.knock_in_date])  # 首次敲入至产品结束期间的派息观察日数量
                    for i in range(len(self.dividend_ob_days)):
                        if self.price.loc[self.dividend_ob_days[i]] >= self.knock_in_price * self.price.loc[self.start_date]:
                            self.get_dividend_months += 1
                            if self.dividend_ob_days[i] >= self.knock_in_date:
                                numerator += 1
                    self.knock_in_but_get_dividend = numerator / denominator
                else:
                    for i in range(len(self.dividend_ob_days)):
                        if self.price.loc[self.dividend_ob_days[i]] >= self.knock_in_price * self.price.loc[self.start_date]:
                            self.get_dividend_months += 1
                    self.knock_in_but_get_dividend = 0
        
    def calc_payoff(self):
        """
        计算收益
        """
        if self.is_end:
            if len(self.coupon_rate.split(',')) == 1 and self.coupon_rate.split('-')[-1] == '0':
                self.payoff=self.get_dividend_months*float(self.coupon_rate.split('-')[1]) / 12 / 100
            else:
                raise Exception('coupon_rate入参格式错误，目前凤凰只支持单一票息且不分段')
                
            if self.is_knock_in & (not self.is_knock_out):
                if self.price.loc[self.end_date]<self.price.loc[self.start_date]:
                    # 0911更新：加入行权价，payoff公式为 期末价/(期初价格 * 行权价) - 1
                    self.payoff+=(self.price.loc[self.end_date] - (self.price.loc[self.start_date]*self.strike_price)) / self.price.loc[self.start_date]
    def calc_knock_in_not_out_but_win(self):
        if self.is_end & self.is_knock_in &(not self.is_knock_out)&(self.payoff>=0):
            self.knock_in_not_out_but_win=1

    def __repr__(self):
        return f'Snowball 敲入 {self.knock_in_date}|敲出 {self.knock_out_date}|存续 {not self.is_end}|收益 {self.payoff}|派息月份 {self.get_dividend_months}|月中敲入但拿到派息{self.knock_in_but_get_dividend}'

class PhoenixSnowballBacktest:
    def __init__(self, und_code,data,knock_in_price, knock_out_price_rule,strike_price, ob_freq,  tenure, lock_period, start_time,end_time,allo_period,coupon_rate, eu_ki_switch, fcn_switch, ob_freq_ki):
       
        #单雪球属性
        self.und_code=und_code
        self.data=data
        self.idx_price=self.data[self.und_code]
        self.knock_in_price = knock_in_price
        self.knock_out_price = knock_out_price_rule
        self.ob_freq = ob_freq
        self.tenure = tenure
        self.lock_period = lock_period
        self.coupon_rate=coupon_rate
        self.eu_ki_switch=eu_ki_switch
        self.fcn_switch=fcn_switch
        self.ob_freq_ki=ob_freq_ki
        self.strike_price=strike_price
        #回测属性
        self.start_time = start_time
        self.end_time= end_time
        if end_time < self.idx_price.index[0]:
            raise ValueError('请重新初始化一个小于今天的结束日期')
        self.allo_period = allo_period #加仓周期 天 1就是1天买一笔
        self.knock_in_but_get_dividend_ratio=0
        self.loss_but_win_ratio=0
        self.backtest_snowballs()

    def _create_snowballs(self):
        open_days = []
        open_day = self.start_time
        while open_day < self.end_time:
            temp = open_day
            while temp not in self.idx_price.index:
                temp = temp + timedelta(days = 1)
            if temp not in open_days:
                open_days.append(temp)
            open_day = open_day + timedelta(days = self.allo_period)
        snowball_list = [phoenix_snowball(self.und_code,self.data,self.knock_in_price, self.knock_out_price,self.strike_price, i, \
                                  self.tenure, self.lock_period,self.ob_freq, self.ob_freq_ki, \
                                  self.coupon_rate, self.eu_ki_switch, self.fcn_switch) for i in open_days]
        self.snowballs = snowball_list
        return snowball_list
    
    def backtest_snowballs(self):
        snowballs = self._create_snowballs()
        #结束组
        self.in_and_out = []
        self.not_in_and_out = []
        self.not_in_not_out = []
        self.in_not_out = []
        self.payoffs = []  #收益率列表
        self.months=[]     #派息月数列表
        self.loss=[]       #亏损的雪球
        self.win=[]        #盈利的雪球
        self.win_time=[]   #盈利的雪球存续期时长
        self.win_ratio=0   #盈利概率
        self.break_even_ratio=0  # 保本概率
        self.loss_but_win=0  # 派息覆盖期末亏损概率
        self.knock_in_but_get_dividend=0
        #未结束组
        self.not_end_in = []
        self.not_end_not_in = []
        self.knock_out_times = []
        self.knock_in_times = []
        self.knock_out_ntimes = []
        self.knock_in_ntimes = []


        for snowball in tqdm(snowballs):

            if snowball.is_end: #结束合约
                self.payoffs.append(snowball.payoff)
                self.months.append(snowball.get_dividend_months)
                if snowball.payoff<0:
                    self.loss.append(snowball.payoff)
                elif snowball.payoff>0:
                    self.win.append(snowball.payoff)
                    self.win_time.append((snowball.end_date-snowball.start_date).days)
                self.loss_but_win+=snowball.knock_in_not_out_but_win
                self.knock_in_but_get_dividend+=snowball.knock_in_but_get_dividend

                if snowball.is_knock_in & snowball.is_knock_out:
                    self.in_and_out.append(snowball)
                    self.knock_out_times.append(len(self.idx_price[(self.idx_price.index <= snowball.knock_out_date) & (self.idx_price.index >= snowball.start_date)]))
                    self.knock_in_times.append(len(self.idx_price[(self.idx_price.index <= snowball.knock_in_date) & (self.idx_price.index >= snowball.start_date)]))
                    
                    self.knock_out_ntimes.append((snowball.knock_out_date-snowball.start_date).days)
                    self.knock_in_ntimes.append((snowball.knock_in_date-snowball.start_date).days)
                elif snowball.is_knock_in & (not snowball.is_knock_out):
                    self.knock_in_times.append(len(self.idx_price[(self.idx_price.index <= snowball.knock_in_date) & (self.idx_price.index >= snowball.start_date)]))
                    self.knock_in_ntimes.append((snowball.knock_in_date-snowball.start_date).days)
                    self.in_not_out.append(snowball)
                elif (not snowball.is_knock_in) & snowball.is_knock_out:
                    self.knock_out_times.append(len(self.idx_price[(self.idx_price.index <= snowball.knock_out_date) & (self.idx_price.index >= snowball.start_date)]))
                    self.knock_out_ntimes.append((snowball.knock_out_date-snowball.start_date).days)
                    self.not_in_and_out.append(snowball)
                else:
                    self.not_in_not_out.append(snowball)

            else: #未结束合约
                if snowball.is_knock_in:
                    self.not_end_in.append(snowball)
                else:
                    self.not_end_not_in.append(snowball)

        # sns.histplot(self.months,stat='probability',kde=False)  # 得到派息的月份数的分布
        # plt.xlabel('get_dividend_months')
        # plt.ylabel('Ratio')
        # plt.title('Histogram')
        # plt.show()
        
        # sns.histplot(self.win,stat='probability',kde=False)  # 盈利的区间分布
        # plt.xlabel('win')
        # plt.ylabel('Ratio')
        # plt.title('Histogram')
        # plt.show()

        # sns.histplot([-i for i in self.loss],stat='probability',kde=False)  # 亏损的区间分布
        # plt.xlabel('loss')
        # plt.ylabel('Ratio')
        # plt.title('Histogram')
        # plt.show()

        # sns.histplot(self.win_time,stat='probability',kde=False)  # 盈利的合约的结束时间分布
        # plt.xlabel('win_time')
        # plt.ylabel('Ratio')
        # plt.title('Histogram')
        # plt.show()

        self.end_ttl = len(self.in_and_out) + len(self.not_in_and_out) + len(self.not_in_not_out) + len(self.in_not_out)
        self.io_ratio = len(self.in_and_out)/self.end_ttl
        self.nio_ratio = len(self.not_in_and_out)/self.end_ttl
        self.nino_ratio = len(self.not_in_not_out)/self.end_ttl
        self.ino_ratio = len(self.in_not_out)/self.end_ttl
        self.not_end_ttl = len(self.not_end_in) + len(self.not_end_not_in)
        self.nei_ratio = len(self.not_end_in)/self.not_end_ttl
        self.neni_ratio = len(self.not_end_not_in)/self.not_end_ttl

        self.win_ratio=len(self.win)/len(self.payoffs)
        self.break_even_ratio=len([i for i in self.payoffs if i==0])/len(self.payoffs)
        self.loss_but_win_ratio=self.loss_but_win/len(self.in_not_out)
        self.knock_in_but_get_dividend_ratio=self.knock_in_but_get_dividend/(len(self.in_and_out)+len(self.in_not_out))
    
    def __repr__(self):
        io = round(self.io_ratio*100,2)
        nio = round(self.nio_ratio*100,2)
        nino = round(self.nino_ratio*100,2)
        ino = round(self.ino_ratio*100,2)
        nei = round(self.nei_ratio*100,2)
        neni = round(self.neni_ratio*100,2)
        loss_but_win = round(self.loss_but_win_ratio*100,2)
        knock_in_but_get_dividend = round(self.knock_in_but_get_dividend_ratio*100,2)
        str1 = f'结束合约:{self.end_ttl}'
        str2 = f'盈利发生次数:{len(self.win)} 盈利概率:{round(self.win_ratio*100,2)}% \n保本概率:{round(self.break_even_ratio*100,2)}%\n亏损发生次数:{len(self.loss)} 亏损概率:{round((1-self.win_ratio-self.break_even_ratio)*100,2)}% \n平均盈利百分比:{round(np.mean(self.win)*100,2)}%   平均亏损百分比:{round(np.mean(self.loss)*100,2)}%\n盈利合约存续期平均天数:{round(np.mean(self.win_time),2)}'
        str3 = f'平均敲入交易日：{np.mean(self.knock_in_times)}\n平均敲出交易日:{np.mean(self.knock_out_times)}'
        str4 = f'平均敲出自然日:{round(np.mean(self.knock_out_ntimes),2)}'
        str5 = f'累计派息可覆盖期末亏损 发生次数:{self.loss_but_win} 比例:{loss_but_win}%'
        str6 = f'月中敲入但当月拿到派息比例:{knock_in_but_get_dividend}%'
        str7 = f'存续合约:{self.not_end_ttl}\n敲入：{len(self.not_end_in)} {nei}%\n未敲入:{len(self.not_end_not_in)} {neni}%'
        str8 = f'平均派息月：{mean(self.months)}'
        # 新增输出检查检查检查
        str9 = f'结束合约:{self.end_ttl}\n敲入 敲出：{len(self.in_and_out)} {io}%\n未敲入 敲出：{len(self.not_in_and_out)} {nio}%\n敲入 未敲出：{len(self.in_not_out)} {ino}%\n未敲入 未敲出:{len(self.not_in_not_out)} {nino}%'
        return str1 +'\n' + str2 +'\n' + str4  + '\n' + str5+'\n'+ str6 +'\n'+ str8 + '\n\n' + str7 

if __name__ == '__main__':  
    data=pd.read_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\data.csv',index_col=0)
    data.index=pd.to_datetime(data.index)
    # print(data['600519.SH'])

    low = datetime(2010,1,1)
    high = datetime(2024,9,10)
    bt1 = PhoenixSnowballBacktest(und_code='000852.SH',data=data,knock_in_price=75,strike_price=100, 
                           knock_out_price_rule='24M-100-0', 
                           ob_freq=1, 
                           tenure=24, 
                           lock_period=3, 
                           start_time=low,
                           end_time=high,
                           allo_period=1,
                           coupon_rate='24M-12-0',
                           eu_ki_switch=True,
                           fcn_switch = True,
                           ob_freq_ki='1D')
    print(bt1)
    
    # single_pho_sb = phoenix_snowball('600519.SH',data,70,'24M-100-0',datetime(2022,7,1),24,1,1,'1D','24M-10-0',True,True)
    # print(single_pho_sb)

    date_pd = data[(data.index >= low)&(data.index <= high)]
    snowball_pd = pd.DataFrame(index = date_pd.index,columns = ['end_date','status','is_knock_in','knock_in_date','is_knock_out','knock_out_date','payoff','get_dividend_months','ratio'])
    for i in range(len(bt1.snowballs)):
        single = []
        snowball = bt1.snowballs[i]
        end_date = snowball.end_date
        status = 'CLOSED' if snowball.is_end else 'ACTIVE'
        is_knock_out = snowball.is_knock_out
        knock_out_date = snowball.knock_out_date
        is_knock_in = snowball.is_knock_in
        knock_in_date = snowball.knock_in_date
        if snowball.is_end:
            payoff=snowball.payoff
        else:
            payoff=None
        
        get_dividend_months=snowball.get_dividend_months
        ratio=snowball.knock_in_but_get_dividend
        
        attr_list = [end_date, status, is_knock_in, knock_in_date, is_knock_out, knock_out_date,payoff,get_dividend_months,ratio]
        snowball_pd.loc[snowball.start_date] = attr_list
    #输出中间表
    snowball_pd.to_csv('C:\\Users\\tongyu\\Desktop\\雪球回测\\大盘雪球打分回测\\FCN-24M-3锁-KI75-KO100-行权100中间表.csv')