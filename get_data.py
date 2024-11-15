import pandas as pd
import numpy as np
from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env
path='C:\\Users\\tongyu\\Desktop\\雪球回测\\'

#输入一个列表，里面是想提取数据的标的，输出的data格式：一个索引是日期，列名是标的代码，数据是收盘价的Dataframe
def get_data(und_code_list):
    data=pd.read_csv(f'{path}data.csv',index_col=0)
    data.index=pd.to_datetime(data.index)

    LoginUtils.set_auth_info(
        username='intern2',
        password='Intern123456'
    )
    for und_code in und_code_list:
        if und_code not in data.columns:
            instrument_id_list = [und_code]
            start_date = "2004-01-01"   # 提取数据的日期,可根据需要修改
            end_date = "2024-07-26"
            result = get_quote_close(
                    instrument_id_list=instrument_id_list,
                    start_date=start_date,
                    end_date=end_date
                )
            res_df=pd.DataFrame(result)
            res_df['tradeDate']=pd.to_datetime(res_df['tradeDate'])
            res_df=res_df.set_index('tradeDate').sort_index()
            data[und_code]=res_df['closePrice']
    data.to_csv(f'{path}data.csv')
    return data   

if __name__=='__main__':
    date=get_data(['000016.SH','000300.SH','000905.SH','000852.SH','HSTECH.HI','600519.SH','601857.SH','515030.SH','300750.SZ'])  #提取数据的标的
    LoginUtils.set_auth_info(
        username='intern2',
        password='Intern123456'
    )
    # result = get_quote_close(
    #                 instrument_id_list=['000905.SH'],
    #                 start_date="2004-01-01",
    #                 end_date="2024-07-19")
    # print(result)
    # print(date)