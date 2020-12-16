import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
import datetime


class current_mean():

    def __init__(self, a):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        self.a = a

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 1d '
        self.df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        self.df['time'] = pd.to_datetime(self.df['time'])
        self.df['time'] = self.df['time'] + datetime.timedelta(hours=5, minutes=30)
        return self.df

    def daily(self, df1):
        df0 = df1[(df1['mean_current'] == 0)]
        df1 = df1[(df1['mean_current'] > 0)]

        time = df1['time'].max()

        MEAN = df1['mean_current'].mean()
        MAX = df1['mean_current'].max()
        MIN = df1['mean_current'].min()
        quantile25 = df1['mean_current'].quantile(0.25)
        quantile75 = df1['mean_current'].quantile(0.75)
        IQR = quantile75 - quantile25
        SD = df1['mean_current'].std()

        df1['flag2'] = 'normal'
        for i in self.a.keys():
            x = df1[df1['DeviceID'] == i]
            x['flag2'] = np.where(x['mean_current'] > self.a[i], 'high', x['flag2'])
            df1[df1['DeviceID'] == i] = x


        y = pd.DataFrame(df1.groupby([(df1.flag2 != df1.flag2.shift()).cumsum()])['time'].apply(
            lambda y: (y.iloc[-1] - y.iloc[0]).total_seconds() / 60))
        y['flag2'] = df1.loc[df1.flag2.shift(-1) != df1.flag2]['flag2'].values
        y.reset_index(drop=True, inplace=True)

        duration_high = y[y['flag2'] == "high"].time.sum()
        duration_normal = y[y['flag2'] == "normal"].time.sum()

        duration_0 = df0.groupby(df0.index.to_series().diff().ne(1).cumsum())['time'].agg(
            lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()

        non_zeros = len(df1)
        zeros = len(df0)
        overlimit_count = len(df1[df1['flag2'] == "high"])
        onlimit_count = len(df1[df1['flag2'] == "normal"])
        df1['current_difference'] = df1['mean_current'].diff(-1)
        Average_jump = abs(df1['current_difference']).mean()
        min_jump = abs(df1['current_difference']).min()
        max_jump = abs(df1['current_difference']).max()
        df1['jumps_instantaneous'] = np.where(abs(df1["current_difference"]) > (Average_jump * 1.1), 1, 0)
        jumps_instantaneous = len(df1[df1['jumps_instantaneous'] == 1])

        df1['delta_current'] = df1['mean_current'] - MEAN
        df1['jumps_average'] = np.where(abs(df1["delta_current"]) > (abs(df1['delta_current']).mean() * 1.1), 1, 0)
        jumps_average = len(df1[df1['jumps_average'] == 1])

        df1['scs'] = np.where((df1['mean_current'] > 25), 1, 0)
        x = df1[['scs', 'mean_current', 'DeviceID']]
        y = x[['scs']].rolling(2).apply(lambda x: x[0] != x[-1], raw=True).sum().astype(int)
        scs=y.item()

        A = [time, MEAN, MAX, MIN, quantile25, quantile75, IQR, SD, duration_high, duration_normal, duration_0,
             non_zeros, zeros, overlimit_count, onlimit_count, Average_jump, min_jump, max_jump, jumps_instantaneous,
             jumps_average,scs]
        FINAL = pd.DataFrame(
            columns=['time', 'MEAN', 'MAX', 'MIN', 'quantile25', 'quantile75', 'IQR', 'SD', 'duration_high',
                     'duration_normal', 'duration_0', 'non_zeros', 'zeros', 'overlimit_count', 'onlimit_count',
                     'Average_jump', 'min_jump', 'max_jump', 'jumps_instantaneous', 'jumps_average','scs'],
            data=np.array(A).reshape(-1, len(A)))
        return FINAL



    def sensor(self):
        df = self.read_Data()
        final_sen = df.groupby(df['DeviceID']).apply(self.daily)
        final_sen.reset_index(inplace=True)
        final_sen.drop(final_sen.columns[1], axis=1, inplace=True)
        final_sen.set_index('time', inplace=True)
        final_sen = final_sen.loc[pd.notnull(final_sen.index)]
        print(self.DFDBClient.write_points(final_sen, cg.CurrentPH1_MEASUREMENT))
        return final_sen


a = {'EM1': 1635, 'EM2': 1020, 'EM3': 1020, 'EM4': 820, 'EM5': 820, 'EM6': 83, 'EM7': 156, 'EM8': 160, 'EM9': 160,
     'EM10': 160, 'EM22': 23.5, 'EM23': 33, 'CT11': 29, 'V11': 29, 'CT12': 16, 'CT13': 16, 'CT14': 16, 'CT15': 16,
     'CT16': 16, 'CT17': 16, 'CT18': 16, 'CT19': 16, 'CT20': 31}
agg1 = current_mean(a)
out = agg1.sensor()
