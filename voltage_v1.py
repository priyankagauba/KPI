import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime


class V1N():

    def __init__(self, Upper_threshold, lower_threshold):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        self.Upper_threshold = Upper_threshold
        self.lower_threshold = lower_threshold

    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.TARGET_MEASUREMENT + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df

    def set_val(self, row):
        if row['EM_Voltage Ph1-N (V)'] > self.Upper_threshold:
            return "high"
        elif row['EM_Voltage Ph1-N (V)'] < self.lower_threshold:
            return "low"
        else:
            return "medium"

    def daily(self, df1):
        A = []
        df0 = df1[(df1['EM_Voltage Ph1-N (V)'] == 0)]
        df1 = df1[(df1['EM_Voltage Ph1-N (V)'] > 0)]

        time = df1['time'].max()

        MEAN = df1['EM_Voltage Ph1-N (V)'].mean()
        MAX = df1['EM_Voltage Ph1-N (V)'].max()
        MIN = df1['EM_Voltage Ph1-N (V)'].min()
        quantile25 = df1['EM_Voltage Ph1-N (V)'].quantile(0.25)
        quantile75 = df1['EM_Voltage Ph1-N (V)'].quantile(0.75)
        IQR = quantile75 - quantile25
        SD = df1['EM_Voltage Ph1-N (V)'].std()

        df1 = df1.assign(flag2=df1.apply(self.set_val, axis=1))
        y = pd.DataFrame(df1.groupby([(df1.flag2 != df1.flag2.shift()).cumsum()])['time'].apply(
            lambda y: (y.iloc[-1] - y.iloc[0]).total_seconds() / 60))
        y['flag2'] = df1.loc[df1.flag2.shift(-1) != df1.flag2]['flag2'].values
        y.reset_index(drop=True, inplace=True)

        duration_high = y[y['flag2'] == "high"].time.sum()
        duration_medium = y[y['flag2'] == "medium"].time.sum()
        duration_low = y[y['flag2'] == "low"].time.sum()

        duration_0 = df0.groupby(df0.index.to_series().diff().ne(1).cumsum())['time'].agg(
            lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()

        non_zeros = len(df1)
        zeros = len(df0)
        overlimit_count = len(df1[df1['flag2'] == "high"])
        underlimit_count = len(df1[df1['flag2'] == "low"])
        onlimit_count = len(df1[df1['flag2'] == "medium"])

        df1['voltage_difference'] = df1['EM_Voltage Ph1-N (V)'].diff(-1)
        Average_jump = abs(df1['voltage_difference']).mean()
        min_jump = abs(df1['voltage_difference']).min()
        max_jump = abs(df1['voltage_difference']).max()
        df1['jumps_instantaneous'] = np.where(abs(df1["voltage_difference"]) > (Average_jump * 1.1), 1, 0)
        jumps_instantaneous = len(df1[df1['jumps_instantaneous'] == 1])

        df1['delta_volt'] = df1['EM_Voltage Ph1-N (V)'] - MEAN
        df1['jumps_average'] = np.where(abs(df1["delta_volt"]) > (abs(df1['delta_volt']).mean() * 1.1), 1, 0)
        jumps_average = len(df1[df1['jumps_average'] == 1])

        upper_difference = MEAN - self.Upper_threshold
        lower_difference = MEAN - self.lower_threshold

        max_time= df1.loc[df1["EM_Voltage Ph1-N (V)"]==df1["EM_Voltage Ph1-N (V)"].max()]['time'].max()
        min_time = df1.loc[df1["EM_Voltage Ph1-N (V)"]==df1["EM_Voltage Ph1-N (V)"].min()]['time'].max()

        A = [time, MEAN, MAX, MIN, quantile25, quantile75, IQR, SD, upper_difference, lower_difference, duration_high,
             duration_medium, duration_low, duration_0, non_zeros, zeros, overlimit_count, underlimit_count,
             onlimit_count, Average_jump, min_jump, max_jump, jumps_instantaneous, jumps_average,max_time,min_time]
        FINAL = pd.DataFrame(
            columns=['time', 'MEAN', 'MAX', 'MIN', 'quantile25', 'quantile75', 'IQR', 'SD', 'upper_difference',
                     'lower_difference', 'duration_high', 'duration_medium', 'duration_low', 'duration_0', 'non_zeros',
                     'zeros', 'overlimit_count', 'underlimit_count', 'onlimit_count', 'Average_jump', 'min_jump',
                     'max_jump', 'jumps_instantaneous', 'jumps_average','max_time','min_time'], data=np.array(A).reshape(-1, len(A)))
        return FINAL

    def sensor(self):
        df = self.read_Data()
        print(df.head())

        final_sen = df.groupby('DeviceID').apply(self.daily)
        final_sen.reset_index(inplace=True)
        final_sen.drop(final_sen.columns[1], axis=1, inplace=True)
        final_sen.set_index('time', inplace=True)
        final_sen = final_sen.loc[pd.notnull(final_sen.index)]

        for col in [ 'MEAN', 'MAX', 'MIN', 'quantile25', 'quantile75', 'IQR', 'SD', 'upper_difference',
                     'lower_difference', 'duration_high', 'duration_medium', 'duration_low', 'duration_0', 'non_zeros',
                     'zeros', 'overlimit_count', 'underlimit_count', 'onlimit_count', 'Average_jump', 'min_jump',
                     'max_jump', 'jumps_instantaneous', 'jumps_average']:
            final_sen[col] = final_sen[col].astype(np.float64)

        #print(final_sen.dtypes)
        print(self.DFDBClient.write_points(final_sen, cg.VoltagePH1toN_MEASUREMENT))
        return final_sen


agg1 = V1N(245, 195)
out = agg1.sensor()
