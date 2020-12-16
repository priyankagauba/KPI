import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class Power_And_Energy():

    def __init__(self):
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
    
    def __call__(self):
        self.output()
    
    def read_Data(self):
        con_obj = InfluxDBClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        query = 'select * from ' + cg.TARGET_MEASUREMENT  + ' where time > now() - 1d '
        df = pd.DataFrame(con_obj.query(query, chunked=True, chunk_size=10000).get_points())
        df['time'] = df['time'].astype('datetime64[ns]')
        df['time'] = df['time'] + datetime.timedelta(hours=5, minutes=30)
        return df
    
       
    def energy_difference(self, df):
        e = df.groupby(pd.Grouper(freq='30T',key='time'))['EM_TOTAL_Import_Energy(kWh)'].apply(lambda x:x.iloc[-1]-x.iloc[0])
        return e
    
    def power_difference(self, df):
        p = df.groupby(pd.Grouper(freq='30T',key='time'))['EM_Active Power (kW)'].mean()
        return p

    def output(self):
        df = self.read_Data()
        y = pd.DataFrame(df.groupby('DeviceID').apply(self.energy_difference))
        y = y.unstack(level = 0)
        y.columns = y.columns.droplevel()
        y = y.rename_axis(None, axis = 1)
        y = y.fillna(0)
        x = pd.DataFrame(df.groupby('DeviceID').apply(self.power_difference))
        x = x.unstack(level = 0)
        x.columns = x.columns.droplevel()
        x = x.rename_axis(None, axis = 1)
        x = x.fillna(0)
        print(self.DFDBClient.write_points(x, cg.POWER_AND_ENERGY))
        return (y,x)
        


if __name__ == '__main__':
    cat = Power_And_Energy()
    y,x = cat.output()
    print(x)
    print(y)
