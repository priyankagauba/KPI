import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from influxdb import *
import Config as cg
from influxdb import DataFrameClient
import datetime



class THD_Mean():
    
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
        
    def categorization_time(self,df):
        bins = [0, 5.1, 6, np.inf]
        names = ['Time_Good', 'Time_Warning', 'Time_Critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['Mean_THD'], bins))
        return df
    
    def calculate_time(self,df): 
        df=self.categorization_time(df)
        x=pd.DataFrame(df.groupby([(df.Status != df.Status.shift()).cumsum()])['time'].apply(lambda x:(x.iloc[-1]-x.iloc[0]).total_seconds()/60))
        x['Status']=df.loc[df.Status.shift(-1) != df.Status]['Status'].values
        x.reset_index(drop=True,inplace=True)
        return x
    
    def categorization_count(self,df):
        bins = [0, 5.1, 6, np.inf]
        names = ['Count_Good', 'Count_Warning', 'Count_Critical']
        d = dict(enumerate(names, 1))
        df['Status'] = np.vectorize(d.get)(np.digitize(df['Mean_THD'], bins))
        return df
    
    def calculate_count(self,df): 
        df=self.categorization_count(df)
        c = pd.DataFrame(df.groupby(['DeviceID', 'Status']).size()).reset_index()
        c = pd.pivot_table(index ='DeviceID', columns ='Status', values = 0, data=c,aggfunc=np.sum).astype(np.float64)
        c.reset_index(inplace=True)
        return c
    
    def time_as_index(self,df):
        t = pd.DataFrame(df.groupby(['DeviceID'])['time'].max())
        t.reset_index(inplace = True)
        return t
    
    def time(self,df):
        df['Time_max'] = df['time'].dt.time
        a = df.loc[list(df.groupby('DeviceID')['Mean_THD'].idxmax())][['Time_max','DeviceID']]
        b = df.loc[list(df.groupby('DeviceID')['Mean_THD'].idxmin())][['Time_max','DeviceID']]
        a.index = a['DeviceID']
        a = a.drop('DeviceID',axis=1)
        a['Time_min']=list(b['Time_max'])
        return a
     
    def zero_duration(self,df):
        p = df.index.to_series().diff()!=1
        p = df.groupby(p.cumsum())['time'].agg(lambda x: abs((x.iloc[-1] - x.iloc[0]).total_seconds() / 60)).sum()
        return p    
        
    def output(self):
        df = self.read_Data()
        t = self.time_as_index(df)
        df0 = df[df['Mean_THD'] == 0]
        df = df[df['Mean_THD'] > 0]
        x = df.groupby('DeviceID')['Mean_THD'].describe()
        x.reset_index(inplace=True)
        x.columns = ['DeviceID', 'Total_Count', 'Average', 'SD', 'Minimum', '25th_percentile', 'Median', '75th_percentile', 'Maximum']
        x = x.merge(t,on='DeviceID', how = "outer")
        y=df.groupby('DeviceID').apply(self.calculate_time)
        y=y.reset_index()
        y=pd.pivot_table(index ='DeviceID', columns ='Status', values = 'time',data=y,aggfunc=np.sum).astype(np.float64) 
        y.reset_index(inplace=True) 
        x=x.merge(y,on='DeviceID', how = "outer")
        a=self.time(df)
        x=x.merge(a,on='DeviceID', how = "outer")
        c = self.calculate_count(df)
        x = x.merge(c,on='DeviceID', how = "outer")
        if(len(df0) != 0):
            o = pd.DataFrame(df0.groupby(['DeviceID']).size(), columns = ['count_0']).reset_index()
            x = x.merge(o, on='DeviceID', how = "outer")
            p = pd.DataFrame(df0.groupby(['DeviceID']).apply(self.zero_duration),columns=['Duration_0']).reset_index()
            x = x.merge(p,on='DeviceID', how = "outer")
        x = x.fillna(0)
        x.set_index('time', inplace = True)
        print(self.DFDBClient.write_points(x, cg.THD_MEAN))
        return x    
             
  



if __name__ == '__main__':
    cat = THD_Mean()
    t=cat.output()
    print(t.columns)
    
      