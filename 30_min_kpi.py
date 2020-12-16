import pandas as pd
import datetime
from math import *
import Config as cg
from influxdb import DataFrameClient
from pytz import *


class PowerFactorLoss_KPI():
    block_values = []

    def __init__(self, base_power, fluctuation_threshold):
        self.base_power = base_power
        self.fluctuation_threshold = fluctuation_threshold
        self.DFDBClient = DataFrameClient(host=cg.INFLUX_DB_IP, port=cg.INFLUX_DB_PORT, database=cg.INFLUX_DB)
        print('init function called')

    def __call__(self):
        self.execute()
        print('execute function executed')

    def record_30_min(self):
        query = "SELECT * FROM " + str(cg.TARGET_MEASUREMENT) + " WHERE time > now() - 30m"
        result = self.DFDBClient.query(query)

        for record in result.items():
            df = record[1]
            df['time'] = df.index + datetime.timedelta(minutes=330)
            return df

    def initialize(self, power_t, power_factor_t, current_time, thd_t, mean_volt_t):
        self.block_counter = 1
        self.block_average_power = power_t
        self.block_average_pf = power_factor_t
        self.block_start_time = current_time
        self.block_average_thd = thd_t
        self.block_average_voltage = mean_volt_t
        return

    def __power_fluctuation(self, power_t):
        if abs(power_t - self.block_average_power) / self.block_average_power <= self.fluctuation_threshold:
            return 0
        else:
            return 1

    def __update_average_power(self, power_t):
        return (self.block_average_power * self.block_counter + power_t) / (self.block_counter + 1)

    def __update_average_power_factor(self, power_factor_t):
        return (self.block_average_pf * self.block_counter + power_factor_t) / (self.block_counter + 1)

    def __update_average_thd(self, thd_t):
        return (self.block_average_thd * self.block_counter + thd_t) / (self.block_counter + 1)

    def __update_average_voltage(self, mean_volt_t):
        return (self.block_average_voltage * self.block_counter + mean_volt_t) / (self.block_counter + 1)

    def check(self, power_t, power_factor_t, current_time, thd_t, mean_volt_t):
        if self.__power_fluctuation(power_t):
            self.block_values.append([(current_time - self.block_start_time).total_seconds(), self.block_average_power,
                                      self.block_average_pf, self.block_average_thd, self.block_average_voltage, ])
            self.initialize(power_t, power_factor_t, current_time, thd_t, mean_volt_t)
        else:
            self.block_average_power = self.__update_average_power(power_t)
            self.block_average_pf = self.__update_average_power_factor(power_factor_t)
            self.block_average_thd = self.__update_average_thd(thd_t)
            self.block_average_voltage = self.__update_average_voltage(mean_volt_t)
            self.block_counter += 1

    def execute(self):
        try:
            print('Above record 30 min')
            df = self.record_30_min()
            print('Below record 30 min')

            for j in list(pd.unique(df['DeviceID'])):
                df_new = df[df['DeviceID'] == j]  # creating sub-dataframe
                df_new = df_new.reset_index(drop=True)
                self.initialize(df_new['EM_Active Power (kW)'].iloc[0], df_new['EM_Power Factor'].iloc[0],
                                df_new['time'].iloc[0],
                                df_new['Mean_THD'].iloc[0],
                                df_new['mean_volt'].iloc[0])  # discuss regarding df.loc[0, 'time'] why it is needed

                for i in range(1, len(df_new) - 1):
                    self.check(df_new['EM_Active Power (kW)'].iloc[i], df_new['EM_Power Factor'].iloc[i],
                               df_new['time'].iloc[i],
                               df_new['Mean_THD'].iloc[i], df_new['mean_volt'].iloc[i])

                df_x = pd.DataFrame(self.block_values)

                def function_cal(row):
                    # print(row[1], row[3], row[4])
                    temp_ = 1 - (1 / sqrt(1 + (row[3] ** 2)))
                    return (row[1] * 1000) * temp_ / (sqrt(3) * row[4])

                df_x['5'] = df_x.apply(lambda x: function_cal(x), axis=1)
                df_x.columns = ['Time_interval', 'Avg_power', 'Avg_pf', 'Avg_thd', 'Avg_vol', 'Loss']

                # ------------------------------------- initializating some of the variables
                kvah_loss_pf = 0
                kvah_loss_thd = 0
                kvah_loss_total = 0
                kVArh_lag = 0
                kVArh_lead = 0
                cycle_power_factor = 0

                for i in range(0, len(df_x)):
                    kvah_loss_pf = kvah_loss_pf + ((df_x['Avg_power'].iloc[i] * df_x['Time_interval'].iloc[i]) / 3600) * (
                            (1 / df_x['Avg_pf'].iloc[i]) - (1 / 0.99))
                    kvah_loss_thd = kvah_loss_thd + (
                            sqrt(3) * (
                            df_x['Avg_vol'].iloc[i] * df_x['Loss'].iloc[i] * df_x['Time_interval'].iloc[i]) / 3600000)
                    if isnan(kvah_loss_pf) or isinf(kvah_loss_pf):
                        kvah_loss_pf = 0.0
                    if isnan(kvah_loss_thd) or isinf(kvah_loss_thd):
                        kvah_loss_thd = 0.0
                    kvah_loss_total = kvah_loss_pf + kvah_loss_thd

                    print(kvah_loss_pf, kvah_loss_thd, kvah_loss_total)

                    print(kvah_loss_pf, kvah_loss_thd, kvah_loss_total)

                    print('Final prepared')
                    india = timezone('Asia/kolkata')
                    time = datetime.datetime.now(india)  # - datetime.timedelta(hours=5, minutes=30)
                    json_body = pd.DataFrame([[kvah_loss_pf, kvah_loss_thd, kvah_loss_total, j]],
                                             columns=['kvah_loss_pf', 'kvah_loss_thd', 'kvah_loss_total', 'DeviceID'],
                                             index=[time])
                    #print(self.DFDBClient.write_points(json_body, cg.WRITE_MEASUREMENT))

            return 'Executed successfully'
        except Exception as e:
            print('Exception in half hour scheduling:'+str(e))
