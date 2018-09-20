"""
Write a class called ‘data’.

Class methods:

1.      load_csv(filename)
This method loads a single csv file and parse it to dataframe.
2.      load_excel(filename)
This method loads a single Excel file and parse it to dataframe.
3.      load_csv_from_dir(path)
This method loads all csv files in a directory and merge all of them into a single dataframe.
4.      load_excel_from_dir(path)
This method loads all Excel files in a directory and merge all of them into a single dataframe.
 

Instance variables:

(Instance variables need to be created in the __init__() method and unique to each instance of a class.)

1.      X
2.      y

If no data has been read, these two variables should be of type None.

import data

'mydata is an instance of the data class'
mydata = data()

Import Excel files from a folder and store the data internally
mydata.load_csv_from_dir("c:/data/plates")

#Fetch data
X,y = mydata.get_data()
"""
import pandas as pd
import numpy as np
import os
import datetime
from dateutil.parser import parse
import time
import re
from dateutil.relativedelta import *

#Data
class Data: 
	HSI = pd.read_excel('TVRM_Indicator_HSI.xlsx', header = 0)
	CPI = pd.read_excel('TVRM_Indicator_CPI.xlsx', header = 0)
	auction_header = ['letters','numbers','price']
	x_list = ['letters','numbers','afternoon','ordering','date','year','month','special','unsold','hsi','cpi','hsi_1m','hsi_1y']   
    
    def __init__(self):
        self.x = pd.DataFrame([])
	    self.y = pd.DataFrame([])

    @classmethod
    def parsing_dataframe(self,filename):
        if filename.endswith('.csv'):
            cls.auction = pd.read_csv(filename,index_col = None,names = Data.auction_header, header = None)
        elif filename.endswith('.xlsx'):
            cls.auction = pd.read_excel(filename,index_col = None,names = Data.auction_header, header = None)
        else:
            pass

    @classmethod
    def cleanning_auction(cls,filename,date):
        cls.parsing_dataframe(filename)
        cls.auction['price'] = cls.auction['price'].apply(str)
        cls.auction = cls.auction[cls.auction.price != '$']
        cls.auction['price'] = cls.auction['price'].str.strip('@')
        cls.auction['price'] = cls.auction['price'].replace(",","")
        cls.auction['letters'] = cls.auction['letters'].apply(str)
        cls.auction['letters'] = cls.auction['letters'].str.strip('*')
        length = len(cls.auction)
        order = list(range(1,length+1))
        cls.auction['ordering'] = order
        cls.auction['date'] = date
        cls.auction['year'] = date.year
        cls.auction['month'] = date.month
        cls.auction['unsold'] = np.where(cls.auction['price']=='U/S',1,0)
        cls.auction['numbers'] = pd.to_numeric(cls.auction['numbers'], errors = 'ignore')
        special = [1,2,3,4,5,6,7,8,9,11,22,33,44,55,66,77,88,99,111,222,333,444,555,666,777,888,999,1111,2222,3333,4444,5555,6666,7777,8888,9999,
                    10, 20, 30, 40, 50, 60, 70, 80, 90,
                    100, 200, 300, 400, 500, 600, 700, 800, 900,
                    1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
                    123, 234, 345, 456, 567, 678, 789,
                    1234, 2345, 3456, 4567, 5678, 6789,
                    12, 13, 14, 15, 16, 17, 18, 19, 21, 23, 24, 25, 26, 27, 28, 29, 31, 32, 34, 35, 36, 37, 38, 39, 41, 42, 43, 45, 46, 47, 48, 49, 51, 52, 53, 54, 56, 57, 58, 59, 61, 62, 63, 64, 65, 67, 68, 69, 71, 72, 73, 74, 75, 76, 78, 79, 81, 82, 83, 84, 85, 86, 87, 89, 91, 92, 93, 94, 95, 96, 97, 98,
                    1100, 1122, 1133, 1144, 1155, 1166, 1177, 1188, 1199, 2200, 2211, 2233, 2244, 2255, 2266, 2277, 2288, 2299, 3300, 3311, 3322, 3344, 3355, 3366, 3377, 3388, 3399, 4400, 4411, 4422, 4433, 4455, 4466, 4477, 4488, 4499, 5500, 5511, 5522, 5533, 5544, 5566, 5577, 5588, 5599, 6600, 6611, 6622, 6633, 6644, 6655, 6677, 6688, 6699, 7700, 7711, 7722, 7733, 7744, 7755, 7766, 7788, 7799, 8800, 8811, 8822, 8833, 8844, 8855, 8866, 8877, 8899, 9900, 9911, 9922, 9933, 9944, 9955, 9966, 9977, 9988,
                    1001, 1221, 1331, 1441, 1551, 1661, 1771, 1881, 1991, 2002, 2112, 2332, 2442, 2552, 2662, 2772, 2882, 2992, 3003, 3113, 3223, 3443, 3553, 3663, 3773, 3883, 3993, 4004, 4114, 4224, 4334, 4554, 4664, 4774, 4884, 4994, 5005, 5115, 5225, 5335, 5445, 5665, 5775, 5885, 5995, 6006, 6116, 6226, 6336, 6446, 6556, 6776, 6886, 6996, 7007, 7117, 7227, 7337, 7447, 7557, 7667, 7887, 7997, 8008, 8118, 8228, 8338, 8448, 8558, 8668, 8778, 8998, 9009, 9119, 9229, 9339, 9449, 9559, 9669, 9779, 9889,
                    101, 121, 131, 141, 151, 161, 171, 181, 191, 202, 212, 232, 242, 252, 262, 272, 282, 292, 303, 313, 323, 343, 353, 363, 373, 383, 393, 404, 414, 424, 434, 454, 464, 474, 484, 494, 505, 515, 525, 535, 545, 565, 575, 585, 595, 606, 616, 626, 636, 646, 656, 676, 686, 696, 707, 717, 727, 737, 747, 757, 767, 787, 797, 808, 818, 828, 838, 848, 858, 868, 878, 898, 909, 919, 929, 939, 949, 959, 969, 979, 989,
                    1010, 1212, 1313, 1414, 1515, 1616, 1717, 1818, 1919, 2020, 2121, 2323, 2424, 2525, 2626, 2727, 2828, 2929, 3030, 3131, 3232, 3434, 3535, 3636, 3737, 3838, 3939, 4040, 4141, 4242, 4343, 4545, 4646, 4747, 4848, 4949, 5050, 5151, 5252, 5353, 5454, 5656, 5757, 5858, 5959, 6060, 6161, 6262, 6363, 6464, 6565, 6767, 6868, 6969, 7070, 7171, 7272, 7373, 7474, 7575, 7676, 7878, 7979, 8080, 8181, 8282, 8383, 8484, 8585, 8686, 8787, 8989, 9090, 9191, 9292, 9393, 9494, 9595, 9696, 9797, 9898]
        cls.auction['special_plate'] = cls.auction.numbers.isin(special)
        cls.auction['special'] = (cls.auction["letters"].apply(len) == 0) | (cls.auction["special_plate"] == True)
        cls.auction['price'] = pd.to_numeric(cls.auction['price'], errors = 'ignore')
        
        if length == 115: 
            cls.auction['afternoon'] = np.where(cls.auction.index > 60,1,0)
        elif length > 75:
            am = length/2
            cls.auction['afternoon'] = np.where(cls.auction.index > am,1,0)
        else:
            cls.auction['afternoon'] = 0

        
        HSI_date = [x for x in Data.HSI['Date']] 
        pivot = date
        HSI_closestdate = sorted(HSI_date, key=lambda x: abs (x-pivot))[0]
        CPI_date = [y for y in Data.CPI['Date ']]
        CPI_closestdate = sorted(CPI_date, key=lambda y: abs (y-pivot))[0]
        cls.auction['cpi'] = Data.CPI[Data.CPI['Date '] == CPI_closestdate]['Composite CPI'].values[0]
        cls.auction['hsi'] = Data.HSI[Data.HSI['Date'] == HSI_closestdate]['Close'].values[0]
        hsi = Data.HSI[Data.HSI['Date'] == HSI_closestdate]['Close'].values[0]
        one_month_pivot = HSI_closestdate - relativedelta(months=1)
        hsi_one_month = sorted(HSI_date, key=lambda x: abs (x-one_month_pivot))[0]
        hsi_1m = Data.HSI[Data.HSI['Date'] == hsi_one_month]['Close'].values[0]
        one_m_movement = (hsi-hsi_1m)/hsi_1m
        one_year_pivot = HSI_closestdate - relativedelta(years=1)
        hsi_one_year = sorted(HSI_date, key=lambda x: abs (x-one_year_pivot))[0]
        hsi_1y = Data.HSI[Data.HSI['Date'] == hsi_one_year]['Close'].values[0]
        one_y_movement = (hsi-hsi_1y)/hsi_1y
        cls.auction['hsi_1y'] = one_y_movement
        cls.auction['hsi_1m'] = one_m_movement
        cls.auction['date'] = str(date.strftime("%d%b%Y"))
        cls.auction['numbers'] = cls.auction['numbers'].astype(str)
        



	def load_csv(self,filename):
		date_str = os.path.basename(filename).strip('.csv')
		date = pd.to_datetime(date_str)
        Data.cleanning_auction(filename,date)
        self.x = self.x.append(self.auction[Data.x_list],ignore_index = False)
        self.y = self.y.append(self.auction[['price']], ignore_index = False)
        




	def load_excel(self,filename):
		date_str = os.path.basename(filename).strip('.xlsx')
		date = pd.to_datetime(date_str)
        Data.cleanning_auction(filename,date)
        self.x = self.x.append(self.auction[Data.x_list],ignore_index = False)
        self.y = self.y.append(self.auction[['price']], ignore_index = False)
        



    def load_csv_from_dir(self,path):
        filepath = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.csv'):
                    filepath.append(os.path.join(root,file))
                else:
                    pass

        for file in filepath:
            date_str = os.path.basename(file).strip('.csv')
            date = pd.to_datetime(date_str)
            Data.cleanning_auction(file,date)
            self.x = self.x.append(self.auction[Data.x_list],ignore_index = False)
            self.y = self.y.append(self.auction[['price']], ignore_index = False)
            
					

    def load_excel_from_dir(self,path):
        filepath = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.xlsx'):
                    filepath.append(os.path.join(root,file))
                else:
                    pass

        for file in filepath:
            date_str = os.path.basename(file).strip('.xlsx')
            date = pd.to_datetime(date_str)
            Data.cleanning_auction(file,date)
            self.x = self.x.append(self.auction[Data.x_list],ignore_index = False)
            self.y = self.y.append(self.auction[['price']], ignore_index = False)

    def get_data(self):
        return self.x, self.y
            		



