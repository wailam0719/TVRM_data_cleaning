"""
# y:
price

# x:
letter part
number part
afternoon (=1, 0 otherwise)
order in auction (starts with 1 and goes up)
date (in DDMMMYYYY)
year
month
unsold (=1, 0 otherwise)
special plate (check government website for rules or just check out the function special_plate() in loadData.py)
HSI closing nearest to auction day
CPI nearest to auction day
HSI 1-month percentage movement 
HSI 1-year percentage movement
split *
"""

import pandas as pd
import numpy as np
import datetime
from dateutil.parser import parse
import time
import re
from dateutil.relativedelta import *

start = time.time()
price = pd.DataFrame([])
x_item = pd.DataFrame([])
x_list = ['letters','numbers','afternoon','ordering','date','year','month','special','unsold','hsi','cpi','hsi_1m','hsi_1y']
HSI = pd.read_excel('TVRM_Indicator_HSI.xlsx', header = 0)
CPI = pd.read_excel('TVRM_Indicator_CPI.xlsx', header = 0)
for year in range(1990,2019):
    for month in range(1,13):
        for day in range(1,32):
            month_2d = '{:02d}'.format(month)
            day_2d = '{:02d}'.format(day)
            try:
                filepath = str(year)+"."+str(month)+"."+str(day)+".xlsx"
                auction_header = ['letters','numbers','price']
                auction = pd.read_excel(filepath,index_col = None,names = auction_header, header = None)
                date = str(month_2d)+"/"+str(day_2d)+"/"+str(year)
                auction['letters'] = auction['letters'].apply(str)
                auction['price'] = auction['price'].apply(str)
                auction = auction[auction.price != '$']
                auction['price'] = auction['price'].str.strip('@')
                length = len(auction)
                order = list(range(1,length+1))
                auction['letters'] = auction['letters'].str.strip('*')
                auction['ordering'] = order
                auction['date'] = pd.to_datetime(date)
                auction['year'] = year
                auction['month'] = month
                auction['unsold'] = np.where(auction['price']=='U/S',1,0)
                auction['numbers'] = pd.to_numeric(auction['numbers'], errors = 'ignore')
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
                
                auction['special_plate'] = auction.numbers.isin(special)
                auction['special'] = (auction["letters"].apply(len) == 0) | (auction["special_plate"] == True)
                
                if length == 115: 
                    auction['afternoon'] = np.where(auction.index > 60,1,0)
                elif length > 75:
                    am = length/2
                    auction['afternoon'] = np.where(auction.index > am,1,0)
                else:
                    auction['afternoon'] = 0

                HSI_date = [x for x in HSI['Date']] 
                pivot = datetime.datetime.strptime(date, "%m/%d/%Y")
                HSI_closestdate = sorted(HSI_date, key=lambda x: abs (x-pivot))[0]
                CPI_date = [y for y in CPI['Date ']]
                CPI_closestdate = sorted(CPI_date, key=lambda y: abs (y-pivot))[0]
                auction['cpi'] = CPI[CPI['Date '] == CPI_closestdate]['Composite CPI'].values[0]
                auction['hsi'] = HSI[HSI['Date'] == HSI_closestdate]['Close'].values[0]
                hsi = HSI[HSI['Date'] == HSI_closestdate]['Close'].values[0]
                one_month_pivot = HSI_closestdate - relativedelta(months=1)
                hsi_one_month = sorted(HSI_date, key=lambda x: abs (x-one_month_pivot))[0]
                hsi_1m = HSI[HSI['Date'] == hsi_one_month]['Close'].values[0]
                one_m_movement = (hsi-hsi_1m)/hsi_1m
                one_year_pivot = HSI_closestdate - relativedelta(years=1)
                hsi_one_year = sorted(HSI_date, key=lambda x: abs (x-one_year_pivot))[0]
                hsi_1y = HSI[HSI['Date'] == hsi_one_year]['Close'].values[0]
                one_y_movement = (hsi-hsi_1y)/hsi_1y
                auction['hsi_1y'] = one_y_movement
                auction['hsi_1m'] = one_m_movement
                objDate = datetime.datetime.strptime(date,"%m/%d/%Y")
                auction['date'] = str(objDate.strftime("%d%b%Y").lower())
                auction['numbers'] = auction['numbers'].astype(str)
    
                auction_price = []    
                for x in auction['price'].values:
                    auction_price.append(x.replace(",",""))
            
                auction['price'] = auction_price
                pd.to_numeric(auction['price'], errors = 'ignore')
        
                auction = auction[['letters','numbers','afternoon','ordering','price','date','year','month','special','unsold','hsi','cpi','hsi_1m','hsi_1y']]
                price = price.append(auction[['price']], ignore_index = False)
                x_item = x_item.append(auction[x_list],ignore_index = False)
            except IOError: 
                print('There is no file named', filepath)
end = time.time()
print("--- %s seconds ---" % (end - start))