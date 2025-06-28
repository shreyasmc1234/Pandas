
#extraction

# I have a csv file coming into a folder evey morning 12 am cst(central standard time). the csv file is sent by the finance team ,
#we need to read the csv file into a df and then we need to tranform the data which includes the below issues.


#tranformation
#1) we need to remove duplicates
#2) fill null values with  mean if its a numerical value, if its date value fill it with 01012025, if its a string value fill it with unknown
#2) create new aggregrated columns which inlcudes the below columns:
#Total revenue
#total expenses
#profit
#gross margin
#operating income
#accounts receivables
#accounts payables.
#year ove year growth


#loading
#Once the data is cleaned move the data in parquet format and save the data in another folder.

#Medallion Architecture

#Bronze Layer: Raw data


#Silver layer: tranformations (drop duplicates, fixing null values, changing datatypes, removing null values)


#Gold Layer: Aggregate columns are built for reporting. (this is where the reporting happens from)






import pandas as pd
df=pd.read_csv('d:\\data\\data.csv')

########  Analysing the data  ####### 

# df.info()
# df.head(20)

#######  Trasforming the data  #######


#1) we need to remove duplicates
#2) fill null values with  mean if its a numerical value, if its date value fill it with 01012025, if its a string value fill it with unknown
#2) create new aggregrated columns which inlcudes the below columns:
#Total revenue
#total expenses
#profit
#gross margin
#operating income
#accounts receivables
#accounts payables.
#year ove year growth


dt=df.copy()
dt.dropna(inplace=True)

dt['Revenue'].fillna(dt['Revenue'].mean(),inplace=True)
dt['COGS'].fillna(dt['COGS'].mean(),inplace=True)
dt['Expenses'].fillna(dt['Expenses'].mean(),inplace=True)
dt['Operating_Expenses'].fillna(dt['Operating_Expenses'].mean(),inplace=True)
dt['Depreciation'].fillna(dt['Depreciation'].mean(),inplace=True)
dt['Amortization'].fillna(dt['Amortization'].mean(),inplace=True)

dt['Date'].fillna('01012025')

dt['Department'].fillna('Unknown',inplace=True)



#aggregrated columns
#total revenue: revenue
#total expense: expense
#profit: revenue - expense
#gross margin: (revenue - COGS)/ revenue
#operating income : revenue-operating expense
#EBITDA (earnings befre interest, tax, depreciation and amortization): operatingincome+depreciation + amortization
#accounts receivables: AR
#accounts payables. AP
#year ove year growth : 5% . yoy = revenue *0.05

dt['Total_revenue']=dt['Revenue']
dt['Total_expense']=dt['Expenses']
dt['Profit']=dt['Revenue']-dt['Expenses']
dt['Gross_margin']=(dt['Revenue']-dt['COGS'])/dt['Revenue']
dt['Operating_Income']=dt['Revenue']-dt['Operating_Expenses']
dt['EBITDA']=dt['Operating_Income']+dt['Depreciation']+dt['Amortization']
dt['Account_Recievables']=dt['AR']
dt['Account_Payables']=dt['AP']
dt['Growth']=dt['Revenue']*(5/100)

