#Creadit given to Miss Diyana 
import pandas as pd
from pymongo import MongoClient
import pymongo
import numpy as np

client = MongoClient('localhost', 27017) # MongodDb connection
db = client['tibit_pon_controller'] # db name

col = db['STATS-OLT-70b3d55236da'] # collection name
col2 = db['STATS-ONU-ALPHe3a69d67'] # collection 2
col3 = db['STATS-ONU-ALPHe3a69d94']# collection 3

res = col.find() # get all record
res2 =col2.find() # get all record
res3 = col3.find() #get all record
dates = [] # create empty dates array
dates2 = [] # empty array for dates in collection two
dates3 = []# empty array for dates in collection three
for r in res: #loop all record from mongo
    dates.append(r['_id'].split(" ")[0]) # append each date only take the date

for r in res2: #loop all record from mongo
    dates2.append(r['_id'].split(" ")[0]) # append each date only take the date
for r in res3: #loop all record from mongo
    dates3.append(r['_id'].split(" ")[0]) # append each date only take the date

df = pd.DataFrame() # create empty dataframe
df2 = pd.DataFrame() # create empty dataframe
df3 = pd.DataFrame() # create empty dataframe
df['dates'] = dates #assign dates added at line 14 to dataframe create above
df2['dates'] = dates2 #assign dates added at line 14 to dataframe create above
df3['dates'] = dates3 #assign dates added at line 14 to dataframe create above

dates = df['dates'].unique()
dates2 = df2['dates'].unique()
dates3 = df3['dates'].unique()

sum=0
sumtx=0
sumponrx=0
sumpontx =0

d1 = [] #create date1 empty array
d2 = [] # temporary array for sorting date C2
d3 = [] # temporary array for sorting data c3

sumoltnni = []
sumoltnnitx = []
sumoltpon = []
sumoltpontx =[]
value_test =[]
value_test2 =[]
value_test3=[]
check = pd.DataFrame()

#Collection one retrieve data
for d in dates: #loop over all unique date from line 18
    #print(d) #print the current date running for the list
    #print(count)
    res = list(col.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
   # print("lenght for" ,'d', len(res))
    oltnni = []
    oltpon = []
    oltnnitx=[]
    oltpontx = []

    for b in range(len(res)):
        df = pd.DataFrame(res[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        oltnni.append(df[df['index']=='RX Frames Green']['OLT-NNI'].iloc[0]) # filter for speicific index and add to array
        oltpon.append(df[df['index']=='RX Frames Green']['OLT-PON'].iloc[0]) # same as above
        sum=sum+oltnni[b]
        sumponrx+=oltpon[b]
 
        oltnnitx.append(df[df['index']=='TX Frames Green']['OLT-NNI'].iloc[0]) # filter for speicific index and add to array
        oltpontx.append(df[df['index']=='TX Frames Green']['OLT-PON'].iloc[0]) # same as above
        sumtx+=oltnnitx[b]
        sumpontx+=oltpontx[b]


    sumoltnni.append(sum)
    sumoltnnitx.append(sumtx)
    sumoltpon.append(sumponrx)
    sumoltpontx.append(sumpontx)

    sum=0
    sumtx=0
    sumponrx=0
    sumpontx =0
    d1.append(d) 
    

#Variable temporary for collection two
sumc2rx=0
sumc2tx=0
sum_all_c2rx=[]
sum_all_c2tx=[]

#Collection two retrieve data
for d in dates2: #loop over all unique date from line 18
    print('c2',d) #print the current date running for the list

    res2 = list(col2.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
    print("lenght for" ,'d', len(res2))
    c2oltpon0tx=[]
    c2oltpon0rx=[]
    for b in range(len(res2)):
        df = pd.DataFrame(res2[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        c2oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
        c2oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array

        sumc2rx+=c2oltpon0rx[b]
        sumc2tx+=c2oltpon0tx[b]



    sum_all_c2rx.append(sumc2rx)
    sum_all_c2tx.append(sumc2tx)

    sumc2tx=0
    sumc2rx=0

    d2.append(d) # append date into empty date array from line 20


#Variable temporary for collection three
sumc3rx=0
sumc3tx=0
sum_all_c3rx=[]
sum_all_c3tx=[]

#Collection three retrieve data
for d in dates3: #loop over all unique date from line 18
    print('c3',d) #print the current date running for the list

    res3 = list(col3.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
    print("lenght for" ,'d', len(res3))
    c3oltpon0tx=[]
    c3oltpon0rx=[]
    for b in range(len(res3)):
        df = pd.DataFrame(res3[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        c3oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
        c3oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array

        sumc3rx+=c3oltpon0rx[b]
        sumc3tx+=c3oltpon0tx[b]


    sum_all_c3rx.append(sumc3rx)
    sum_all_c3tx.append(sumc3tx)

    sumc3tx=0
    sumc3rx=0

    d3.append(d) # append date into empty date array from line 20







# create dataframe into excel 

dsum = pd.DataFrame()
dsum['dates'] = d1
dsum['total rx for oltnni'] =sumoltnni
dsum['totoal tx for oltnni'] =sumoltnnitx
dsum['total rx for oltpon'] = sumoltpon
dsum['total tx for oltpon'] = sumoltpontx
dsum.to_excel('sumrx6.xlsx',index = False)

dsumc2= pd.DataFrame()
dsumc2['dates'] = d2
dsumc2['total rx for oltpon0']= sum_all_c2rx
dsumc2['total tx for oltpon0'] =sum_all_c2tx
dsumc2.to_excel('Sum_c2.xlsx', index=False)

dsumc3= pd.DataFrame()
dsumc3['dates'] = d3
dsumc3['total rx for oltpon0']= sum_all_c3rx
dsumc3['total tx for oltpon0'] =sum_all_c3tx
dsumc3.to_excel('Sum_c3.xlsx', index=False)

