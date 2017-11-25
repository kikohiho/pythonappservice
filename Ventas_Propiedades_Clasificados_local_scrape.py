# -*- coding: utf-8 -*-
"""
Created on Fri Oct  6 19:47:05 2017

@author: Hiram
"""
#Packages
from lxml import html
import pandas as pd
import requests
import re
from collections import OrderedDict
from itertools import repeat
from datetime import datetime
import time

start = time.time()


import math

#This File will Scrape Clasificados Online for Housing Data

#User ID & URL of First Page

headers= {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36"}
url='http://www.clasificadosonline.com/UDREListing.asp?RESPueblos=%25&Category=%25&LowPrice=0&HighPrice=999999999&Bedrooms=%25&Area=&Repo=Search+Busqueda+con+Repose%EDdas'

def getsoup(url):
    page=requests.get(url, headers=headers)
    tree = html.fromstring(page.content)
    return tree
    
def scrape_page(tree):
    
    #Number of Pages and Enrtries to Scrape
    entries = tree.xpath('//font[@class="Tahoma16BrownNound"]/text()')
    entries = ''.join(entries[0].split())
    entries = entries.replace("al", " ")
    entries = entries.replace("de", " ")
    entries = entries.split()    
    entries = list(map(int, entries))
    entries = range(entries[1]-entries[0])
    
    #Urba/Barrio y Municipio
    urb_muni = tree.xpath('//a[@class="Tahoma13nounder"]/text()')
    urba=urb_muni[0:][::2]
    urba=[str(i) for i in urba]
    muni=urb_muni[1:][::2]
    muni=[str(i) for i in muni]
    
    # Precio, Tipo, Cuartos, Baños, Repo
    stats = tree.xpath('//span[@class="Tah14nounder"]/text()')
    stats=[str(i) for i in stats]
    
    #Precios
    price=stats[0:][::2]
    price=[str(i) for i in price]
    prices=[]
    for i in price:
        p=i.split()

        if p:
            p=p[0].rstrip(",")
            #p = re.findall(r'(^[^\s]+).*?(\.\w+)$', i)  #Old Method, New seems 
            #p = ','.join(p)                             # to be working better
            prices.append(p)
    
    #Tipo, Cuartos, Baños
    types=[]
    rooms=[]
    bathrooms=[]
    
    tcbr=stats[1:][::2]
    tcbr=[str(i) for i in tcbr]
    for i in tcbr:
        t_cb=i.split(',')
        types.append(t_cb[0].strip())
        cb=t_cb[1].split('-')
        rooms.append(cb[0])
        bathrooms.append(cb[1])
        
    #ID
    ids = tree.xpath('//a["Tahoma15blacknound"]/@href')
    ids = [str(i) for i in ids]
    ID = [x for x in ids if 'ID=' in x]
    ids=[]
    for i in ID:
        x = next(re.finditer(r'\d+$', i)).group(0)
        ids.append(x)
    ID = list(OrderedDict(zip(ids, repeat(None))))
    ids=[x for x in ID if len(x)>=7][5:]

    #Repo 
    rr = tree.xpath('//span[@class="Tahoma12Rojonounder"]')
    repo_dummy=[]
    repo=[]
    for elem in rr:
        repo_dummy.append(elem.text_content())
    for elem in repo_dummy:
        if elem=='Repo':
            repo.append(1)
        else:
            repo.append(0)
            
    return  urba, muni, prices, types, rooms, bathrooms, ids, repo
        
def to_datframe():
    headers=['Barrio','Municipio','Precio','Tipo','Cuartos','Baños','ID','Repo']
    data=pd.DataFrame(scrapeddata)
    data=data.transpose()
    data.columns = headers
    data['DateParsed']=str(datetime.now()) #Current Time
    
    return data

def nextpage():
    els = tree.xpath('//a')
    for el in els:
        href = el.xpath("//@href")
        href = [str(i) for i in href]
        for h in href:
            if "/UDREListing.asp?RESPueblos=%25&Category=%25&Bedrooms=%25&LowPrice=0&HighPrice=999999999&Area=&Repo=Search+Busqueda+con+Repose%EDdas&offset=" in h:
                nextlink=h
    global url
    url="http://www.clasificadosonline.com"+nextlink



#Execution Loop
new_data=[]

tree=getsoup(url)
entries = tree.xpath('//font[@class="Tahoma16BrownNound"]/text()')
entries = ''.join(entries[0].split())
entries = entries.replace("al", " ")
entries = entries.replace("de", " ")
entries = entries.split()
totalentries=entries[2]

count=0
# for run in range((int(math.ceil(int(totalentries)/30)))):
#
#     count=count+1
#     print(count)
#
#     tree=getsoup(url)
#     scrapeddata=list(scrape_page(tree))
#
#     data=to_datframe()
#     new_data.append(data)
#
#     nextpage()


for run in range(2):
    count = count + 1
    print(count)

    tree = getsoup(url)
    scrapeddata = list(scrape_page(tree))

    data = to_datframe()

    new_data.append(data)

    nextpage()

#Prep & Export 
#print(new_data)
new_data = pd.concat(new_data)

# for record in new_data.iterrows():
#     print(record)
# new_data = new_data.reset_index(drop=True)
#
# old_data= pd.read_csv('Scraped_Ventas.csv', index_col=False, header=0,encoding = "ISO-8859-1")
# old_data = old_data.sort_values('ID')
#
# last_ID = int(old_data.tail(1)['ID'])
#
# new_data['ID']=new_data['ID'].astype(int)
# new_data=new_data[new_data["ID"] < last_ID]

# new_data = pd.concat([old_data,new_data])
new_data.to_csv('Scraped_Ventas.csv', index=False)

end = time.time()
print("Time Ellapsed: " + (end - start))

#plain_muni_names=["Adjuntas","Aguada","Aguadilla","Aguas Buenas","Aibonito","Anasco",
#"Arecibo","Arroyo","Barceloneta","Barranquitas","Bayamon","Cabo Rojo","Caguas",
#"Camuy","Canovanas","Carolina","Catano","Cayey","Ceiba","Ciales","Cidra","Coamo",
#"Comerio","Corozal","Culebra","Dorado","Fajardo","Florida","Guanica","Guayama",
#"Guayanilla","Guaynabo","Gurabo","Hatillo","Hormigueros","Humacao","Isabela",
#"Jayuya","Juana Diaz","Juncos","Lajas","Lares","Las Marias","Las Piedras","Loiza",
#"Luquillo","Manati","Maricao","Maunabo","Mayaguez","Moca","Morovis","Naguabo",
#"Naranjito","Orocovis","Patillas","Penuelas","Ponce","Quebradillas","Rincon",
#"Rio Grande","Sabana Grande","Salinas","San German","San Juan","San Lorenzo",
#"San Sebastian","Santa Isabel","Toa Alta","Toa Baja","Trujillo Alto","Utuado",
#"Vega Alta","Vega Baja","Vieques","Villalba","Yabucoa","Yauco"]


