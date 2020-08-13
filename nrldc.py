#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 19:43:20 2020

@author: image
"""


import xml.etree.ElementTree as etree
import re
import pandas as pd
import urllib.request
from datetime import date

date_sg = int(date.today().strftime("%d"))
link = 'http://wbes.nrldc.in/xml/'


def extract_filename(date_sg):
    '''

    Parameters
    ----------
    date_sg : int
        day of the date for which file needs to be downloaded 
        from nrldc website.

    Returns
    -------
    filename : str
        filename of the file matched.

    '''
    
    x = pd.read_html(link)
    y = 0
    flag = 0
    try:
        while (not y == date_sg):
            filename = x[0].iat[flag,0]
            y = int(re.findall('[0-9]+',filename)[1])
            flag+=1
        return filename
    except:
        print('xml file not found on nrldc page')
        return 0

def download():
    filename = extract_filename(date_sg)
    if filename == 0:
        print('filename not found')
    else:
        try:
            url = link+filename
            print('Beginning file download with urllib2...')
            h = urllib.request.urlretrieve(url,filename)
            if (h[0]==filename):
                print('File downloaded')
        except:
            print('file not downloaded')
    return filename

filename = download()
var = etree.parse(filename)

fullSchedule = var.findall('FullScheduleList/FullSchedule')
dclist = var.findall('lstDeclaration/Declaration')

revnumber = [var.find('RevisionNo').text]
revtime = var.find('createdOn').text.split('T')
revision = pd.DataFrame({'revnumber':revnumber,
                         'revdate':revtime[0],
                         'revtime':revtime[1]}).transpose()

class station(object):
    
    def __init__(self,station):
        self.station = station
        
    def text_to_float(self,x):
        '''x is a list
        '''
        return [round(float(each),2) for each in x] 

    def extract_sg(self):
        term = 'ScheduleAmount'
        buyer = []
        schedule= []
        for i in fullSchedule: 
            if i.find('Seller/Acronym').text == self.station:                
                try:
                    sg = self.text_to_float(i.find(term).text.split(","))
                    schedule.append(sg)
                    buyer.append(i.find('Buyer/Acronym').text)    
                except:
                    return pd.DataFrame(schedule, index=buyer)
        return pd.DataFrame(schedule, index=buyer)
    
    def extract_dc(self):       
        
        term = ['DeclaredAmount',
                'OffBarDC',
                'DeclarationOnBar']
        dc = []
        for i in dclist: 
            if i.find('Seller/Acronym').text == self.station:
                try:
                    # dc_date = i.find('DeclaredForDate').text.split('T')
                    # dc.append(dc_date)
                    x = i.find('DeclaredAmount').text.split(",")
                    totaldc = self.text_to_float(x)
                    dc.append(totaldc)
                    y = i.find('DeclarationOnBar').text.split(",")
                    onbardc = self.text_to_float(y)
                    offbardc = [p-q for p,q in zip(totaldc,onbardc)]
                    dc.append(offbardc)
                    dc.append(onbardc)
                except:
                    return pd.DataFrame(dc, index=term)
        return pd.DataFrame(dc, index=term)
    
    def get_dc_sg(self):
        dc = self.extract_dc()
        sg = round(self.extract_sg().sum(),2)
        sg.name = 'Schedule'
        return dc.append(sg)
                    
stationlist = ['DADRI_GF','DADRI_RF','DADRI_LF','Total']  

  
gf = station('DADRI_GF')        
rf = station('DADRI_RF')
lf = station('DADRI_LF') 
# solar = station('DADRI_SOLAR') 


x = gf.get_dc_sg()
y = rf.get_dc_sg()
z = lf.get_dc_sg()
# s = solar.get_dc_sg()


total = round((x.tail(3)+y.tail(3)+z.tail(3)),2)
total = total.append(revision)
dgps = pd.concat([x,y,z,total],keys = stationlist).transpose()
print(dgps)

dgps.to_csv('dgps.csv')




