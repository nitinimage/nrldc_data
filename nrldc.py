#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 19:43:20 2020

@author: image

This script gives you a csv file with the latest nrldc dc sg data
"""

import xml.etree.ElementTree as etree
import re
import pandas as pd
import urllib.request
from datetime import date
from tqdm import tqdm
import time

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

def download_url(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

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
    returns filename of the xml file matched on nrldc website.
    returns 'filename not found' if xml file not found
    '''
    try:
        link = 'http://wbes.nrldc.in/xml/'
        print('fetching latest filename from wbes.nrldc.in/xml/\n')
        x = pd.read_html(link)
        y = 0
        flag = 0
        while (not y == date_sg):
            filename = x[0].iat[flag,0]
            y = int(re.findall('[0-9]+',filename)[1])
            flag+=1
        return filename
    except:
        print('nrldc.in not accessible')
        return 'filename not found'

def download(filename): 
    '''
    parameter
        filename of the file to be downloaded
    downloads the file 
    save it as rawdata.xml
    parse it
    
    returns parsed file or 0 
    '''
    
    link = 'http://wbes.nrldc.in/xml/'
    if filename == 'filename not found':
        print('filename not found')
        return 0
    else:
        try:
            url = link+filename
            print('Beginning file download with urllib2...')
            download_url(url,'rawdata.xml')
            parsed_data = etree.parse('rawdata.xml')
            print('File downloaded')
            return parsed_data
        except:
            print('file not downloaded or parsed')
            return 0

def check_parse(date_sg):
    '''
    checks if file exists otherwise call download

    Parameters
    ----------
    filename : str
        filename of the latest rev on nrldc website.

    Returns
    -------
    parsed file or 0 if not downloaded

    '''
    import os.path
    filename = extract_filename(date_sg)
    
    #if internet not working try to parse the local file if it exists
    if filename == 'filename not found':
        print('internet not working')
        if os.path.exists('rawdata.xml'):
            print('attempting parsing with existing file')
            print('data may be outdated')
            parsed_data = etree.parse('rawdata.xml')
            return parsed_data
        else:
            print('local file not found')
            print('nothing to parse')
            return 0
    #if internet is working and local file doesn't exists, download it
    elif not os.path.exists('rawdata.xml'):
        print('file does not exists')
        return download(filename)
    else:
    #if internet is working and local file exists,compare rev no.
        print('local xml file exists..attempting to parse and compare')
        try:
            parsed_data = etree.parse('rawdata.xml')          
            revnumber = parsed_data.find('RevisionNo').text
            latest_rev = re.findall('[0-9]+',filename)[0]
            flag = (revnumber == latest_rev)
            print('\nlatest_rev is: ', latest_rev)
            print('existing rev is: ', revnumber,'\n')
        except:
            print('parse error')
            flag = 0 #needs downloading
        #download if there is a new revision or parse error
        if (flag == 0):
            print('downloading latest file')
            return download(filename)
        return parsed_data
 
def block_time():
    '''
    Returns
    -------
    dataframe
        block Start and End times.

    '''
    # blocknumber=[]
    Start = []
    End = []
    for i in range(96):
        # blocknumber.append(i+1)
        if not i%4:
            Start.append(str(i//4).zfill(2)+':00')
        else:
            Start.append(str(i//4).zfill(2)+':'+str(15*(i%4)))
    End = Start[1:]+[Start[0]]
    
    # n = pd.DataFrame(blocknumber,columns=['block_number'])
    x = pd.DataFrame(Start,columns=['Start'])
    y = pd.DataFrame(End,columns=['End'])
    return pd.concat([x,y],axis=1).transpose()

def get_revision():
    revnumber = parsed_data.find('RevisionNo').text
    revtime = parsed_data.find('createdOn').text.split('T')
    # revision = pd.DataFrame({'revnumber':revnumber,
    #                         'revdate':revtime[0],
    #                         'revtime':revtime[1]}).transpose()
    #revtime.append(revnumber)
    revdate = revtime[0].split('-')
    issuedon = f"{revdate[2]}-{revdate[1]}-{revdate[0]}  {revtime[1]}"
    return [revnumber,issuedon]

def display(df, output_filename):
    '''takes dataframe as input and displays as html'''
    
    rev = get_revision()

    print(df)
    df.to_csv(f"{output_filename}.csv",na_rep='')
    # df.to_html(f"{output_filename}.html",na_rep='',classes='mystyle')
    
    
    pd.set_option('colheader_justify', 'center')   # FOR TABLE <th>
    
    html_string = '''
        <html>
          <head>
              <title>DGPS DC SG </title>
          </head>
          <script src="df_script.js"></script>
          <link rel="stylesheet" type="text/css" href="df_style.css"/>
          <body>
            <div> <h1 id=rev >Revision no. {revno} &nbsp;&nbsp;&nbsp; 
            Issued on: {revtime} 
            </h1></div>
            {table}
          </body>
        </html>
        '''
    
    # OUTPUT AN HTML FILE
    with open(f"{output_filename}.html", 'w') as f:
        f.write(html_string.format
                (table=df.to_html(na_rep='', classes='mystyle'),
                                   revno=rev[0],
                                   revtime=rev[1]))


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
        fullSchedule = parsed_data.findall('FullScheduleList/FullSchedule')
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
        term = ['Total DC',
                'OffBar DC',
                'OnBar DC']
        dc = []
        dclist = parsed_data.findall('lstDeclaration/Declaration')
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
        sg.name = 'SG'
        dc_sg = dc.append(sg)
        return dc_sg

#set date as today or any date                  
date_sg = int(date.today().strftime("%d"))
parsed_data = check_parse(date_sg)

def main():
    # while True:
        try:
            gf = station('DADRI_GF')        
            rf = station('DADRI_RF')
            crf = station('DADRI_CRF')
            lf = station('DADRI_LF') 
            
            g = gf.get_dc_sg()
            r = rf.get_dc_sg()
            c = crf.get_dc_sg()
            l = lf.get_dc_sg()
    
            total = round((g+r+c+l),2)
            blocktime = block_time()
            
            stationlist = ['Block_time','DGPS_Total',
                           'DADRI_GF','DADRI_CRF',
                           'DADRI_RF','DADRI_LF'
                           ]  
            
            # revision = get_revision()
            
            dgps = pd.concat([blocktime,total,g,c,r,l],
                            keys = stationlist).transpose()
            dgps.index = list(range(1,97))
            
            # display output
            display(dgps, 'dgps')

        except:
            print('something wrong...Try again')
        # time.sleep(10)
        # date_sg = int(date.today().strftime("%d"))
        # global parsed_data 
        # parsed_data = check_parse(date_sg)
        
if __name__ == "__main__":
    main()


