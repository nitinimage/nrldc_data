#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 07:45:04 2020

@author: image
"""
from nrldc import * 

try:
    gf = station('DADRI_GF')        
    rf = station('DADRI_RF')
    crf = station('DADRI_CRF')
    lf = station('DADRI_LF') 
    
    g = gf.get_dc_sg()
    r = rf.get_dc_sg()
    c = crf.get_dc_sg()
    l = lf.get_dc_sg() 
    total = g+r+c+l
    blocktime = block_time()
 
    
    #dataframe v1
    header = ['Block_time','DGPS_Total',
                   'DADRI_GF','DADRI_CRF',
                   'DADRI_RF','DADRI_LF']  
    dgps = pd.concat([blocktime,total,g,c,r,l],
                    keys = header).transpose()
    
    dgps = addfooter(dgps)
    display(dgps, 'dgps')
 
    
    
    #dataframe v2
    keylist = ['GF', 'CRF', 'RF', 'LF', 'Total']
     
    
    OnBarDC = pd.concat([pd.DataFrame(i.loc["OnBar DC"]) 
                         for i in [g,c,r,l,total]],
                        axis=1, keys = keylist)
    OnBarDC.columns = keylist
    
    Schedule = pd.concat([pd.DataFrame(i.loc["SG"]) 
                         for i in [g,c,r,l,total]],
                        axis=1, keys = keylist)
    Schedule.columns = keylist 
    
    OffBarDC = pd.concat([pd.DataFrame(i.loc["OffBar DC"]) 
                         for i in [g,c,r,l,total]],
                        axis=1, keys = keylist)
    OffBarDC.columns = keylist
    
    Total_DC = pd.concat([pd.DataFrame(i.loc["Total DC"]) 
                         for i in [g,c,r,l,total]],
                        axis=1, keys = keylist)
    Total_DC.columns = keylist
    
    
    header2 = ['Block_time','OnBar DC', 'Schedule Gen', 
               'OffBar DC', 'Total DC']
    dgps2 = pd.concat([blocktime.transpose(),OnBarDC,Schedule,OffBarDC,Total_DC],
                    axis=1, keys = header2)
    dgps2 = dgps2.transpose().transpose()
    dgps2 = addfooter(dgps2)
   
    display(dgps2, 'dgps2')
    
except:
    print('something wrong...Try again')
