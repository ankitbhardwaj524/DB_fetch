# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 11:06:19 2025

@author: Ankit Bhardwaj
"""

import psycopg2 as pc
import pandas as pd
import csv
from openpyxl import Workbook

def get_db_credentials():
    user=input('Enter your DB user id : ')
    password=input('Enter your DB password : ')
    return user,password

def get_user_file():
    file=input('Enter address of your file : ')
    return file

def case_reason():
    user_reason=input('Enter case no : ')
    return "Case " + user_reason

query='''
Select macid, 
min(datetimetstamp) as start,
max(datetimestamp) as end
from <table name>
where macid in {mac_id}
group by macid;
'''

def fetch_dates(mac_id,conn_info):
    try:
        mac_id_str = ', '.join([f"'{mac}'" for mac in mac_id])
        conn = pc.connect(**conn_info)
        query_with_mac = query.format(mac_id=mac_id_str)
        cursor=conn.cursor()
        cursor.execute(query_with_mac)
        result=cursor.fetchone()
        conn.close()
        
        if result:
            return result[1],result[2]
        else:
            return None,None
    except Exception as e:
        print(f"Error querying Database for mac {mac_id} : {e}")
        return None,None
    
def process_file(input_file,conn_info):
    df=pd.read_csv(input_file)
    good_df= pd.DataFrame(columns=["Original macid","New macid","Start Merge","End Merge","reason"])
    overlapping_df=pd.DataFrame(columns=["Original macid","New macid","Overlapping Start time","Overlapping End time"])
    error_df=pd.DataFrame(columns=["Oringinal macid","New macid","Error Reason"])
    
    for _, row in df.itterrows():
        original_mac_id=row['original mac id']
        new_mac_id=row['new mac id']
        
        original_start,original_end=fetch_dates(original_mac_id,conn_info)
        new_start,new_end=fetch_dates(new_mac_id, conn_info)
        
        if not (original_start or original_end) and not (new_start or new_end):
            error_df=error_df.append({
                "Original macid" : original_mac_id,
                "New macid" : new_mac_id,
                "Error Reason" : "Both mac ID is not in DB"
                }, ignore_index=True)
                
        continue
        
        if not original_start or not original_end:
            error_df=error_df.append({
                "Original macid" : original_mac_id,
                "New macid" : new_mac_id,
                "Error Reason" : "Old mac ID is not in DB"
                }, ignore_index=True)
                
        continue
        
        if not new_start or not new_end:
            error_df=error_df.append({
                "Original macid" : original_mac_id,
                "New macid" : new_mac_id,
                "Error Reason" : "New mac ID is not in DB"
                }, ignore_index=True)
                
        continue
    
        if new_start > original_end:
            good_df=good_df.append({
                "Original macid" : original_mac_id,
                "New macid" : new_mac_id,
                "Start Merge" : original_start,
                "End Merge" : original_end,
                "reason" : case_reason()
                }, ignore_index=True)
                
        else:
            overlapping_df=overlapping_df.append({
                "Original macid" : original_mac_id,
                "New macid" : new_mac_id,
                "Overlapping Start time" : new_start,
                "Overlapping End time" : original_end
                }, ignore_index=True)
            
    
    with pd.ExcelWriter('Good.xlsx') as writer:
        good_df.to_excel(writer, sheet_name='Good', index=False)
    with pd.ExcelWriter('Overlapping.xlsx') as writer:
        overlapping_df.to_excel(writer,sheet_name='Overlapping',index=False)
    with pd.ExcelWriter('Error.xlsx') as writer:
        error_df.to_excel(writer,sheet_name='Error', index=False)
        
        
    print("Excel files have been created: Good.xlsx, Overlapping.xlsx, Errors.xlsx")
    
    
def main():
    
    db_user,db_pass=get_db_credentials()
    input_file=get_user_file()
    
    conn_info={
        'host' : 'your host name',
        'port' : 'port no',
        'user' : db_user,
        'password' : db_pass,
        'database' : 'your database name'
        }
    
    case_reason()
    
    process_file(input_file, conn_info)
    
if __name__ == "__main__":
    main()
 
    