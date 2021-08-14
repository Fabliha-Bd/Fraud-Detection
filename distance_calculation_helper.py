#!/usr/bin/env python
# coding: utf-8

# In[8]:


import os
import pandas as pd
from pandas import (
    merge,
    read_csv,
    DataFrame,
    to_datetime,
    to_numeric,
)

from os.path import join
from bson.tz_util import FixedOffset
import numpy as np
import pymysql
from math import acos, sin, cos, atan2, sqrt
import os, sys
import datetime
from datetime import datetime, timedelta
import shutil


import ast
import warnings
warnings.filterwarnings("ignore")

from base64converter import base64_converter


# In[6]:


pi = acos(-1.0)
def to_radian(deg):
    """
    to convert degree to radian
    parameters: 
    
    deg: numeric 
    """
    return (deg*pi)/180.0

def get_haversine_distance(coord1, coord2):
    """
    Get Haversine distnace between two coordinates
    parameters: 
    
    coord1: numeric 
    coord2: numeric 
    """
#     print(coord1)
#     print(coord2)
    lat1 = coord1[0]
    long1 = coord1[1]
    lat2 = coord2[0]
    long2 = coord2[1]
    R = 6371000
    dlat = to_radian(lat2-lat1)
    dlong = to_radian(long2-long1)
    lat1 = to_radian(lat1)
    long1 = to_radian(long1)
    lat2 = to_radian(lat2)
    long2 = to_radian(long2)
    a = (sin(dlat/2.0)**2.0)+cos(lat1)*cos(lat2)*(sin(dlong/2.0)**2)
    c = 2*atan2(sqrt(a), sqrt(1.0-a))
    return R*c

def tag_patterned(x,y):
    """
    for tagging deliveries with foorprint patterns where large number of coordinates fall in a line

    inputs: lists of coordinates
    """
    coords_ap = ast.literal_eval(x)
    coords_pd = ast.literal_eval(y)
    coords_all = coords_ap+coords_pd
    
    lnglst = []
    for j in coords_all:
        lnglst.append(j[1])
         
    dummy = pd.DataFrame({"coord_y": lnglst})
    dummyframe = dummy.coord_y.value_counts().to_frame().reset_index()
    if len(dummyframe[dummyframe.coord_y>99])>=1:
        return 1
    else:
        return 0

def delivery_drop_distance(del_lat,del_lng,delivery_latlng):
    """
    calculate distance between user dropoff pin and where the driver tapped 'delivered' in the app
    """
    del_latlng = [del_lat, del_lng]
    coords_pd = ast.literal_eval(delivery_latlng)
    if (len(coords_pd)==0):
        return 0
    else:
        dropoff_coord = coords_pd[len(coords_pd)-1]
        distance = get_haversine_distance(del_latlng, dropoff_coord)
        return distance
def pickup_restaurant_distance(res_lat, res_lng, restaurant_latlng):
    """
    calculate distance between restaurant location and where the driver tapped 'picked up' in the app
    """
    res_latlng = [res_lat, res_lng]
    coords_ap = ast.literal_eval(restaurant_latlng)
    if (len(coords_ap)==0):
        return 0
    else:
        pickup_coord = coords_ap[len(coords_ap)-1]
        distance = get_haversine_distance(res_latlng, pickup_coord)
        return distance

def pickedUp_time(deliveryStatelist):
    for state in deliveryStatelist:
        if state['DeliveryState'] == 'OrderPickedUp':
            return state['TransitionTime']
    return 'NaT'


def mysql_rides_to_dataframe(query):
    """
    A utility function to connect to database through pymysql connector, query and return the result
    parameters: 
    
    query: string 
    """
    host = "hostname"
    port= 3306
    dbname= "db name"
    user="user name"
    password="password"
    conn = pymysql.connect(host, user=user,port=port,password=password, db=dbname)
    cursor = conn.cursor()
    q = "use database_name"
    cursor.execute(q)
    cursor.close()
    df = pd.read_sql(query,conn)
    return df

def json_concat_and_conversion(date):
    """
    A function to concat all the footprints' json files in a directory and then convert them using base64_converter
    
    """
    path_to_json = './location-history-'+date+'/'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

    dfs = [] # an empty list to store the data frames
    for file in json_files :
        try:
            data = pd.read_json('./location-history-'+date+'/'+file, lines=True) # read data frame from json file
            dfs.append(data) # append the data frame to the list
        except:
            print('Error in json data')
    print('json append')
    coordinates_file = pd.concat(dfs, ignore_index=True)
    coordinates = base64_converter(coordinates_file)
    print('json converted')
    coordinates= coordinates.rename(
        columns=
        {
            "ProviderId": "providerid",
            "Latitude":"lat",
            "Longitude":"lng",
            "ServiceType":"servicetypeid",        
        }
    )
    coordinates['datetime_formatted'] = pd.DatetimeIndex(coordinates['datetime']).tz_convert(None)
    return coordinates

def remove_footprint_data_directory():
    for number in range(1,8):
        date = datetime.strftime(datetime.now() - timedelta(number), '%d-%m-%Y')
        print(date)
        pwd = os.getcwd()
        directory_path = join(pwd, 'location-history-'+date)
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
            print(directory_path + ' is removed.')
        coord_food_file = datetime.strftime(datetime.now() - timedelta(i), '%d-%m-%Y') +'_coordinate_foods.csv'
        if os.path.exists(coord_food_file):
            os.remove(coord_food_file)
            print(coord_food_file + ' is removed.')
    if os.path.exists(filename):
        os.remove(filename)
        print(filename + ' is removed.')
    if os.path.exists(filename2):
        os.remove(filename2)
        print(filename2 + ' is removed.')


# In[ ]:




