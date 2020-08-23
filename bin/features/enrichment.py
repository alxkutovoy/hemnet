#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import re
import uuid
from os import path
from sklearn.model_selection import train_test_split
from pathlib import Path

# In[2]:


import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import seaborn as sns;

sns.set()

# In[3]:


pd.options.mode.chained_assignment = None

# In[4]:


processed_data_path = './data/processed'
tokens_municipality = ['Botkyrka', 'Danderyd', 'Ekerö',
                       'Haninge', 'Huddinge', 'Järfälla',
                       'Lidingö', 'Nacka', 'Norrtälje',
                       'Nykvarn', 'Nynäshamn', 'Salem',
                       'Sigtuna', 'Sollentuna', 'Solna',
                       'Stockholm', 'Sundbyberg', 'Södertälje',
                       'Tyresö', 'Täby', 'Upplands-Bro',
                       'Upplands Väsby', 'Vallentuna',
                       'Vaxholm', 'Värmdö', 'Österåker']
tokens_proptype = ['Lägenhet']
tokens_owntype = ['Bostadsrättslägenhet']

# In[5]:


data = pd.read_pickle('./data/raw/hemnet_v3.pkl')


# In[6]:


def filter_municipality(dataset, tokens):
    """
    Return list of relevant municipalities
    param: dataset, type: pandas df, desc: raw dataframe
    param: tokens, type: list, desc: list of strings, tokens of relevant municipalities
    return: matches, type: list, desc: list of strings, relevant municipalities
    """
    distinct_municipalities = data.municipality.unique().tolist()
    matches = []
    for municipality in distinct_municipalities:
        for token in tokens:
            if token in municipality:
                matches.append(municipality)
    return matches


def fix_buildyear(raw):
    current_year = pd.Timestamp.now().year
    min_year = 1500
    str_raw = str(raw)

    if len(raw) == 4 and int(raw) <= current_year and int(raw) >= min_year:
        return int(raw)
    if len(raw) > 4:
        if int(raw[0:4]) <= current_year and int(raw[0:4]) >= min_year:
            return int(raw[0:4])
        else:
            return None
    else:
        return None


def add_buildage(build_year):
    current_year = pd.Timestamp.now().year
    return current_year - build_year


# In[ ]:


# In[8]:


def data_filter(dataset, tokens_municipality=tokens_municipality, tokens_proptype=tokens_proptype,
                tokens_owntype=tokens_owntype):
    # Filter relevant transactions
    filtered_municipality = filter_municipality(dataset, tokens_municipality)
    dataset = dataset[data['municipality'].isin(filtered_municipality)
                      & dataset['proptype'].isin(tokens_proptype)
                      & dataset['owntype'].isin(tokens_owntype)]
    # Transform
    dataset['buildyear'] = dataset['buildyear'].apply(lambda x: fix_buildyear(x))
    dataset['brokerfullname'] = dataset['brokerfullname'].apply(lambda x: ' '.join(x))
    # Remove NaN values
    dataset = dataset.replace('N/A', float('NaN'))
    dataset = dataset.dropna(
        subset=['ts', 'address', 'coordinates', 'startprice', 'endprice', 'area', 'utils', 'rum', 'buildyear'])
    # Reset index
    dataset = dataset.reset_index(drop=True)
    # Drop redundant
    dataset = dataset.drop(columns=['proptype', 'owntype', 'brokerphone'])
    return dataset


# In[9]:


def add_streetbuilding(raw):
    street = add_street(raw)
    building = add_build(raw)
    street_building = street + ' ' + building
    return street_building


def add_street(raw):
    list_address = raw.split(',')
    if len(list_address) > 1:
        raw = list_address[0]
    if any(char.isdigit() for char in raw):
        return re.match('(.+?(?=[(0-9)]))', raw)[0].strip().lower()
    else:
        return raw.strip().lower()


def add_build(raw):
    list_address = raw.split(',')
    if len(list_address) > 1:
        raw = list_address[0]
    return re.sub("[^0-9]", "", raw)


def add_floor(raw):
    list_address = raw.split(',')
    if len(list_address) > 1:
        raw = list_address[1]
        floor = re.sub("[^0-9]", '', raw)
        if floor:
            return floor
    else:
        return None


# In[ ]:


# In[275]:


test_addr = 'Wilhelm Kåges gata 1,  nb'
# re.sub("[^0-9]", "", test_addr)
any(char.isdigit() for char in test_addr)


# In[10]:


def feature_engineering(dataset):
    # Add
    dataset['id'] = dataset['ts'].apply(lambda x: str(uuid.uuid4()))
    dataset['buildage'] = dataset['buildyear'].apply(lambda x: add_buildage(x))
    dataset['coordinates_lat'] = dataset['coordinates'].apply(lambda x: x[0])
    dataset['coordinates_lon'] = dataset['coordinates'].apply(lambda x: x[1])
    dataset['address_streetbuilding'] = dataset['address'].apply(lambda x: add_streetbuilding(x))
    dataset['address_street'] = dataset['address'].apply(lambda x: add_street(x))
    dataset['address_building'] = dataset['address'].apply(lambda x: add_build(x))
    dataset['address_floor'] = dataset['address'].apply(lambda x: add_floor(x))
    # Magic
    kmeans = KMeans(n_clusters=30, init='k-means++')
    kmeans.fit(dataset[['coordinates_lat', 'coordinates_lon']])  # Compute k-means clustering
    dataset['geo_cluster'] = kmeans.fit_predict(dataset[['coordinates_lat', 'coordinates_lon']])
    # Drop redundant
    dataset = dataset.drop(columns=['address', 'coordinates'])
    return dataset


# In[ ]:


# In[ ]:


# In[11]:


data = pd.read_pickle('./data/raw/hemnet_v3.pkl')
data = data_filter(data)
data = feature_engineering(data)
data.fillna(value=pd.np.nan, inplace=True)
data.head()

# In[39]:


from scipy.stats import norm
from scipy import stats
import warnings

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import Imputer
from xgboost import XGBRegressor

get_ipython().run_line_magic('matplotlib', 'inline')
warnings.filterwarnings('ignore')

# In[40]:


list(data.columns)

# In[43]:


dataset = data.drop(['ts', 'startprice', 'buildyear', 'id', 'coordinates_lat', 'coordinates_lon', 'address_building'],
                    axis=1)

# In[44]:


dataset

# In[49]:


test = ['example-1', 2]

# In[55]:


print('The page number', 100, 'is protected by ReCaptcha. Calling AntiCaptcha...',
      'Attempt:', str(4) + '.')

# In[64]:


import sys


def test_x(i):
    print('test', sys._getframe().f_code.co_name, 'test2')
    print(i)


# In[72]:


from datetime import datetime

# In[73]:


str(datetime.now().replace(microsecond=0))

# In[ ]:




