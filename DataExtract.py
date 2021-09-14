#!/usr/bin/env python
# coding: utf-8

# In[6]:



import requests
import json

class Extract:
    
    def __init__(self):
        self.api = self.data_sources['data_sources']['api']
    
    
    def getAPISData(self, api_name):
        api_url = self.api[api_name]
        response = requests.get(api_url)
        return response.json()


# In[ ]:




