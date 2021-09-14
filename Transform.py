#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from DataExtract import Extract
from DataLoad import MongoDB
import urllib
import pandas as pd

class Transformation:
    
    def __init__(self, dataSource, dataSet):
        print('Inside')
        extractObj = Extract()
        if dataSource == 'api':
            self.data = extractObj.getAPISData(dataSet)
            funcName = dataSource+dataSet
            getattr(self, funcName)()
        elif dataSource == 'csv':
            self.data = extractObj.getCSVData(dataSet)
            funcName = dataSource+dataSet
            getattr(self, funcName)()
        else:
            self.data = extractObj.databases(dataSet)
   
        
        
    def apiPollution(self):
        air_data = self.data['results']
        # Converting nested data into linear structure
        air_list = []
        for data in air_data:
            for measurement in data['measurements']:
                air_dict = {}
                air_dict['location'] = data['location']
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['lastUpdated'] = measurement['lastUpdated']
                air_dict['unit'] = measurement['unit']
                air_dict['sourceName'] = measurement['sourceName']
                air_list.append(air_dict)
        # Convert list of dict into pandas df
        df = pd.DataFrame(air_list, columns=air_dict.keys())
        # connection to mongo db
        mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('poln@recover'), '104.155.187.175', 'Pollution_Data')
        # Insert Data into MongoDB
        mongoDB_obj.insert_into_db(df, 'Air_Quality_India')


    
if __name__ == '__main__':
    dataSource = input("Please Select the DataSource i.e 'API'/'CSV'/'Database': ").lower()
    print(dataSource)
    dataSet = input('Please select the Data set for Transformation: ').title()
    print(dataSet)
    trans_obj = Transformation(dataSource, dataSet)


# In[ ]:




