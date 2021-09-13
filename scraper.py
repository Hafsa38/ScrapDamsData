#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime,date, timedelta
import re
import os
import warnings
import tabula
import pandas as pd
warnings.filterwarnings('ignore')
from urllib.error import HTTPError


# In[2]:


base_path = "http://81.192.10.228/wp-content/uploads/"


# In[3]:


def format_date(day, month, year, return_data_path=True):
    '''
    function to change date values to required format
    
    args:
        day: day of the month required (integer)
        
        month: month of the year required (integer)
        
        year: year required (integer)
        
        return_data_path: boolean to return path of data
        
        hyphen_date_path: boolen to return delimiter of date path as hyphen
        
    return:
        date_path: path of data (pdf)
        
        date_val: formatted day, month and year of the required date
        
        date_val0: formatted day, month and year of the required date for previous year
    '''
    
    if len(str(month)) == 1:
        date_path = "{}/0{}/{}_{}_{}.pdf".format(year, month, day, month, year)
    else:
        date_path = "{}/{}/{}_{}_{}.pdf".format(year, month, day, month, year)

        
    date_val = "{}/{}/{}".format(day, month, year)
    date_val_0 = "{}/{}/{}".format(day, month, year-1)
    
    if return_data_path:
        return date_path, date_val, date_val_0
   
    else: return date_val, date_val_0


# In[4]:


def format_barrages(data):
    '''
    function to format data(pdf) and filter barrages
    '''
    
    barrages = {
    'ALWAHDA': 'Al Wahda',
    'IDRISS 1 er': 'Idriss 1er',
    'EL KENSERA': 'El Kensera',
    'OUED EL MAKHAZINE': 'Oued El Makhazine',
    'BIN EL OUIDANE': 'Bin El Ouidane',
    'AHMED AL HANSSALI': 'Al Hanssali',
    'AL MASSIRA': 'Al Massira',
    'HASSAN II': 'Hassan II',
    'MOHAMED V': 'Mohamed V',
    'BARRAGE SUR OUED ZA': 'Oued ZA'
    }
    data = data[data['Name'].isin(barrages.keys())]
    data['Name'].replace(to_replace=barrages, inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    return data


# In[5]:


def pdf_to_csv(date_path, date_val, date_val_0, base_path=base_path):
    '''
    function to download pdf file online
    
    args:
        date_path: portion of the pdf url path that contains the date values
        
        date_val: the date of the required data
        
        date_val0: date of required data, minus a year
        
    return:
        df: pandas dataframe 
    '''
    
    column_names = ['Name',
                    'Normal_capacity',
                    'Reserve_{}'.format(date_val),
                    'Fill_rate_{}'.format(date_val), 
                    'Reserve_{}'.format(date_val_0),
                    'Fill_rate_{}'.format(date_val_0)]
    
    url_path = base_path+date_path
        
    df = tabula.read_pdf(url_path, stream=True, pages='all', multiple_tables=False,
                          pandas_options={'names': column_names, 'skiprows': [0,1,2,3]} )
    
    
    df = format_barrages(df[0])
    return df


# In[6]:


date4_download = (28,11,2018)  # Enter as day, month, year


# In[7]:


df = pdf_to_csv(*format_date(*date4_download))


# In[8]:


df


# In[9]:


def clean_data(data):
    '''
    function to clean our data 
    '''
    
    column_names = data.columns
    missing_values = sum(data.isna().sum())
    
   
    if missing_values <1 :
        pass
    
    else:
        null_data = data[data.isnull().any(axis=1)]
        other_data = data[~data.isnull().any(axis=1)]
        
        expanded_col_3 = null_data[column_names[3]].str.split(" ", expand=True)
        if expanded_col_3.shape[1] > 1:
            null_data[column_names[4]] = expanded_col_3[0]
            null_data[column_names[5]] = expanded_col_3[1]


        expanded_col_2 = null_data[column_names[2]].str.split(" ", expand=True)
        if expanded_col_2.shape[1] > 1:
            null_data[column_names[2]] = expanded_col_2[0]
            null_data[column_names[3]] = expanded_col_2[1]
        
        data = pd.concat([null_data, other_data])
            
        del null_data, other_data
        
    cols_to_format = [col for col in column_names if col not in ['Name']]
    for col in cols_to_format:
        data[col] = data[col].apply(lambda x: re.sub(',', '.', str(x)))

    return data


# In[10]:


df=clean_data(df)


# In[11]:


df


# In[12]:


def format_data(date_val, date_val_0, df=df):
    '''
    function to format our data to a required format
    '''
    
    formatted_df = []
    for index in range(0, df.shape[0]):
        original = pd.DataFrame(df.iloc[index]).T
        original['Date'] = date_val
        original['Fill_rate'] = original["Fill_rate_{}".format(date_val)]
        original['Reserve'] = original["Reserve_{}".format(date_val)]
                
        new = original.copy()
        new['Date'] = date_val_0
        new['Fill_rate'] = new["Fill_rate_{}".format(date_val_0)]
        new['Reserve'] = new["Reserve_{}".format(date_val_0)]
        
        formatted_df.append(pd.concat((original, new)))
                
        del original, new
            
    formatted_df = pd.concat(formatted_df).drop(columns=[col for col in df.columns if col not in 
    ['Name', 'Normal_capacity', 'Fill_rate', 'Reserve', 'Date']]).reset_index(drop=True)
    
    formatted_df = formatted_df[['Name', 'Fill_rate', 'Normal_capacity', 'Reserve', 'Date']]
        
    return formatted_df                                       


# In[13]:


new_df = format_data(*format_date(*date4_download, return_data_path=False))


# In[14]:


new_df


# In[15]:


def datetime_range(start=None, end=None):
    '''
    use function to generate a datetime range
    
    args:
        start: beginning of range (datetime)
        
        end: end of range (datetime)
    '''
    
    span = end - start
    for i in range(span.days + 1):
        yield start + timedelta(days=i)


# In[16]:


dates_4download = list(datetime_range(start=datetime(2020, 7, 7), end=datetime(2021,9,13)))


# In[17]:


def folder_download():
    '''
    function to organize pdf file downloads in monthly folders.
    '''
    download_count = 0
    unable_to_download = []
    if os.path.exists('./dataset'):
        pass
    else:
        os.mkdir('dataset')
    
    for date_4download in dates_4download:
        date_4download = (date_4download.day, date_4download.month, date_4download.year)
        print(date_4download)
        try:
            data = pdf_to_csv(*format_date(*date_4download))
            download_count+=1
        except HTTPError:
            unable_to_download.append(date_4download)
            print('Could not download pdf file because of HTTP connection error')
            continue

        data = clean_data(data)

        new_data = format_data(*format_date(*date_4download, return_data_path=False), df=data)

        save_path = 'dataset/{}-{}/'.format(date_4download[1], date_4download[2])   # month-year
        print('save_path', save_path)
        if os.path.exists(save_path):
            if os.path.exists(os.path.join(save_path, 'repr')):
                data.to_csv(save_path + '/repr/' + '_'.join([str(val) for val in date_4download])+'repr.csv', index=False)
            else:
                os.mkdir(os.path.join(save_path, 'repr'))
                data.to_csv(save_path + '/repr/' + '_'.join([str(val) for val in date_4download])+'repr.csv', index=False)

            if os.path.exists(os.path.join(save_path, 'repr0')):
                new_data.to_csv(save_path + '/repr0/' + '_'.join([str(val) for val in date_4download])+'repr0.csv', index=False)
            else:
                os.mkdir(os.path.join(save_path, 'repr0'))
                new_data.to_csv(save_path + '/repr0/' + '_'.join([str(val) for val in date_4download])+'repr0.csv', index=False)

        else:
            os.mkdir(save_path)
            if os.path.exists(os.path.join(save_path, 'repr')):
                data.to_csv(save_path + '/repr/' + '_'.join([str(val) for val in date_4download])+'repr.csv', index=False)
            else:
                os.mkdir(os.path.join(save_path, 'repr'))
                data.to_csv(save_path + '/repr/' + '_'.join([str(val) for val in date_4download])+'repr.csv', index=False)

            if os.path.exists(os.path.join(save_path, 'repr0')):
                new_data.to_csv(save_path + '/repr/0' + '_'.join([str(val) for val in date_4download])+'repr0.csv', index=False)
            else:
                os.mkdir(os.path.join(save_path, 'repr0'))
                new_data.to_csv(save_path + '/repr0/' + '_'.join([str(val) for val in date_4download])+'repr0.csv', index=False)
        print('*'*60)


# In[18]:


# folder_download()  


# In[19]:


def single_file_download():
    '''
    function to download pdf files into one file
    '''
    download_count = 0
    unable_to_download = []
    
    all_data = []
    for date_4download in dates_4download:
            date_4download = (date_4download.day, date_4download.month, date_4download.year)
            print(date_4download)
            try:
                data = pdf_to_csv(*format_date(*date_4download))
            except HTTPError:
                    print('Could not download pdf file because of HTTP connection error')
                    continue
                
                    
            data = clean_data(data)
            
            new_data = format_data(*format_date(*date_4download, return_data_path=False), df=data)
                        
            all_data.append(new_data)
    return pd.concat(all_data)
    


# In[20]:


pdfs = single_file_download()


# In[21]:


pdfs.to_csv('C:/Users/hp/Desktop/all_DATA.csv', index=False)


# 
