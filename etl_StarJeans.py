# 0.0. Imports

import re
import requests 
import pandas as pd
import numpy  as np

from datetime import datetime

from tqdm import tqdm

from bs4 import BeautifulSoup

import sqlite3
from sqlalchemy import create_engine


## 0.1. Loading data
def get_showroom_data( url, headers ):
    page = requests.get( url, headers=headers )
    soup = BeautifulSoup( page.text, 'html.parser' )

    # get section with all products
    products = soup.find('ul', class_ = 'products-listing small')

    # get list of products overall
    product_list = soup.find_all('article', class_ = 'hm-product-item')

    # product_id
    product_id = [p.get('data-articlecode') for p in product_list]

    # product_category
    product_category = [p.get('data-category') for p in product_list]

    # get list of products to get name
    product_list = products.find_all('a', class_ = 'link')

    # product_name
    product_name = [p.get_text() for p in product_list]

    # get list of products to get price
    product_list = products.find_all('span', class_ = 'price regular')

    # product_price
    product_price = [p.get_text() for p in product_list]

    # pass data to DataFrame
    df_products = pd.DataFrame([product_id, product_category, product_name, product_price]).T
    df_products.columns = ['product_id', 'product_category', 'product_name', 'product_price']
    
    return df_products


def get_product_details( data_scraped, headers):
    cols = ['Art. No.', 'Composition', 'Fit', 'Product safety', 'Size', 'More sustainable materials']
    df_pattern = pd.DataFrame(columns = cols)
    df_compositions = pd.DataFrame()
    aux = []

    for index in tqdm(data_scraped['product_id']):
        url = f'https://www2.hm.com/en_us/productpage.{index}.html'

        # request
        page = requests.get(url, headers = headers)
        
        # instantiate BeatifulSoup
        soup = BeautifulSoup(page.text, 'html.parser')
        
        # COLOR
        ## product list
        product_list = soup.find_all('a', class_ = 'filter-option miniature active') + soup.find_all('a', class_ = 'filter-option miniature')
        
        ## color name
        color_name = [p.get('data-color') for p in product_list]
        
        # ID FOR MERGE
        ## product_id
        color_product_id = [p.get('data-articlecode') for p in product_list]
        
        # pass to dataframe
        df_color = pd.DataFrame([color_product_id, color_name]).T
        df_color.columns = ['product_id', 'color_name']

        for color_index in df_color['product_id']:
            url = f'https://www2.hm.com/en_us/productpage.{color_index}.html'

            # request
            page = requests.get(url, headers = headers)

            # instantiate BeatifulSoup
            soup = BeautifulSoup(page.text, 'html.parser')
            
            # product name
            product_name = soup.find_all('h1', class_ = 'primary product-item-headline')
            product_name = product_name[0].get_text()
            
            # product price
            product_price = soup.find_all('div', class_ = 'primary-row product-item-price')
            product_price = re.findall(r'\d+\.?\d+', product_price[0].get_text())[0]
            
            # COMPOSITION
            ## composition list
            product_composition_list = soup.find_all('div', class_ = 'pdp-description-list-item')

            ## composition names
            product_composition = [list(filter(None, c.get_text().split('\n'))) for c in product_composition_list]

            # pass to dataframe
            df_composition = pd.DataFrame(product_composition).T

            # set columns
            df_composition.columns = df_composition.iloc[0]

            # delete first row and fill na
            df_composition = df_composition.iloc[1:].fillna(method = 'ffill')

            # remove pocket lining, shell and lining
            df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket lining: ', '', regex = True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Shell: ', '', regex = True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Lining: ', '', regex = True)

            # garantee same number of columns
            df_composition = pd.concat([df_pattern, df_composition], axis = 0).reset_index(drop = True)

            # rename columns
            df_composition.columns = ['product_id', 'composition', 'fit', 'product_safety', 'size', 'more_sustainable_materials']
            
            # set product name and price
            df_composition['product_name'] = product_name
            df_composition['product_price'] = product_price   

            # keep track on new columns
            aux = aux + df_composition.columns.to_list()

            # merge color and composition
            df_composition = df_composition.merge(df_color, how = 'left', on = 'product_id')

            # all details from products
            df_compositions = pd.concat([df_compositions, df_composition], axis = 0).reset_index(drop = True)

    # generate style id + color id
    df_compositions['style_id'] = df_compositions['product_id'].apply(lambda x: x[:-3])
    df_compositions['color_id'] = df_compositions['product_id'].apply(lambda x: x[-3:])

    # scrapy datetime
    df_compositions['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # lower column names
    df_compositions.columns = df_compositions.columns.str.lower()        
        
    return df_compositions



def data_cleaning( data ):
    # product_id
    df_data = data.dropna(subset = ['product_id'])

    # product name - change format and remove tabs and newlines
    df_data['product_name'] = df_data['product_name'].str.replace('\n', '')
    df_data['product_name'] = df_data['product_name'].str.replace('\t', '')
    df_data['product_name'] = df_data['product_name'].str.replace('  ', '')
    df_data['product_name'] = df_data['product_name'].str.replace(' ', '_').str.lower()

    # product price - remove $
    df_data['product_price'] = df_data['product_price'].astype(float)

    # # scrapy datetime
    # df_data['scrapy_datetime'] = pd.to_datetime(df_data['scrapy_datetime'], errors = 'coerce')

    # # style id
    # df_data['style_id'] = df_data['style_id'].astype(int)

    # # color id
    # df_data['color_id'] = df_data['color_id'].astype(int)

    # color name - change format
    df_data['color_name'] = df_data['color_name'].str.replace(' ', '_').str.lower()\

    # fit
    df_data['fit'] = df_data['fit'].apply(lambda x: x.lower().replace(' ', '_') if pd.notnull(x) else x)

    # size number
    df_data['size_number'] = df_data['size'].apply(lambda x: re.search('\d{3}cm', x).group(0) if pd.notnull(x) else x)
    df_data['size_number'] = df_data['size_number'].apply(lambda x: re.search('\d+', x).group(0) if pd.notnull(x) else x)

    # size model
    df_data['size_model'] = df_data['size'].str.extract('(\d+/\\d+)')

    # # composition
    # df_data = df_data[~df_data['composition'].str.contains('Pocket lining:', na = False)]
    # df_data = df_data[~df_data['composition'].str.contains('Lining:', na = False)]
    # df_data = df_data[~df_data['composition'].str.contains('Shell:', na = False)]
    # df_data = df_data[~df_data['composition'].str.contains('Pocket:', na = False)]

    # # drop duplicates
    # df_data = df_data.drop_duplicates(subset = ['product_id', 'product_category', 'product_name', 'product_price',
    #                                   'scrapy_datetime', 'style_id', 'color_id', 'color_name', 'fit'],
    #                         keep = 'last')

    # # reset index
    # df_data = df_data.reset_index(drop = True)

    # break composition by comma
    df1 = df_data['composition'].str.split(',', expand = True).reset_index(drop = True)

    # cotton / polyester / elastane / elasterell
    df_ref = pd.DataFrame(index = np.arange(len(df_data)), columns = ['cotton', 'polyester', 'elastane', 'elasterell'])

    # cotton
    df_cotton_0 = df1.loc[df1[0].str.contains('Cotton', na = True), 0]
    df_cotton_0.name = 'cotton'

    df_cotton_1 = df1.loc[df1[1].str.contains('Cotton', na = True), 1]
    df_cotton_1.name = 'cotton'

    ## combine cotton
    df_cotton = df_cotton_0.combine_first(df_cotton_1)

    df_ref = pd.concat([df_ref, df_cotton], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep = 'last')]

    # polyester
    df_polyester_0 = df1.loc[df1[0].str.contains('Polyester', na = True), 0]
    df_polyester_0.name = 'polyester'

    df_polyester_1 = df1.loc[df1[1].str.contains('Polyester', na = True), 1]
    df_polyester_1.name = 'polyester'

    ## combine polyester
    df_polyester = df_polyester_0.combine_first(df_polyester_1)

    df_ref = pd.concat([df_ref, df_polyester], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep = 'last')]

    # elastane
    df_elastane_1 = df1.loc[df1[1].str.contains('Elastane', na = True), 1]
    df_elastane_1.name = 'elastane'

    df_elastane_2 = df1.loc[df1[2].str.contains('Elastane', na = True), 2]
    df_elastane_2.name = 'elastane'

    df_elastane_3 = df1.loc[df1[3].str.contains('Elastane', na = True), 3]
    df_elastane_3.name = 'elastane'

    ## combine elastane
    df_elastane_c2 = df_elastane_1.combine_first(df_elastane_2)
    df_elastane = df_elastane_c2.combine_first(df_elastane_3)

    df_ref = pd.concat([df_ref, df_elastane], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep = 'last')]

    # elasterell
    df_elasterell = df1.loc[df1[1].str.contains('Elasterell', na = True), 1]
    df_elasterell.name = 'elasterell'

    df_ref = pd.concat([df_ref, df_elasterell], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep = 'last')]

    # join combine df with product_id
    df_aux = pd.concat([df_data['product_id'].reset_index(drop = True), df_ref], axis = 1)

    # format composition data
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['elastane'] = df_aux['elastane'].apply(lambda x: int(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['elasterell'] = df_aux['elasterell'].apply(lambda x: int(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)

    # final join
    df_aux = df_aux.groupby('product_id').max().reset_index().fillna(0)

    df_data = pd.merge(df_data, df_aux, on = 'product_id', how = 'left')

    # drop columns
    df_data = df_data.drop(columns = ['size', 'product_safety', 'composition'])

    # drop duplicates
    df_data = df_data.drop_duplicates(subset = ['product_id'], keep = 'last').reset_index(drop = True)

    return df_data

def data_insert(df_clean):
    df_clean = df_clean[['product_id',
                        'product_name', 
                        'product_price',
                        'scrapy_datetime', 
                        'style_id', 
                        'color_id', 
                        'color_name', 
                        'fit',
                        'more_sustainable_materials',
                        'size_number', 
                        'size_model', 
                        'cotton',
                        'polyester',
                        'elastane',
                        'elasterell']]
    
    # connect to database
    conn = sqlite3.connect('./database/hm_db.sqlite')

    # insert data to table
    df_clean.to_sql('showroom', con = conn, if_exists = 'append', index = False)


if __name__ == "__main__":
    # parameters
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    
    # Extraction
    data_scraped = get_showroom_data( url, headers )

    # Transformation
    df_raw = get_product_details( data_scraped, headers)
    
    # Cleaning
    data = data_cleaning( df_raw )

    # Load
    data_insert(data)

    
