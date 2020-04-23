# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 15:09:29 2020

@author: dawid
"""

import sys
import pandas as pd
from sqlalchemy import create_engine

# A function to load and merge the two tables.
def load_data(messages_filepath, categories_filepath):
    messages_data = pd.read_csv(messages_filepath)
    categories_data = pd.read_csv(categories_filepath)
    df = messages_data.merge(categories_data, left_on='id', right_on='id')
    
    return df

# A function to identify all categories and count them.
def categories_details(df):
    categories_names = [k[:-2] for k in df["categories"][0].split(';')]
    num_categories = len(categories_names)
    
    return categories_names, num_categories

# A function to separate out the categories squeezed into one field for one row.
def get_category_values(row, num_categories):
    
    # Separate combined categories into different columns.
    splitted_row = pd.Series(row).str.split(";", expand = True)
    
    # Extract 0s and 1s.
    for i in range(0, num_categories):
        splitted_row.loc[:,i] = int(splitted_row.loc[:,i].values[0][-1])
    
    return splitted_row.values

def clean_data(df):
    
    # Remove duplicates. 
    # My approach: check original messages and their IDs for duplicates.
    
    # There are different ways in which you could identify duplicates.
    # If you check for IDs and original messages, you make sure that
    # only unique original messages are included and if there are two identical
    # texts but relating to different situations/times (e.g. "help!"), those 
    # will also be captured in the final DataFrame as they should given their
    # different IDs. 
    
    df_no_duplicates = df[df.duplicated(["original", "id"]) == False].reset_index(drop=True)
    
    # Drop the "id" column since we do not need it anymore.
    df_no_duplicates = df_no_duplicates.drop(columns = ["id"])
    
    # Get the names of categories and their count.
    categories_names, num_categories = categories_details(df_no_duplicates)
    
    # Create an empty Dataframe.
    categories_data_splitted = pd.DataFrame(columns = list(range(0, num_categories)))
    
    # Split the categories and place inside the new dataframe.
    for i in range(0, df_no_duplicates.shape[0]):
        categories_data_splitted.loc[i,:] = get_category_values(df_no_duplicates["categories"][i],
                                                                num_categories)
    
    # Drop the "categories" column since we do not need it anymore.
    df_no_duplicates = df_no_duplicates.drop(columns = ["categories"])
    
    # Combine the newly-created DataFrame and the original one w/o duplicates.
    df_combined = pd.concat([df_no_duplicates, categories_data_splitted], axis=1, 
                            ignore_index=True)
    
    # Rename the columns.
    df_combined.columns = df_no_duplicates.columns.tolist()  + categories_names
    
    return df_combined

def save_data(df, database_filename):
    
    engine = create_engine('sqlite:///DisasterResponse.db')
    df.to_sql('ETL_data', engine, index=False) 

def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()