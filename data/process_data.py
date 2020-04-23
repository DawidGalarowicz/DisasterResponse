# Import all required libraries.
import sys
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    '''
    Imports two .csv files containing the messages and their categories to combine them
    into one pandas DataFrame.

    Parameters:
        messages_filepath: A file path of the .csv with our messages and their genres.
        
        categories_filepath: A file path of the .csv with our categories.

    Returns:
        df: A DataFrame resulting from merging both .csv files.   
    '''
    messages_data = pd.read_csv(messages_filepath)
    categories_data = pd.read_csv(categories_filepath)
    df = messages_data.merge(categories_data, left_on='id', right_on='id')
    
    return df

def categories_details(df):
    '''
    Finds the names of all possible categories and counts how many there are.

    Parameters:
        df: A DataFrame from load_data().

    Returns:
        categories_names: Names of all possible categories that our messages fall into.
        
        num_categories: Count of all possible categories that our messages fall into.
    '''
    categories_names = [k[:-2] for k in df["categories"][0].split(';')]
    num_categories = len(categories_names)
    
    return categories_names, num_categories

def get_category_values(row, num_categories):
    '''
    Separates out the categories squeezed into one cell and places them into a single row.

    Parameters:
        row: A row from the DataFrame based on disaster_categories.csv where
             all categories are kept in one cell.
             
        num_categories: Count of all possible categories that our messages fall into.

    Returns:
        splitted_row.values: A row where each cell corresponds to a category and
                             is populated with either 0 or 1.
    '''
    
    # Separate combined categories into different columns.
    splitted_row = pd.Series(row).str.split(";", expand = True)
    
    # Extract 0s and 1s.
    for i in range(0, num_categories):
        splitted_row.loc[:,i] = int(splitted_row.loc[:,i].values[0][-1])
    
    return splitted_row.values

def clean_data(df):
    '''
    Cleans the data before it is inserted into the database.

    Parameters:
        df: A DataFrame from load_data().

    Returns:
        splitted_row.values: A row where each cell corresponds to a category and
                             is populated with either 0 or 1.
    '''

    # Remove duplicates. 
    df_no_duplicates = df[df.duplicated(["original", "id"]) == False].reset_index(drop=True)
    
    ##############################################################################
    # There are different ways in which you could identify duplicates.
    # If you check for IDs and original messages (like here), you make sure that
    # only unique original messages are included and if there are two identical
    # texts but relating to different situations/times (e.g. "help!"), those 
    # will also be captured in the final DataFrame as they should given their
    # different IDs. 
    ##############################################################################
    
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
    
    # Combine the newly-created DataFrame and the original one without duplicates.
    df_combined = pd.concat([df_no_duplicates, categories_data_splitted], axis=1, 
                            ignore_index=True)
    
    # Rename the columns.
    df_combined.columns = df_no_duplicates.columns.tolist()  + categories_names
    
    return df_combined

def save_data(df, database_filename):
    '''
    Inserts the data (table "ETL_data") into the database.

    Parameters:
        df: A DataFrame returned by clean_data().

    Returns:
        -
    '''
    engine = create_engine('sqlite:///data/DisasterResponse.db')
    df.to_sql('ETL_data', engine, index=False, if_exists='replace') 

def main():
    '''
    Combines the functions defined above - imports the data, cleans it and 
    stores the clean data into a SQLite database.
    '''
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