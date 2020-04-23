# Import all required libraries.
import pandas as pd
from sqlalchemy import create_engine
from iso639 import languages
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# Function to establish the languages of messages and return 10 most frequently occuring ones.
def popular_languages(messages):
    '''
    Establishes what languages the messages have been written in and
    returns the 10 most frequently occuring languages.

    Parameters:
        messages: A column from a Dataframe that contains original messages
                  in various languages.

    Returns:
        breakdown_languages: A DataFrame showing 10 most popular languages and
                             what % of all messages have been written in those languages.
    '''
    # Assign languages and place them in a list.
    collected_languages = []
    for i in messages:
        
        # Consider only the messages longer than 5 characters and omit all Nones.
        # Otherwise, the language detection library will not be able to appropriately 
        # process the data.
        try:
            if i is not None:
                if len(i) >= 5:
                    code_iso = detect(i)
                    collected_languages.append(languages.get(alpha2 = code_iso).name)
        except LangDetectException:
            pass
    
    # Create a DataFrame showing the most popular languages.
    collected_languages = pd.Index(collected_languages)
    breakdown_languages = round(collected_languages.value_counts()/len(collected_languages)*100)[0:10]
    breakdown_languages = pd.DataFrame(breakdown_languages, columns = ['Language'])
    
    return breakdown_languages

# Use the function above and save the data.
def main():
    '''
    Combines the functions defined above - imports the data, cleans it and 
    stores the clean data into a SQLite database.
    '''
    # Load the data from the database.
    engine = create_engine('sqlite:///data/DisasterResponse.db')
    df = pd.read_sql_table('ETL_data', engine)
    
    # Detect languages.
    print('Detecting languages...')
    languages_data = popular_languages(df["original"])
    
    # Store the top 10 results in the database.
    print('Saving the output...')
    languages_data.to_sql('languages_data', engine, if_exists='replace') 
    
    print('Done!')

if __name__ == '__main__':
    main()