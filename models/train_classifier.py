# Import the required libraries.
import sys
import re
import nltk

nltk.download('stopwords') # Check if downloaded (if not, download)
nltk.download('wordnet') # Check if downloaded (if not, download)
nltk.download('punkt') # Check if downloaded (if not, download)

import pandas as pd
from sklearn.externals import joblib
from sqlalchemy import create_engine
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import classification_report

def load_data(database_filepath):
    '''
    Imports the data that has been created during the ETL stage.

    Parameters:
        database_filepath: A file path of the database.

    Returns:
        X: A DataFrame holding the column with messages translated into English.
        
        y: A DataFrame holding columns with 0s and 1s indicating categories.
    '''
    engine = create_engine("sqlite:///"+database_filepath)
    df = pd.read_sql_table("ETL_data", engine)
    X = df["message"]
    y = df.drop(columns = ["original", "message", "genre"])
    return X, y

def tokenize(text):
    '''
    Processes the text data as the first step of the pipeline.

    Parameters:
        text: A text message to be processed.

    Returns:
        stemmed: The same message after normalising, lemmatising, stemming and
                 tokenising.
            
    '''
    text = re.sub(r"[^a-zA-Z0-9]", " ", text.lower())
    words = word_tokenize(text)
    words = [w for w in words if w not in stopwords.words("english")]
    lemmed = [WordNetLemmatizer().lemmatize(w) for w in words]
    stemmed = [PorterStemmer().stem(w) for w in lemmed]
    return stemmed

def build_model():
    '''
    The NLP pipeline based on a Random Forest classifier which will be used to make
    predictions for each category of messages.

    Parameters:
        -

    Returns:
        cv: A multi-output classifier that has been tuned using GridSearchCV.
            
    '''
    pipeline_text = Pipeline([
        ('vect', CountVectorizer(tokenizer = tokenize)),
        ('tfidf', TfidfTransformer()),
        ('classifier', MultiOutputClassifier(RandomForestClassifier(n_jobs=-1)))
    ])

    # Having a small number of hyperparameters below has been dictated 
    # by the long waiting times to perform the computations.
    parameters = {
        'classifier__estimator__n_estimators': [50, 100],
        'classifier__estimator__min_samples_split': [2, 3],
    }

    # cv = 2 to mainly speed the process of cross-validation up. 
    cv = GridSearchCV(pipeline_text, param_grid = parameters, cv = 2)

    return cv

def evaluate_model(model, X_test, y_test):
    '''
    Produces metrics given the test data for each category 
    to evaluate how well the classifier is performing; also, prints out
    macro averages of f1-scores, recall and precision for each category.

    Parameters:
        model: A multi-output classifier from build_model().
        
        X_test: Our testing set - messages in English.
        
        y_test: Our testing set - columns with indicators of categories/labels.

    Returns:
        -
            
    '''
    # Generate predictions.
    y_pred = model.predict(X_test)
    label_names = y_test.columns
    y_pred = pd.DataFrame(y_pred, columns = label_names)
    
    # Produce performance reports.
    reports = []
    for col in label_names:
        reports.append(classification_report(y_test[col], y_pred[col], output_dict=True))
    reports = dict(zip(label_names, reports))
    
    # Print the metrics out.
    print('######')
    print('Model metrics (macro avg.):')    
    for label in label_names:
        metrics = reports[label].get("macro avg")
        f1 = round(metrics.get('f1-score'), 3)
        recall = round(metrics.get('recall'), 3)
        precision = round(metrics.get('precision'), 3)
        print("%s - f1-score:%s, recall:%s, precision:%s," % (label, f1, recall, precision))
    print('######')

def save_model(model, model_filepath):
    '''
    Saves our model as a .pkl.

    Parameters:
        model: A multi-output classifier from build_model().
        
        model_filepath: A file path of the saved model.

    Returns:
        -
            
    '''
    joblib.dump(model, model_filepath)

def main():
    '''
    Combines the functions defined above - loads the data, uses the pipeline,
    evaluates the model and saves it.
    '''
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, y = load_data(database_filepath)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        
        print('Building model...')
        model = build_model()
        
        print('Training model...')
        model.fit(X_train, y_train)
        
        print('Evaluating model...')
        evaluate_model(model, X_test, y_test)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__':
    main()
  