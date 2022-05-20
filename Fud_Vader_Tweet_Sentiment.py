# function to Compute sentiments using VADER
# of the sentence.
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import Fud_Fetch_DB_data as dbdf
import re
import pandas as pd
import sqlite3

def cleanTxt(text):
    text = re.sub(r'@[A-Za-z0-9]+','',text) #remove @mentions
    text = re.sub(r'#','',text) #remove # symbol
    text = re.sub(r'http?:\/\/S+','',text) #remove Http links
    return text

def sentiment_scores():
# Create a SentimentIntensityAnalyzer object.
    analyzer = SentimentIntensityAnalyzer()
# polarity_scores method of SentimentIntensityAnalyzer
# object gives a sentiment dictionary.
    sql_query = """select a.Tweet_id as Tweet_id, a.Tweet_Content as Tweet_Content from Tweets_Raw_Data a where length(location) > 0
                and a.Tweet_ID not in (select b.Tweet_id from Tweets_Sentiment_Data b) """
    
    data = dbdf.fn_get_DB_data(sql_query)
    print("Total Fetched Tweets Data Size:" + str(len(data))) 

    if len(data)== 0:
        print('No New Tweet Raw Data Available to process')
        exit()

    df = pd.DataFrame(data, columns=['Tweet_id','Tweet_Content'])

    df['Tweet_Content'] = df['Tweet_Content'].apply(cleanTxt)    
    
    df['neg'] = [analyzer.polarity_scores(x)['neg'] for x in df['Tweet_Content']]
    df['neu'] = [analyzer.polarity_scores(x)['neu'] for x in df['Tweet_Content']]
    df['pos'] = [analyzer.polarity_scores(x)['pos'] for x in df['Tweet_Content']]
    df['compound'] = [analyzer.polarity_scores(x)['compound'] for x in df['Tweet_Content']]    

    ##SQLITE3 DATABASE CONNECTION IN ORDER TO INSERT THE TWEET DATA
    path = r"lboro_lab.db"
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    try:
         for row in df.itertuples():         
             insert_sql = f"insert into Tweets_Sentiment_Data (Tweet_ID, Clean_Tweet_text, neg, neu, pos, compound) values ('{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}','{row[6]}')"
             cursor.execute(insert_sql)
    except Exception as e:
         ##Exception handling in case of error received while inserting. 
         print(e)
    conn.commit()
    conn.close()
    print("Sentiment Analysis Completed for total Tweets:" + str(len(df)))    

# Driver code
if __name__ == "__main__" :
    # function calling
    sentiment_scores()