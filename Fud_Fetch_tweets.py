import snscrape.modules.twitter as sntwitter
import pandas as pd
import Fud_Fetch_DB_data as dbdf
import sqlite3
import datetime as dt

## This program will fetch the Tweets on the basis of the query format accepted by Twitter

def fetch_tweets_data_scn(keywords,start_Date,end_Date,total_tweets,del_flag):
 query ="(" + keywords + ") lang:en until:" + end_Date + " since:" + start_Date + " -filter:links -filter:replies" 
 tweets = []
 
 ## To create the Process log to maintain the details of each run
 todaydatetime = dt.datetime.today()
 todaydate = dt.date.today()
 log_text = "Process starting datetime         :" + str(todaydatetime) + "\n"
 log_file_name = r"Process_log"+ "_" + str(todaydate) + ".txt"
 f = open(log_file_name, "a")
 f.write("===================================================================\n")
 f.write("Keywords  :"+ keywords + "\n")
 f.write("Date Range from: "+ start_Date + "to " + end_Date + "\n")
 f.write("Tweet Query:"+ query + "\n") 
 f.write(log_text)

 ##Twitter Realtime Tweets Fetching Begin
 for tweet in sntwitter.TwitterSearchScraper(query).get_items():
     if len(tweets) == total_tweets:
       break
     else:
       tweets.append([tweet.id, tweet.user.location, tweet.date, tweet.content])

 ##End of Twitter Realtime Tweets Fetching         

 ##Logging of Total Tweets and Timesatamp in the process log file
 todaydatetime = dt.datetime.today()
 log_text = "Total Tweets Fetched              :" + str(len(tweets)) + "\n" 
 f.write(log_text) 
 log_text = "Tweet fetching Completed datetime :" + str(todaydatetime) + "\n" 
 f.write(log_text)
 
 ##Creation of Dataframe to hold the tweet data
 df = pd.DataFrame(tweets, columns=['Tweet_ID', 'Location', 'Date', 'Tweet_Content'])
 tweets_df = df.replace("'"," ", regex=True)#Data Cleaning to make it compatible with SQLite3 insert
 
 ##SQLITE3 DATABASE CONNECTION IN ORDER TO INSERT THE TWEET DATA
 path = r"lboro_lab.db"
 conn = sqlite3.connect(path)
 cursor = conn.cursor()
 
 ##Delete Flag to remove existing data and load new data
 if del_flag == "Y":
     del_Str = "delete from Tweets_Raw_Data"
     cursor.execute(del_Str)
     conn.commit()
 
 ##High perfomrance methodlogy used to insert data into the database table Tweets_Raw_Data
 try:
     for row in tweets_df.itertuples():         
         insert_sql = f"insert into Tweets_Raw_Data (Tweet_ID, Location, PostDate, Tweet_Content) values ('{row[1]}','{row[2]}','{str(row[3])[0:10]}','{row[4]}')"
         cursor.execute(insert_sql)
     conn.commit()
     conn.close()
 except Exception as e:
     ##Exception handling in case of error received while inserting. 
     f = open(log_file_name, "a")
     log_text = str(e) + "\n"        
     f.write(log_text)
     f.write("===================================================================\n")
     f.close()
     return "Process Completed with Failure(s). Please check the details in log file [" + log_file_name + "]"
 todaydatetime = dt.datetime.today()
 log_text = "Tweet Insertion Completed Datetime:" + str(todaydatetime) + "\n" 
 f.write(log_text)
 f.write("===================================================================\n")
 f.close()
 ##Process completion logging into log file
 return "Process Completed Successfully. Please check the details in log file [" + log_file_name + "]"

## Main Program to call the fetch_tweets_data_scn
from tqdm import tqdm, trange ##To be used to show progress bar
from turtle import textinput ##To prompt the user to enter the required input parameters
import time
def main(): 
    ''' 
    Yearstr = textinput("Start Year", "Please Enter Year  Value (format YYYY only):")
    Monthstr = textinput("Start Month","Enter Month Value (format MM only) :")
    No_of_months_str = textinput("Total Months","Please Enter Total Months to be Processed :")
    Tweets_fetch_limit_str = textinput("Total Tweets","Please Enter Total Tweets to be Fetched :") '''

    Yearstr = input("Please Enter Year  Value (format YYYY only):")
    Monthstr = input("Enter Month Value (format MM only) :")
    No_of_months_str = input("Please Enter Total Months to be Processed :")
    Tweets_fetch_limit_str = input("Please Enter Total Tweets to be Fetched :")
    
    df = dbdf.fn_get_DB_data("select * from Tweets_Raw_Data")
    before_count = len(df)    
    print("Total Tweets Data Size Before Processing:" + str(len(df)))
    
    ##User input Validation For Year Field
    if int(Yearstr[0:2]) != 20 or len(Yearstr) != 4 or len(Yearstr) == 0:
        print("Error: Please Enter valid current Century Year (4 Chars only)! Exiting")
        exit()
    try:
        Year = int(Yearstr)
    except Exception as e:
      print(e)
      exit()     
    
    ##User input Validation For Month Field
    try:        
        Month = int(Monthstr)
    except Exception as e:
      print("Error: Please Enter valid Month {values between 1-12}! Exiting")
      exit()    
  
    if Month > 12 or Month < 1:
        print("Error: Please Enter valid Month {values between 1-12}! Exiting")
        exit()       
    
    ##User input Validation For No_of_months_str Field
    try:
        No_of_months = int(No_of_months_str)
    except Exception as e:
      print("Please Enter valid Value for Total Months (Numeric only!!)")
      exit()

    ##User input Validation For Tweets_fetch_limit Field
    try:
        Tweets_fetch_limit = int(Tweets_fetch_limit_str)
    except Exception as e:
      print("Please Enter valid Value for Total Tweets To Be Fetched (Numeric only!!)")
      exit() 

    if Tweets_fetch_limit > 10000 or Tweets_fetch_limit < 1:
        print("Error: Please Enter valid Total Tweets To Be Fetched {values between 1-10000}! Exiting")
        exit()  
    
    import calendar
    pbar =tqdm(total=100)
    for i in range(No_of_months):        
        #iter_Start_date = str(Year) + "-" + str(Month) + "-" + "1"
        days_in_month = calendar.monthrange(Year,Month)[1]
        days_counter = 1
        for x in range(2):
            if x == 0:
                iter_Start_date = str(Year) + "-" + str(Month) + "-"+ str(days_counter)
                days_counter = days_counter + 15
                iter_end_Date = str(Year) + "-" + str(Month) + "-"+ "15"
            else:
                iter_Start_date = str(Year) + "-" + str(Month) + "-"+ str(days_counter)
                iter_end_Date = str(Year) + "-" + str(Month) + "-"+ str(days_in_month)
            
            ##To call the tweets fetching program for each iteration
            Status = fetch_tweets_data_scn('#Covid19 OR #Covid OR #Coronavirus OR #Pandemic',iter_Start_date,iter_end_Date,Tweets_fetch_limit,"N")
            pbar.update(int(100/(No_of_months*2)))
        
        Month = Month + 1
        if Month == 13:
           Month = 1
           Year = Year + 1
    pbar.close()
    print(Status)        
    
    df = dbdf.fn_get_DB_data("select * from Tweets_Raw_Data")
    after_count = len(df)
    print("Total Loaded Tweets Data Size with current run:" + str(after_count - before_count))    

if __name__ == "__main__":
    main()