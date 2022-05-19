# This Function will Fetch Vader sentiment data on the basis of the SQL query and plot 5 graphs
from click import style
from inflection import titleize
import Fud_Fetch_DB_data as dbdf
import pandas as pd
from nltk.corpus import stopwords #This is used to remove commpon keywords from tweet text while calculating the Word frequency count
from dash import Dash, dcc, html, Input, Output #To plot the Graph figures
import plotly.express as px #To plot the Graph figures
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#==============================To PLOT BAR CHARTS OF MOST COMMON USED HASHTAGS IN EACH SENTIMENT CATEGORY=============#

##Dynamic Query Built to fetch Vader Sentiment processed data from DB table TWEETS_SENTIMENT_DATA_VADER
sql_query = "SELECT  DISTINCT A.Tweet_id, B.TWEET_CONTENT, SUBSTR(POSTDATE,1,7) AS TWEET_MONTH, COMPOUND, " 
sql_query = sql_query + " CASE WHEN UPPER(LOCATION) LIKE '%USA%' OR UPPER(LOCATION) LIKE '%UNITED%STATES%' OR UPPER(LOCATION) LIKE '%NEW%YORK%' THEN 'USA' "
sql_query = sql_query + " WHEN UPPER(LOCATION) LIKE '%ENGLAND%' OR UPPER(LOCATION) LIKE '%UK' OR UPPER(LOCATION) LIKE '%LONDON%' THEN 'UK' "
sql_query = sql_query + " WHEN UPPER(LOCATION) LIKE '%INDIA%' THEN 'INDIA' END AS COUNTRY, "
sql_query = sql_query + " CASE WHEN COMPOUND >= 0.05   THEN 'Positive' "
sql_query = sql_query + "     WHEN COMPOUND  <= -0.05 THEN 'Negative' "
sql_query = sql_query + "	 ELSE 'Neutral' "
sql_query = sql_query + " END AS ANALYSIS "
sql_query = sql_query + " FROM TWEETS_SENTIMENT_DATA_VADER A,TWEETS_ID_DATA B "
sql_query = sql_query + " WHERE A.TWEET_ID = B.TWEET_ID "
#sql_query = sql_query + " AND ((UPPER(LOCATION) LIKE '%USA%' OR UPPER(LOCATION) LIKE '%UNITED%STATES%' OR UPPER(LOCATION) LIKE '%NEW%YORK%') OR(UPPER(LOCATION) LIKE '%ENGLAND%' OR UPPER(LOCATION) LIKE '%UK' OR UPPER(LOCATION) LIKE '%LONDON%') OR(UPPER(LOCATION) LIKE '%INDIA%')) "

##DB call to fetch the rows returned from SQL Query
data = dbdf.fn_get_DB_data(sql_query)

df = pd.DataFrame(data)

df.columns = ['Tweet_id', 'TWEET_CONTENT','TWEET_MONTH', 'COMPOUND', 'COUNTRY','ANALYSIS']

df['tweet_without_stopwords'] = df['TWEET_CONTENT'].str.upper()

df['tweet_without_stopwords'] = df['tweet_without_stopwords'].str.replace(' amp ','').str.replace(' 1 ','')

df['tweet_without_stopwords'] = df['tweet_without_stopwords'].str.replace('-','').str.replace('.','').str.replace(',','').str.replace(':','')
df['tweet_without_stopwords'] = df['tweet_without_stopwords'].str.replace('?','').str.replace('COVID_19','').str.replace('COVID-19','').str.replace('COVID19','').str.replace('COVID','').str.replace('CORONAVIRUS','')
df['tweet_without_stopwords'] = df['tweet_without_stopwords'].str.replace('ー19','')

stop = stopwords.words('english')
df['tweet_without_stopwords'] = df['tweet_without_stopwords'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

######  POSITIVE SENTIMENT GRAPHS DF
new_df_pos = df[df['ANALYSIS']== "Positive"]
bar_df_pos = new_df_pos.tweet_without_stopwords.str.split(expand=True).stack().value_counts().reset_index()
bar_df_pos.columns = ['Word', 'Frequency']
bar_df_pos = bar_df_pos[bar_df_pos['Word'].str.startswith('#') ]##PICK ONLY WORDS WITH HASHTAGS
bar_df_pos = bar_df_pos[(bar_df_pos['Word'] != "#")]
#fig_bar_pos = px.bar(new_df[1:50], x='Word', y='Frequency', title='Most Words Frequency In Postive Tweets')

######  NEGATIVE SENTIMENT GRAPHS DF
new_df_neg = df[df['ANALYSIS']== "Negative"]
bar_df_neg = new_df_neg.tweet_without_stopwords.str.split(expand=True).stack().value_counts().reset_index()
bar_df_neg.columns = ['Word', 'Frequency']
bar_df_neg = bar_df_neg[bar_df_neg['Word'].str.startswith('#') ]
bar_df_neg = bar_df_neg[(bar_df_neg['Word'] != "#")]
#fig_bar_neg = px.bar(new_df[1:50], x='Word', y='Frequency', title='Most Words Frequency In Negative Tweets')

######  NEUTRAL SENTIMENT GRAPHS DF
new_df_neu = df[df['ANALYSIS']== "Neutral"]
bar_df_neu = new_df_neu.tweet_without_stopwords.str.split(expand=True).stack().value_counts().reset_index()
bar_df_neu.columns = ['Word', 'Frequency']
bar_df_neu = bar_df_neu[bar_df_neu['Word'].str.startswith('#') ]
bar_df_neu = bar_df_neu[(bar_df_neu['Word'] != "#")]
#fig_bar_neu = px.bar(bar_df_neu[1:50], x='Word', y='Frequency', title='Most Words Frequency In Neutral Tweets')


# ======================== Plotly Graphs
import plotly.graph_objs as go

#==============================To PLOT LINE CHARTS OF EACH SENTIMENT CATEGORY VS EACH MONTH=============#

##Dynamic Query Built to fetch Vader Sentiment processed data from DB table TWEETS_SENTIMENT_DATA_VADER
sql_query = "WITH Q1 AS ( "
sql_query = sql_query + " SELECT  DISTINCT A.TWEET_ID, A.CLEAN_TWEET_TEXT, A.COMPOUND, SUBSTR(POSTDATE,1,7) AS TWEET_MONTH, "
sql_query = sql_query + " CASE WHEN COMPOUND >= 0.05 THEN 'POSITIVE' WHEN COMPOUND  <= -0.05 THEN 'NEGATIVE' ELSE 'NEUTRAL' END AS ANALYSIS "
sql_query = sql_query + " FROM TWEETS_SENTIMENT_DATA_VADER A,TWEETS_ID_DATA B "
sql_query = sql_query + " WHERE A.TWEET_ID = B.TWEET_ID) "
sql_query = sql_query + " SELECT SUM(CASE WHEN ANALYSIS = 'POSITIVE' THEN 1 ELSE 0 END) AS Positive, "
sql_query = sql_query + " SUM(CASE WHEN ANALYSIS = 'NEGATIVE' THEN 1 ELSE 0 END) AS Negative, "
sql_query = sql_query + " SUM(CASE WHEN ANALYSIS = 'NEUTRAL' THEN 1 ELSE 0 END) AS Neutral, "
sql_query = sql_query + " SUM(COMPOUND) AS Compound, "
sql_query = sql_query + " TWEET_MONTH AS Month FROM Q1 "
sql_query = sql_query + " GROUP BY TWEET_MONTH "
data = dbdf.fn_get_DB_data(sql_query)
df_month_wise = pd.DataFrame(data, columns=['Positive','Negative','Neutral', 'Compound', 'Month'])

import plotly.graph_objs as go

Positive_trace = dict(
    x = df_month_wise.Month,
    y = df_month_wise['Positive'],
    mode = 'lines',
    type = 'scatter',
    name = 'Positive',
    line = dict(shape = 'linear', color = 'rgb(10, 120, 24)', width= 4, dash = 'dash'),
    connectgaps = True
)

Negative_trace = go.Scatter(
    x = df_month_wise['Month'],
    y = df_month_wise['Negative'],
    mode = 'lines+markers',
    name = 'Negative',
    line = dict(shape = 'linear', color = 'rgb(205, 12, 24)', dash = 'dash'),
    marker = dict(symbol = "star-diamond", color = 'rgb(205, 12, 24)',size = 12),
    connectgaps = True
)

Neutral_trace = go.Scatter(
    x = df_month_wise.Month,
    y = df_month_wise['Neutral'],
    mode = 'lines',
    name = 'Neutral',
    line = dict(shape = 'linear', color = 'rgb(17, 157, 255)', dash = 'dot'),
    connectgaps = True
)

layout =  dict(
    xaxis = dict(title = 'Month'),
    yaxis = dict(title = 'Sentiments Volume'),
    title='Sentiments Monthly Wise'
)

data_multi = [Positive_trace, Negative_trace, Neutral_trace]
fig_multi =  go.Figure(data = data_multi, layout=layout)

#==============================To PLOT PIE CHART FOR EACH SENTIMENT CATEGORY=============#

sql_query = "with q1 as (select tweet_id,compound, case when compound >= 0.05 then 'Positive' when compound  <= -0.05 then 'Negative' else 'Neutral' end as Sentiment from Tweets_Sentiment_Data_Vader) "
sql_query = sql_query + " select count(1) as Tweets, Sentiment from q1 group by Sentiment order by 1 DESC "
data = dbdf.fn_get_DB_data(sql_query)
df_sentiment = pd.DataFrame(data, columns=['Tweets','Sentiment'])
fig_pie = px.pie(df_sentiment, values='Tweets', names='Sentiment',
             title='Sentiments in Tweets',
             hover_data=['Tweets'], labels={'Total Tweets':'Tweets'})
fig_pie.update_traces(textposition='inside', textinfo='percent+label')

# ======================== Setting the margins
layout = go.Layout(
    margin=go.layout.Margin(
        l=40,  # left margin
        r=40,  # right margin
        b=10,  # bottom margin
        t=35  # top margin
    )
)

def get_pie_graph():
    pie_graph = dcc.Graph(figure=fig_pie,
        style={'width': '30%', 'display': 'inline-block'})
    return pie_graph 

def get_multi_line_graph():
    multi_line_graph = dcc.Graph(figure=go.Figure(data = data_multi, layout=layout),
        style={'width': '70%', 'display': 'inline-block'})
    return multi_line_graph


def get_bar_chart_pos():
    barChart_pos = dcc.Graph(figure=go.Figure(layout=layout).add_trace(go.Bar(x=bar_df_pos['Word'][1:50],
                                                                          y=bar_df_pos['Frequency'][1:50],
                                                                          marker=dict(color='rgb(10, 120, 24)'))).update_layout(
        title='Hashtags Frequency In Postive Tweets', plot_bgcolor='rgba(0,0,0,0)'),
        style={'width': '33%', 'height': '60vh', 'display': 'inline-block'})
    return barChart_pos    

def get_bar_chart_neg():
    barChart_neg = dcc.Graph(figure=go.Figure(layout=layout).add_trace(go.Bar(x=bar_df_neg['Word'][1:50],
                                                                          y=bar_df_neg['Frequency'][1:50],
                                                                          marker=dict(color='rgb(205, 12, 24)'))).update_layout(
        title='Hashtags Frequency In Negative Tweets', plot_bgcolor='rgba(0,0,0,0)'),
        style={'width': '33%', 'height': '60vh', 'display': 'inline-block'})
    return barChart_neg

def get_bar_chart_neu():
    barChart_neu = dcc.Graph(figure=go.Figure(layout=layout).add_trace(go.Bar(x=bar_df_neu['Word'][1:50],
                                                                          y=bar_df_neu['Frequency'][1:50],
                                                                          marker=dict(color='rgb(17, 157, 255)'))).update_layout(
        title='Hashtags Frequency In Neutral Tweets', plot_bgcolor='rgba(0,0,0,0)'),
        style={'width': '33%', 'height': '60vh', 'display': 'inline-block'})
    return barChart_neu

app = Dash(__name__)
# ======================== App Layout
app.layout = html.Div([
    html.H1('Tweets Sentiment Analytics Dashboard', style={'text-align': 'center', 'background-color': '#d3d3d3'}),
    get_bar_chart_pos(),
    get_bar_chart_neg(),
    get_bar_chart_neu(),
    get_pie_graph(),
    get_multi_line_graph()
])
if __name__ == '__main__':
    app.run_server()