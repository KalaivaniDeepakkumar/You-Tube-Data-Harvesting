import googleapiclient.discovery
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from datetime import datetime
import plotly.express as px
from PIL import Image
# YouTube API key
api='AIzaSyD64sl0n4KHDp0ZFQlqP3ToLzqD8TXuwCs'
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["project1"]
my_collection = mydb["data"]
youtube = googleapiclient.discovery.build(
        "youtube", "v3", developerKey=api)
#channel details
def get_channel_details(ch_ids):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id=','.join(ch_ids)).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = ch_ids[0],
                    channel_name = response['items'][i]['snippet']['title'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    subscribers = response['items'][i]['statistics']['subscriberCount'],
                    viewcount = response['items'][i]['statistics']['viewCount'],
                    videocount= response['items'][i]['statistics']['videoCount'],
                    description = response['items'][i]['snippet']['description'],
                    country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

#playlist details
def get_channel_videos(ch_ids):
    video_ids = []
    # get Uploads playlist id
    response = youtube.channels().list(id=ch_ids,
                                  part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return (video_ids)

#duration conversion

def convert_duration(duration_string):
  duration_string = duration_string[2:]  
  duration = timedelta()
    
    
  if 'H' in duration_string:
    hours, duration_string = duration_string.split('H')
    duration += timedelta(hours=int(hours))
    
  if 'M' in duration_string:
    minutes, duration_string = duration_string.split('M')
    duration += timedelta(minutes=int(minutes))
    
  if 'S' in duration_string:
    seconds, duration_string = duration_string.split('S')
    duration += timedelta(seconds=int(seconds))
    
    # Format duration as H:MM:SS
  duration_formatted = str(duration)
  if '.' in duration_formatted:
    hours, rest = duration_formatted.split(':')
    minutes, seconds = rest.split('.')
    duration_formatted = f'{int(hours)}:{int(minutes):02d}:{int(seconds):02d}'
  else:
    duration_formatted = duration_formatted.rjust(8, '0')
    
  return duration_formatted

def convert_timestamp(timestamp):
    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time

#video details
def get_video_details(video_ids):
    video_data = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = ','.join(video['snippet'].get('tags',[])),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = convert_duration(video['contentDetails']['duration']),
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption'])
            video_data.append(video_details)
    return video_data

#comment details
def get_comments_details(video_ids):
    comment_data = []
    for i in video_ids:
      try:

            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=i,
                                                    maxResults=100).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)

      except:
          pass
    return comment_data

#channel information main function

def channel_info(ch_ids):
  c=get_channel_details(ch_ids)
  p=get_channel_videos(ch_ids)
  b=get_video_details(p)
  com=get_comments_details(p)

  data={'channel details':c,
        'video details':b ,
        'comment details':com}
  return data

#store in mongodb

def store_in_mongodb(details):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["project1"]
    my_collection = mydb["data"]
    my_collection.insert_one(details)
    

#connect to mysql database


mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="kalai@20",
    database="project1",
    auth_plugin = 'mysql_native_password',
    autocommit=True
)
mycursor=mydb.cursor(buffered=True)

#create sql table
def create_tables():
    drop_query = "drop table if exists channels"
    mycursor.execute(drop_query)
    mydb.commit()
    
    try:
        mycursor.execute('''CREATE TABLE channeldetails (Channel_id VARCHAR(255) primary key,
                                                  channel_name VARCHAR(255), 
                                                  playlist_id VARCHAR(255), 
                                                  subscribers VARCHAR(255),
                                                  viewcount VARCHAR(255), 
                                                  videocount VARCHAR(255),
                                                  description text, 
                                                  country text)''')
   
        mycursor.execute('''CREATE TABLE videodetails(Channel_name VARCHAR(255),
                                              Channel_id VARCHAR(255),
                                              Video_id VARCHAR(255) primary key,
                                              Title VARCHAR(255),
                                              Tags text,
                                              Thumbnail VARCHAR(255),
                                              Description text,
                                              Published_date VARCHAR(255),
                                              Duration VARCHAR(255),
                                              Views VARCHAR(255),
                                              Likes VARCHAR(255),
                                              Comments VARCHAR(255),
                                              Favorite_count VARCHAR(255),
                                              Definition text,
                                              Caption_status VARCHAR(255))''')
        mycursor.execute('''CREATE TABLE commentdetails(Comment_id VARCHAR(255), 
                                                  Video_id VARCHAR(255),
                                                  Comment_text text,
                                                  Comment_author VARCHAR(255),
                                                  Comment_posted_date VARCHAR(255),
                                                  Like_count VARCHAR(255), 
                                                  Reply_count VARCHAR(255))''')
    except:
         print("Channels Table alredy created")

#insert into table values
   
def store_in_tables(A):
    sql='''INSERT INTO channeldetails (Channel_id, 
                                   channel_name,
                                    playlist_id,
                                    subscribers,
                                    viewcount, 
                                    videocount, 
                                    description, 
                                    country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
    value=tuple(A['channel details'][0].values())
    try:
            mycursor.execute(sql,value)                     
            mydb.commit()    
    except:
            st.write("Channels values are already inserted")

    sql='''INSERT INTO videodetails(Channel_name, 
                               Channel_id, 
                               Video_id, 
                               Title, 
                               Tags, 
                               Thumbnail, 
                               Description, 
                               Published_date, 
                               Duration, 
                               Views, 
                               Likes, 
                               Comments, 
                               Favorite_count, 
                               Definition,  
                               Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    for i in A['video details']:
        value=tuple(i.values())
        mycursor.execute(sql,value)

    sql='''INSERT INTO commentdetails(Comment_id, 
                                  Video_id, 
                                  Comment_text,
                                  Comment_author, 
                                  Comment_posted_date,
                                  Like_count, 
                                  Reply_count) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    for i in A['comment details']:
        value=tuple(i.values())
        mycursor.execute(sql,value)
    mydb.commit()


 # SETTING STREAMLIT PAGE 
st.set_page_config(
    page_title="YOU TUBE PROJECT",
    page_icon=":smile:",
    layout= "wide",
    initial_sidebar_state= "expanded"
)
st.title("YOU TUBE DATA PROJECT")
with st.sidebar:
    selected = option_menu("Main Menu", ["Home","Data Extraction","Data Transformation","Queries"], 
        icons=['house', 'gear'], menu_icon="cast", default_index=1,orientation="vertical")
if selected== "Home":
      st.write(" This you tube data harvest project can be used to collect large amount of data from YouTube. The data can be stored in MongoDB and SQL and to visualize the data using sql query.") 
      col1,col2 = st.columns(2,gap= 'large')
if selected == "Data Extraction":
    tab1 = st.tabs(["$\ EXTRACT $"])
    st.markdown("#    ")
    st.write("#### Enter YouTube Channel_ID below :")
    ch_ids = st.text_input("Get Channel ID from channel Page").split(',')
    if ch_ids and st.button("Extract Data"):
        details = channel_info(ch_ids)
        st.write('### Data Collected Successfully')
    #if st.button("### :green[Upload to MongoDB]"):
        with st.spinner('Please Wait for it...'):
            store_in_mongodb(details)
            st.write("## :red[Data Collection Successfully Stored to MongoDB]") 

if selected == "Data Transformation":
    tab1 = st.tabs(["$\ Transform $"])
    st.markdown("#   ")
    st.write("## :red[Data Migration from MongoDB to SQL]")
    st.markdown("#    ")
    ch_names = []
    
    for i in my_collection.find():
        ch_names.append(i['channel details'][0]['channel_name'])
        
    c_names=st.selectbox('select the channel',options=ch_names)
    import_to_sql=st.button("### :green[Migrate to SQL]")
    st.write("Click the button to migrate data")


    if import_to_sql:
            create_tables()
            A=my_collection.find_one({'channel details.channel_name':c_names},{'_id':0})
            store_in_tables(A)
            st.write('### Migrated succcesfully')
    
if selected == "Queries":
    st.write("Select any question to get Insights")       
    Questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    if Questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videodetails ORDER BY channel_name;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    elif Questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name  , videocount AS TotalVideos FROM channeldetails ORDER BY videocount desc ;""")
        df   = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif Questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""select channel_name ,views, Title from videodetails order by views desc limit 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        
    elif Questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments FROM videodetails AS a  
                        LEFT JOIN (SELECT video_id,COUNT(comment_id) 
                        AS Total_Comments  FROM commentdetails GROUP BY video_id) AS b 
                        ON a.video_id = b.video_id  ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    elif Questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""select channel_name as channelname, title as Title , likes as Likescount 
                        from videodetails order by likes  desc limit 10;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif Questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""select likes as likecount ,title as videonames from videodetails limit 10;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    elif Questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""select views as totalviews, channel_name as channelnames from videodetails order by views limit 10;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### [Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif Questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name  FROM videodetails  WHERE published_date LIKE '2022%' 
                            GROUP BY channel_name ORDER BY channel_name;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
            
    elif Questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""select a.channel_name AS Channel_name,TIME_FORMAT(sec_to_time(avg(time_to_sec(Duration))),'%H:%i:%s') AS Average_Video_Duration_hrs 
                            from videodetails as a inner join 
                            channeldetails as b on a.channel_id=b.channel_id group by a.channel_name;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                            
        st.write(df)
        st.write("### [Average video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
            
    elif Questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mycursor.execute("""select channel_name AS channel_name,video_id AS Video_ID,comments AS comments
                                    FROM videodetails ORDER BY comments DESC LIMIT 10;""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Videos with most comments :]")
            fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
            st.plotly_chart(fig,use_container_width=True)
            


                    


        
     
