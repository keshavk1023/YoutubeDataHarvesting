# Import necessary libraries
import streamlit as st  # A Python library for creating web apps
import pandas as pd  # A library for data manipulation and analysis
from pprint import pprint

# Import libraries for Google Sheets API
import googleapiclient.discovery

# Set up API credentials
api_key = 'AIzaSyAh_ajCgEPccQR1DgsateTWWmYbXLAcYIY'
api_service_name = "youtube"
api_version = "v3"

# Build the YouTube API service
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Import MySQL connector library
import mysql.connector

# Connect to MySQL database
conn = mysql.connector.connect(host='localhost', username='root', password='Ke$hw0rd-12345', database='youtube_harvest')
cursor = conn.cursor()
conn.commit()

# Create necessary database tables if they don't exist
cursor.execute('''create table if not exists channel_detail(channel_name VARCHAR(225),
                                                            publish_at VARCHAR(225),
                                                            playlist_id VARCHAR(225),
                                                            sub_count VARCHAR(225),
                                                            vid_count VARCHAR(225),
                                                            views VARCHAR(225))''')
conn.commit()

cursor.execute('''create table if not exists video_ids(video_id VARCHAR(150))''')
conn.commit()

cursor.execute('''create table if not exists video_detail(channel_name VARCHAR(225),
                                                           video_id VARCHAR(225),
                                                           title VARCHAR(225),
                                                           published_date VARCHAR(225),
                                                           duration VARCHAR(225),
                                                           views VARCHAR(225),
                                                           likes VARCHAR(225),
                                                           comments VARCHAR(225)                                      
                                                            )''')
conn.commit()

cursor.execute('''create table if not exists comment_detail(comment_Text text,
                                                            comment_Author VARCHAR(225),
                                                            comment_Published VARCHAR(225),
                                                            video_id VARCHAR(225))''')
conn.commit()


def get_channel_detail(channel_id):
    """
    Retrieve details of a YouTube channel using its channel ID.
    
    Parameters:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        dict: A dictionary containing details of the channel.
    """
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    channel_data = {
        "channel_name": response['items'][0]['snippet']['title'],
        "publish_at": response['items'][0]['snippet']['publishedAt'],
        "playlist_id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "sub_count": response['items'][0]['statistics']['subscriberCount'],
        "vid_count": response['items'][0]['statistics']['videoCount'],
        "views": response['items'][0]['statistics']['viewCount']
    }

    return channel_data


def get_videos_ids(channel_id):
    """
    Retrieve video IDs of all videos uploaded to a YouTube channel.
    
    Parameters:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        list: A list containing video IDs.
    """
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_detail(video_ids):
    """
    Retrieve details of YouTube videos using their video IDs.
    
    Parameters:
        video_ids (list): A list containing video IDs.
        
    Returns:
        list: A list of dictionaries containing details of each video.
    """
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id,
        )
        response = request.execute()
        for item in response["items"]:
            data = {"channel_name": item['snippet']['channelTitle'],
                    "video_id": item['id'],
                    "title": item['snippet']['title'],
                    "published_date": item['snippet']['publishedAt'],
                    "duration": item['contentDetails']['duration'],
                    "views": item['statistics'].get('viewCount'),
                    "likes": item['statistics'].get('likeCount'),
                    "comments": item['statistics'].get('commentCount')
                    }
            video_data.append(data)
    return video_data


def get_comment_detail(video_ids):
    """
    Retrieve details of comments on YouTube videos using their video IDs.
    
    Parameters:
        video_ids (list): A list containing video IDs.
        
    Returns:
        list: A list of dictionaries containing details of each comment.
    """
    comment_data = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()

        for i in range(len(response['items'])):
            data = {
                "comment_Text": response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                "comment_Author": response['items'][i]['snippet']['topLevelComment']['snippet'][
                    'authorDisplayName'],
                "comment_Published": response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                "video_id": response['items'][i]['snippet']['topLevelComment']['snippet']['videoId']
            }

            comment_data.append(data)
    return comment_data


def channel_tables(channel_id):
    """
    Fetch channel details and insert them into the 'channel_detail' table.
    
    Parameters:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        dict: A dictionary containing channel details.
    """
    channel_data = get_channel_detail(channel_id)
    # To insert the channel data in its respective SQL table :
    cursor = conn.cursor()

    sql_ch = '''INSERT INTO channel_detail(channel_name ,
                                        publish_at ,
                                        playlist_id ,
                                        sub_count ,
                                        vid_count,
                                        views) VALUES (%s,%s,%s,%s,%s,%s)'''
    val_ch = tuple(channel_data.values())
    cursor.execute(sql_ch, val_ch)
    conn.commit()
    return channel_data


def video_tables(channel_id):
    """
    Fetch video details and insert them into the 'video_detail' table.
    
    Parameters:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        list: A list of dictionaries containing video details.
    """
    video_ids = get_videos_ids(channel_id)
    video_data = get_video_detail(video_ids)

    # To insert the video data into its respective SQL table :
    vid_detail = []
    for i in video_data:
        vid_detail.append(tuple(i.values()))
    cursor = conn.cursor()

    sql_vi = '''INSERT INTO video_detail(channel_name ,
                                        video_id ,
                                        title ,
                                        published_date ,
                                        duration ,
                                        views ,
                                        likes ,
                                        comments) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

    val_vi = vid_detail
    cursor.executemany(sql_vi, val_vi)
    conn.commit()
    return video_data


def comment_tables():
    """
    Fetch comment details and insert them into the 'comment_detail' table.
    
    Returns:
        list: A list of dictionaries containing comment details.
    """
    video_ids = get_videos_ids(channel_id)
    comment_data = get_comment_detail(video_ids)
    # To insert the comment data into its respective SQL table ;
    comment_detail = []
    for i in comment_data:
        comment_detail.append(tuple(i.values()))
    cursor = conn.cursor()

    sql_co = '''INSERT INTO comment_detail(comment_Text,
                                        comment_Author,
                                        comment_Published,
                                        video_id) VALUES (%s,%s,%s,%s)'''
    val_co = comment_detail
    cursor.executemany(sql_co, val_co)
    conn.commit()

    return comment_data


# Streamlit app setup
st.title(":red[YOUTUBE DATA HARVESTING & WAREHOUSING]")
st.divider()

# Input channel ID
channel_id = str(st.text_input("ENTER CHANNEL ID: "))
options = ['select option', 'show channel details', 'show video details', 'show comments']

# Sidebar setup
with st.sidebar:
    st.header(":red[SKILLS]")
    st.subheader("Python Scripting")
    st.subheader("API integration")
    st.subheader("Data Collection")
    st.subheader("SQL")
    st.subheader("Pandas and dataframe")

with st.sidebar:
    st.header(":orange[SELECT THE TABLE]")
    selected = st.selectbox('👇', options=options)

# Main content
if selected == options[0]:
    st.write('')

if selected == options[1]:
    data = channel_tables(channel_id)
    st.dataframe(data)

if selected == options[2]:
    data = video_tables(channel_id)
    st.dataframe(data)

if selected == options[3]:
    data = comment_tables()
    st.dataframe(data)

st.divider()
st.subheader(":orange[SELECT THE QUERY]")
question = st.selectbox("Select your question", ("select the query",
                                                  "1. All the videos and the channel name",
                                                  "2. channels with most number of videos",
                                                  "3. 10 most viewed videos",
                                                  "4. comments in each videos",
                                                  "5. Videos with highest likes",
                                                  "6. likes of all videos",
                                                  "7. views of each channel",
                                                  "8. videos published in the year of 2022",
                                                  "9. average duration of all videos in each channel",
                                                  "10. videos with highest number of comments"))

if question == "select the query":
    pass

if question == "1. All the videos and the channel name":
    query1 = '''select title as videos,channel_name as channelname from video_detail'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif question == "2. channels with most number of videos":
    query2 = '''select channel_name as channelname,vid_count as no_videos from channel_detail
                order by vid_count desc'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from video_detail
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. comments in each videos":
    query4 = '''select comments as no_comments,title as videotitle from video_detail where comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question == "5. Videos with highest likes":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                from video_detail where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from video_detail'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. views of each channel":
    query7 = '''select channel_name as channelname ,views as totalviews from channel_detail'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question == "8. videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videoRelease,channel_name as channelname from video_detail
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as AverageDuration from video_detail group by channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "AverageDuration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["AverageDuration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question == "10. videos with highest number of comments":
    query10 = '''select title as videotitle, channel_name as channelname,comments as comments from video_detail where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)