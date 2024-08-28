import praw 
import time
import csv
from datetime import datetime

reddit = praw.Reddit(
    client_id='client_id',
    client_secret='client_secret',
    user_agent='RedditAnalyzer (by u/blank)'
)



subreddit = reddit.subreddit('Tekken')

start = datetime(2023,10,1)
end = datetime(2024,8,30)


startTimeStamp = int(time.mktime(start.timetuple()))
endTimeStamp = int(time.mktime(end.timetuple()))

with open('tekken_data.csv',mode='w',newline='',encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Title','Score','Created_UTC','Upvote_Ratio', 'Number_of_Comments','Url', 'Author','Flair'])

    for post in subreddit.top(limit=None):
        if startTimeStamp <= post.created_utc <= endTimeStamp and post.link_flair_text:
            writer.writerow([post.title, post.score, post.created_utc, post.upvote_ratio, post.num_comments,post.url + ' ', str(post.author),post.link_flair_text])
            print(post.title)
print("Data has been saved to 'tekken_data.csv'")