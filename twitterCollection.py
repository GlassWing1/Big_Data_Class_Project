import codecs
import tweepy
import json
import datetime

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
consumer_key="SSUEH450RBqhlNkAX9Mkng"
consumer_secret="EOSZkc2Cv3VFewnfJGgW9mnvJdnQ7ppngO28GSoELE"

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
access_token="1915208858-Jm5kK4el1M9mKWgk910n5mpIJkUmfuFdatpjTa5"
access_token_secret="pufkd8T4rZvpFWHllH4IBspJBwqdLdRufGbkbuxYWo"

# filters
keywords = {'flood','natural disaster','evacuate','flood releaf', 'Colorado flood', '100 year flood','boulder flood', 'boulderflood'}
language = ['en']

count = 0

#open file to write to
f = codecs.open('twitter.txt',encoding='utf-8', mode='w+')

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
This is a basic listener that just prints received tweets to stdout.

"""

    def on_data(self, data):
        global count
        count = count + 1

        # Load the Tweet into the variable "t"
        t = json.loads(data)

        # Pull important data from the tweet to store in the database.
        tweet_id = t['id_str'] # The Tweet ID from Twitter in string format
        username = t['user']['screen_name'] # The username of the Tweet author
        followers = t['user']['followers_count'] # The number of followers the Tweet author has
        text = t['text'] # The entire body of the Tweet
        hashtags = t['entities']['hashtags'] # Any hashtags used in the Tweet
        dt = t['created_at'] # The timestamp of when the Tweet was created
        language = t['lang'] # The language of the Tweet

        # Convert the timestamp string given by Twitter to a date object called "created". This is more easily manipulated in MongoDB.
        created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

        # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
        tweet = {'id':tweet_id, 'username':username, 'followers':followers, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created}

        # Optional - Print the username and text of each Tweet to your console or write to a file in realtime as they are pulled from the stream
        print count
        print username + ':' + ' ' + text + '\n'
        f.write(username + ':' + ' ' + text + '\n\n')

        if count == 100:
            return False
        else:
            return True
    
    def on_error(self, status):
        print status


if __name__ == '__main__':
    
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=keywords, languages=language)

    f.close()
