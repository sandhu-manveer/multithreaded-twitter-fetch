import sys
import requests
import json
import twitter
import csv
import threading
from threading import Thread
try:
  import Queue
except:
  import queue as Queue

def convert_status_to_pi_content_item(s):
  return { 
      'userid': str(s.user.screen_name), 
      'id': str(s.id), 
      'sourceid': 'python-twitter', 
      'contenttype': 'text/plain', 
      'language': s.lang, 
      'content': s.text,
      'reply': (s.in_reply_to_status_id == None),
      'forward': False
  }

def multithreaded_twitter_fetch(keys, handle, no):
  """Fetches tweets
    :param List keys: Twitter api keys
    :param str handle: Twitter handle
    :param int no: Number of tweets to fetch for handle
  """
  handles = Queue.Queue()
  list = [handles.put(h) for h in handle]

  results = {}
  threadlist = []

  def thread_job(handle, key, results, no):
      twitter_api = twitter.Api(consumer_key=key["consumerKey"],
                      consumer_secret=key["consumerSecret"],
                      access_token_key=key["accessToken"],
                      access_token_secret=key["accessSecret"],
                      debugHTTP=True)

      alltweets = []
      max_tweets = no
      new_tweets = twitter_api.GetUserTimeline(screen_name = handle,
                                              count=max_tweets,
                                              include_rts=False)
      alltweets.extend(new_tweets)
      oldest = alltweets[-1].id - 1
      lim = len(alltweets)

      while(len(alltweets) < max_tweets):
        alltweets.extend(twitter_api.GetUserTimeline(screen_name = handle,
                                                    count=(max_tweets-len(alltweets)),
                                                    include_rts=False,
                                                    max_id=oldest))
        oldest = alltweets[-1].id - 1
        if lim == len(alltweets):
          break
        lim = len(alltweets)

      pi_content_items_array = map(convert_status_to_pi_content_item, alltweets)
      pi_content_items = { 'contentItems' : pi_content_items_array }

      results[handle] = pi_content_items

  def threader(key, results, no,):
    while True:
      handle = handles.get()
      thread_job(handle, key, results, no)
      handles.task_done()

  for key in keys:
    t = Thread(target=threader, args=(key, results, no,))
    t.daemon = True
    t.start()
    threadlist.append(t)

  handles.join()

  return results

if __name__ == '__main__':
  f = open('keys.json', 'r') # save keys as json
  keys = json.load(f)

  file_contents = open('handles.txt').read().decode("utf-8") # save handles in txt file, line separated
  handles = file_contents.replace('@','').splitlines()

  results = multithreaded_twitter_fetch(keys, handles, 20)

  print results