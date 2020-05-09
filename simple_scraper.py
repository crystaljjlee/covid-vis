

import tweepy
import datetime
import csv, codecs, cStringIO
from dateutil import parser


CONSUMER_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
CONSUMER_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
ACCESS_TOKEN = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
ACCESS_TOKEN_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'


def scrape_term(term, max_tweets=10000000, start_date='2016-01-01', end_date='2017-01-01'):
    '''
    This function scrapes up to max_tweets mentioning a particular term
    :param term: a search term that each scraped tweet needs to contain
    :param max_tweets: the maximum number of tweets to scrape
    :param start_date: the earliest date from which to scrape. note: this param is mostly used by scrape_term_by_day
    :param end_date: the last date from which to scrape. note: this param is mostly used by scrape_term_by_day
    :return A list of tweet dicts. Each dict represents a tweet and has the following keys:
        'tweet_id', 'date', 'text', 'author_id', 'author_name', 'author_screen_name', 'is_retweet', 'retweets_number'
    '''

    api = set_up_twitter_api()
    tweets = []

    for c, tweet in enumerate(tweepy.Cursor(api.search,
                                            q=term,
                                            rpp=100,
                                            since=start_date,
                                            until=end_date).items(max_tweets)):

        if (c+1) %100 == 0: print "{} tweets mentioning {} scraped.".format(c+1, term)

        tweets.append(tweet_to_dict(tweet))

    return tweets

def scrape_term_by_day(term, start_date='2016-09-06', end_date='2016-09-17', tweets_per_day=100):
    '''
    This function scrapes up to tweets_per_day tweets for each day between start_date and end_date
    Note: Twitter only allows searches for the last 10 days. Hence, start_date will usually be 10
    days ago and end_date will be today
    :param term: a search term that each scraped tweet needs to contain
    :param tweets_per_day: maximum number of tweets to scrape for each day
    :param start_date: format: "YYYY-MM-DD", usually set to 10 days ago
    :param end_date: format: "YYYY-MM-DD", usually set to today
    :return A list of tweet dicts. Each dict represents a tweet and has the following keys:
        'tweet_id', 'date', 'text', 'author_id', 'author_name', 'author_screen_name', 'is_retweet', 'retweets_number'
    '''

    dt_start = parser.parse(start_date)
    dt_end = parser.parse(end_date)

    tweets = []
    while(dt_start <= dt_end):

        date_string_start = "{}-{}-{}".format(dt_start.year, dt_start.month, dt_start.day)
        date_string_end = "{}-{}-{}".format(dt_start.year, dt_start.month, dt_start.day+1)
        tweets_of_date = scrape_term(term,
                                      max_tweets=tweets_per_day,
                                      start_date=date_string_start,
                                      end_date=date_string_end)

        print "Scraped {} tweets mentioning {} from {}".format(len(tweets_of_date), term, date_string_start)
        tweets = tweets + tweets_of_date
        dt_start += datetime.timedelta(days=1)

    return tweets



def store_tweets_to_csv(tweets, filename='scraped_tweets.csv'):
    '''
    This function stores a list of tweets (produced by scrape_term or scrape_term_by_day)
    in a csv file
    :param tweets: a list of tweets produced by scrape_term or scrape_term_by_day
    :param filename: the name of the csv file that you want to create. can also be a path
    :return Nothing, but creates a csv of a list of tweets
    '''

    print "Storing {} tweets in {}".format(len(tweets), filename)

    with open(filename, 'wb') as csvfile:
        csvwriter = UnicodeWriter(csvfile)
        csvwriter.writerow(['tweet_id', 'date', 'author_id', 'author_name', 'author_screen_name',
                            'is_retweet', 'retweets_number', 'text'])
        for tweet in tweets:
            try:
                csvwriter.writerow([tweet['tweet_id'], tweet['date'], tweet['author_id'], tweet['author_name'],
                                tweet['author_screen_name'], tweet['is_retweet'], tweet['retweets_number'], tweet['text']])
            except AttributeError, e:
                print "err", e, tweet


def set_up_twitter_api():
    '''
    This function initializes the twitter api
    Note: CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET need to be already set
    '''

    # Set up Twitter API
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    return api

def is_retweet(tweet):
    '''
    Returns true if a tweet is a retweet, false if it does not
    '''

    # A tweet is a retweet if it has the attribute "retweeted_status"
    # -> find out if it has that attribute
    try:
        _ = tweet.retweeted_status
        return True
    except AttributeError:
        return False

def tweet_to_dict(tweet):
    '''
    Parses a tweepy tweet to a dict for the csv
    Extracts tweet id, text, date, author_id, author name, author screen name, retweets, is_retweet
    '''

    tweet_dict = {
        'tweet_id': str(tweet.id),
        'text': tweet.text,
        'date': str(datetime.datetime.date(tweet.created_at)),
        'author_id': str(tweet.author.id),
        'author_name': tweet.author.name,
        'author_screen_name': tweet.author.screen_name,
        'retweets_number': str(tweet.retweet_count),
        'is_retweet': str(is_retweet(tweet))
    }
    return tweet_dict

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    Copied from https://docs.python.org/2/library/csv.html
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def tutorial():

    ###############################################
    ##          HOW TO USE THIS SCRAPER          ##
    ###############################################


    # To scrape tweets mentioning a certain term, use:
    tweets_1 = scrape_term('@realdonaldtrump', max_tweets=100)

    # To scrape more tweets, increase the max_tweets variable
    # Note: Twitter limits the number of tweets that you can scrape per hour.
    # Hence, it may seem as if the script is stuck. In that case, either
    # wait or reduce the number of tweets to be scraped
    tweets_2 = scrape_term('@realdonaldtrump', max_tweets=1000)

    # To scrape 100 tweets for each of the last 10 days, use:
    # Note: Twitter only allows you to scrape tweets from the last 10 days
    # Earlier dates simply return no tweets
    # Again, you can increase the number of tweets scraped per day with
    # the tweets_per_day parameter.
    tweets_3 = scrape_term_by_day('@realdonaldtrump',
                                start_date='2016-09-06',
                                end_date = '2016-09-16',
                                tweets_per_day=100)

    # To store a collection of tweets, pass the list and a filename
    # to store_tweets_to_csv:
    store_tweets_to_csv(tweets_3, 'trump.csv')

if __name__=='__main__':

    tutorial()

    # Sample usage (uncomment to run)
    # tweets = scrape_term_by_day('@realdonaldtrump', start_date='2016-09-06', end_date = '2016-09-16', tweets_per_day=100)
    # store_tweets_to_csv(tweets, 'trump.csv')
