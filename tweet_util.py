import re

def get_hashtags_from_tweet(tweet):
    """
    get the hashtags from a tweet json object
    :param tweet:tweet json object
    :return: list of hashtags
    """
    tweet_text = tweet.get('doc').get('text').lower()
    tweet_text = tweet_text.lower()
    hashtags = re.findall(r" #(\S+) ",tweet_text)
    return hashtags



def get_coordinates_from_tweet(tweet):
    """
    get the coordinates from a tweet json object
    :param tweet: tweet json object
    :return: coordinates in list of size of 2
    """
    outer_coordinates = tweet.get('doc').get('coordinates')
    if outer_coordinates is None:
        return []
    else:
        return tweet.get('doc').get('coordinates').get('coordinates')

