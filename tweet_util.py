import re

def get_hashtags_from_tweet(tweet):
    """
    get the hashtags from a tweet obejct
    :param tweet:tweet dictionary object
    :return: array of hashtags
    """
    tweet_text = tweet.get('doc').get('text')
    hashtags = re.findall(r" #(\S+) ",tweet_text)
    return hashtags



def get_coordinates_from_tweet(tweet):
    coordinates = []
    outer_coordinates = tweet.get('doc').get('coordinates')
    if outer_coordinates is None:
        return coordinates
    else:
        return tweet.get('doc').get('coordinates').get('coordinates')

