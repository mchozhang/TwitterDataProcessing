# -*- encoding: utf-8 -*-
#
# Utility to parse tweet data from json object

import re


def get_hashtags_from_tweet(tweet):
    """
    get the list of hashtags from a tweet json object,
    hashtags are in lowercase
    :param tweet:tweet json object
    :return: list of hashtags
    """
    tweet_text = tweet.get('doc').get('text')
    hashtags = re.findall(r" #(\S+) ", tweet_text)
    return [hashtag.lower() for hashtag in hashtags]


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
