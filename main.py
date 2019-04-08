from mpi4py import MPI
from geo_util import GridManager
import tweet_util
import json
import os
from collections import Counter
import sys


comm = MPI.COMM_WORLD
process_number = comm.size
rank = comm.rank

grid_manager = GridManager()

# get twitter json file
filename = "tinyTwitter.json"
if len(sys.argv) == 2:
    filename = sys.argv[1]

# initial json file information
file_size = 0
try:
    file_size = os.path.getsize(filename)
except Exception as e:
    print(str(e))

start = (file_size // process_number) * rank
stop = (file_size // process_number) * (rank + 1)
if rank + 1 == process_number:
    stop += file_size % process_number

with open(filename) as twitter_file:

    cur_pos = start
    twitter_file.seek(start)

    # skip first line
    first_line = twitter_file.readline()
    cur_pos += len(first_line)
    for line in twitter_file:
        cur_pos += len(line)
        try:
            line = line.strip()
            if line == "]}":
                break
            if line.endswith(","):
                line = line[:-1]

            tweet = json.loads(line)

            hashtags = tweet_util.get_hashtags_from_tweet(tweet)
            coordinates = tweet_util.get_coordinates_from_tweet(tweet)

            grid_manager.collect_tweet(hashtags, coordinates)

            if cur_pos > stop:
                break
        except Exception as e:
            print(e)

# gather the posts counter result
posts_table = grid_manager.posts_table
posts_table_list = comm.gather(posts_table, root=0)

# gather the hashtags counter result
hashtags_table_dict = grid_manager.hashtags_table_dict
hashtags_table_list = comm.gather(hashtags_table_dict, root=0)

if rank == 0:
    posts_counter_dict = {name: 0 for name in posts_table.keys()}
    for table in posts_table_list:
        for name, hashtag_table in table.items():
            posts_counter_dict[name] += hashtag_table
    posts_counter_list = sorted(posts_counter_dict.items(), key=lambda kv: kv[1])
    posts_counter_list.reverse()
    print(str(posts_counter_list))

    hashtags_counter_dict = {name: Counter() for name in hashtags_table_dict.keys()}
    for table in hashtags_table_list:
        for cell_name, hashtag_table in table.items():
            for hashtag, number in hashtag_table.items():
                hashtags_counter_dict[cell_name][hashtag] += number

    for cell_name, number in posts_counter_list:
        print(cell_name + " " + str(hashtags_counter_dict[cell_name].most_common(5)))
