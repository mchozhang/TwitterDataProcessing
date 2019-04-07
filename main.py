from mpi4py import MPI
from geo_util import GridManager
import tweet_util
import json
import os
import sys


comm = MPI.COMM_WORLD
process_number = comm.size
rank = comm.rank

grid_manager = GridManager()
grid_manager.initial_grid()

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
    new_filename = "part_{}.json".format(rank)
    new_file = open(new_filename, "w+")
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

            if cur_pos > stop:
                break
        except Exception as e:
            print(e)
