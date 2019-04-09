from mpi4py import MPI
from geo_util import GridManager
import tweet_util
import json
from collections import Counter
import sys
import getopt


twitter_filepath = "bigTwitter.json"
melb_filepath = "melbGrid.json"
opts, args = getopt.getopt(sys.argv[1:], "t:m:")

for op, value in opts:
    if op == "-t":
        twitter_filepath = value
    elif op == "-m":
        melb_filepath = value

# initial grid and twitter file info
grid_manager = GridManager(melb_filepath)
twitter_file_size = tweet_util.get_file_size(twitter_filepath)

# mpi info
comm = MPI.COMM_WORLD
process_number = comm.size
rank = comm.rank

# compute the start and stop point
start = (twitter_file_size // process_number) * rank
stop = (twitter_file_size // process_number) * (rank + 1)

with open(twitter_filepath) as twitter_file:

    # trim each start line and stop line
    twitter_file.seek(start)
    start_line = twitter_file.readline()

    twitter_file.seek(stop)
    stop_line = twitter_file.readline()

    start += len(start_line)
    stop += len(stop_line)

    cur_pos = start
    twitter_file.seek(cur_pos)

    for line in twitter_file:
        cur_pos += len(line)
        if cur_pos > stop:
            break

        try:
            line = line.strip()

            if len(line) < 3:
                continue

            if line.endswith(","):
                line = line[:-1]

            tweet = json.loads(line)

            hashtags = tweet_util.get_hashtags_from_tweet(tweet)
            coordinates = tweet_util.get_coordinates_from_tweet(tweet)

            grid_manager.collect_tweet(hashtags, coordinates)

        except Exception as e:
            print("exception line:" + line)
            print(e)

# gather the posts counter result
posts_counter = grid_manager.posts_counter
posts_counter_list = comm.gather(posts_counter, root=0)

# gather the hashtags counter result
hashtags_counter_table = grid_manager.hashtags_counter_table
hashtags_counter_table_list = comm.gather(hashtags_counter_table, root=0)

if rank == 0:
    # add up all the post counter result
    final_posts_counter = Counter()
    for counter in posts_counter_list:
        final_posts_counter.update(counter)

    # sort the posts number
    final_posts_counter_list = sorted(final_posts_counter.items(), key=lambda kv: kv[1])
    final_posts_counter_list.reverse()
    print(str(final_posts_counter_list))

    # add up all the hashtag counter result
    final_hashtags_counter_table = {name: Counter() for name in hashtags_counter_table.keys()}
    for table in hashtags_counter_table_list:
        for cell_name, counter in table.items():
            final_hashtags_counter_table[cell_name].update(counter)

    for cell_name, number in final_posts_counter_list:
        print(cell_name + " " + str(final_hashtags_counter_table[cell_name].most_common(5)))
