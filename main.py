from mpi4py import MPI
import geo_util
import tweet_util
import json
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
grid_manager = geo_util.GridManager(melb_filepath)
twitter_file_size = tweet_util.get_file_size(twitter_filepath)

# mpi info
comm = MPI.COMM_WORLD
process_number = comm.size
rank = comm.rank

# compute the start and stop byte
start = (twitter_file_size // process_number) * rank
stop = (twitter_file_size // process_number) * (rank + 1)

with open(twitter_filepath, 'rb') as twitter_file:

    # trim each start line and stop line
    twitter_file.seek(start, 0)
    start_line = twitter_file.readline()

    twitter_file.seek(stop, 0)
    stop_line = twitter_file.readline()

    start += len(start_line)
    stop += len(stop_line) - 1

    cur_pos = start
    twitter_file.seek(cur_pos, 0)

    for line in twitter_file:

        cur_pos += len(line)
        line = line.decode("utf-8").strip()
        try:
            # remove "," at the end of the line
            if line.endswith(","):
                line = line[:-1]

            tweet = json.loads(line)

            hashtags = tweet_util.get_hashtags_from_tweet(tweet)
            coordinates = tweet_util.get_coordinates_from_tweet(tweet)

            grid_manager.collect_tweet(hashtags, coordinates)

        except Exception as e:
            continue

        if cur_pos > stop:
            break

# gather the posts counter result
posts_counter = grid_manager.posts_counter
posts_counter_list = comm.gather(posts_counter, root=0)

# gather the hashtags counter result
hashtags_counter_table = grid_manager.hashtags_counter_table
hashtags_counter_table_list = comm.gather(hashtags_counter_table, root=0)

if rank == 0:
    # add up all the post counter result, in form of { "A1": 123, "A2": 321 }
    final_posts_counter = geo_util.sum_up_post_counter(posts_counter_list)

    # sort in tuple
    final_posts_counter_list = geo_util.sort_post_counter(final_posts_counter)

    # print the posts counter result
    geo_util.print_post_result(final_posts_counter_list)

    # add up all the hashtag counter result
    final_hashtags_counter_table = geo_util.sum_up_hashtags_counter(final_posts_counter, hashtags_counter_table_list)

    # print the top 5 hashtags counter of every cell
    geo_util.print_hashtags_counter(final_hashtags_counter_table, final_posts_counter_list)
