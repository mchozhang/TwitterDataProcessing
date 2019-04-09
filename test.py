from mpi4py import MPI
import tweet_util
import json
import os
import sys

comm = MPI.COMM_WORLD
process_number = comm.size
rank = comm.rank

# get twitter json file
filename = "123.json"
if len(sys.argv) == 2:
    filename = sys.argv[1]

# initial json file information
file_size = 0
try:
    file_size = os.path.getsize(filename)  # get the size of twitter file in bytes
except Exception as e:
    print(str(e))

start = (file_size // process_number) * rank
stop = (file_size // process_number) * (rank + 1)



with open(filename) as twitter_file:
    new_filename = "part_{}.json".format(rank)
    new_file = open(new_filename, "w+")
    # trim each start line and stop line
    twitter_file.seek(start, 0)
    start_line = twitter_file.readline()

    twitter_file.seek(stop, 0)
    stop_line = twitter_file.readline()

    start += len(start_line)+1 # start cursor at head of next line
    stop += len(stop_line)-1 # stop cursor at tail of this line
    twitter_file.seek(start, 0)
    print("\n","this is the start line \n", twitter_file.readline())
    twitter_file.seek(stop, 0)
   # print("\n this is the end line \n", twitter_file.readline())
    cur_pos = start
    twitter_file.seek(cur_pos, 0)
    # read file and partition it
    for line in twitter_file:
        cur_pos += len(line)-1
        if cur_pos > stop:
      #      print("\n this is super line \n",line)
            break
        new_file.write(line)
    new_file.close()
