from mpi4py import MPI
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
    start += len(start_line)
    stop += len(stop_line)

    cur_pos = start
    twitter_file.seek(cur_pos, 0)
    # read file and partition it
    for line in twitter_file:
        cur_pos += len(line)
        if cur_pos > stop:
            break
        new_file.write(line)

    new_file.close()
