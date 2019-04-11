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



with open(filename,"rb") as twitter_file:
    new_filename = "part_{}.json".format(rank)
    new_file = open(new_filename, "wb")
    # trim each start line and stop line
    twitter_file.seek(start, 0)
    start_line = twitter_file.readline()
    twitter_file.seek(stop, 0)
    stop_line = twitter_file.readline()

    start += len(start_line) # start cursor at head of next line
#    print(rank,": the start position: ",start,twitter_file.readline())
    stop += len(stop_line)-1 # stop cursor at tail of this line
#    print(rank, ": the stop position: ", stop)
#    print("\n process",rank,"\n this is the start line \n", twitter_file.readline()[0:50])
#    twitter_file.seek(stop, 0)

#    print("\n process",rank,"\n this is the end line \n", twitter_file.readline()[0:50])
    cur_pos = start
    twitter_file.seek(cur_pos, 0)
    # read file and partition it
    print("curstart",cur_pos)
    for line in twitter_file:
        # if rank == 0 and cur_pos <30000:
        #     print("process: ",rank,":cur_pos:",cur_pos,"stop:",stop,line[0:30])
        cur_pos += len(line)
        new_file.write(line)
        if cur_pos > stop:
                #            print("process: ", rank,"\n this is super line \n",line[0:50])
            break


    # while(cur_pos<stop):
    #     line = twitter_file.readline()
    #     cur_pos += len(line)-1
    #     # print("process: ", rank,"\n this is super line \n",line[0:50]
    #     new_file.write(line)
    #     twitter_file.seek(cur_pos,0)
    new_file.close()
