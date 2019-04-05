from itertools import islice
from mpi4py import MPI
import time
import json

comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

print("rank {}, size {}".format(rank, size))

LINE_NUMBER = 1000

start = (LINE_NUMBER // size) * rank + 1
stop = (LINE_NUMBER // size) * (rank + 1) + 1
print(stop)
if rank + 1 == size:
    stop += LINE_NUMBER % size

print("start:{}  stop:{}".format(start, stop))

t1 = time.time()
with open("tinyTwitter.json") as file:
    filename = "part_{}.json".format(rank)
    new_file = open(filename, 'w+')
    for line in islice(file, start, stop, None):
        line = line.strip()
        if line.endswith(","):
            line = line[:-1]

        tweet = json.loads(line)
        # hashtags
        hashtags = tweet.get("doc").get("entities").get("hashtags")
        tags = []
        for hashtag in hashtags:
            tags.append(hashtag.get('text'))

        # coordinates
        a = tweet.get('doc').get('coordinates')
        coordinates = []
        if a is not None:
            coordinates = a.get("coordinates")

        item = {}
        item["hashtags"] = tags
        item["coordinates"] = coordinates
        new_file.write(str(item) + "\n")
    new_file.close()

t2 = time.time()
print(t2 - t1)
