from mpi4py import MPI
from geo_util import GridManager
import json
from itertools import islice

comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

print("rank {}, size {}".format(rank, size))

grid_manager = GridManager()
grid_manager.initial_grid()
grid_manager.print_grid()

if rank == 0:

    offset = 0
    with open("tinyTwitter.json") as twitter_file:
        twitter_file.readline()
        for i, line in enumerate(twitter_file):
            try:
                line = line.strip()
                if line == "]}":
                    break
                if line.endswith(","):
                    line = line[:-1]

                tweet = json.loads(line)

                # hashtags
                hashtags = tweet.get('doc').get("entities").get("hashtags")
                # if len(hashtags) == 0:
                #     print("no hash tag")
                # else:
                #     tags_str = ""
                #     for hashtag in hashtags:
                #         tags_str += hashtag.get('text') + " "
                #     print(tags_str)

                # coordinates
                a = tweet.get('doc').get('coordinates')
                if a is None:
                    print("a is None")
                else:
                    coordinates = tweet.get('doc').get('coordinates').get('coordinates')
                    if len(coordinates) != 2:
                        print("out of bounds!!!!")
                    if coordinates[0] < 144.7 or coordinates[0] > 145.3 \
                            or coordinates[1] < -38.1 or coordinates[1] > -37.50:
                        print("out of melbourne!!!")
                    # print("{} {} {}".format(i, coordinates[0], coordinates[1]))
            except Exception as e:
                print(e)
