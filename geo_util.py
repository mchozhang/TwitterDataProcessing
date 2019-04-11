# -*- encoding: utf-8 -*-
#
# Utility to process geo data

import json
import sys
from collections import Counter


class GridManager(object):
    """
    manage and store important data of the grid map
    """

    def __init__(self, filename):
        # list of cell, in order of [A1,A2 ... D5]
        self.cell_list = []

        # hash table of the post number of a cell
        # in the form of {"A1": 100, "A2": 99 ... }
        self.posts_counter = Counter()

        # table of the hashtags counter of a cell
        # in the form of { "A2": {"melbourne": 100} }
        self.hashtags_counter_table = {}

        self.initial_grid(filename)

    def initial_grid(self, filename="melbGrid.json"):
        """
        read melbGrid file, initial grid data
        """
        try:
            with open(filename) as grid_file:
                grid = json.load(grid_file)
        except Exception as e:
            print("Failed to open melbGrid file.")
            sys.exit()

        for cell in grid.get('features'):
            properties = cell.get('properties')
            cell = dict()
            cell["name"] = properties.get('id')
            cell["xmin"] = properties.get('xmin')
            cell["xmax"] = properties.get('xmax')
            cell["ymin"] = properties.get('ymin')
            cell["ymax"] = properties.get('ymax')

            self.cell_list.append(cell)
            self.posts_counter[cell["name"]] = 0
            self.hashtags_counter_table[cell["name"]] = Counter()

    def collect_tweet(self, hashtags, coordinates):
        """
        collect a tweet with its hashtags and coordinates data
        :param hashtags: list of hashtags
        :param coordinates: list of coordinate, size of 2 or 0
        :return:
        """
        if len(coordinates) == 2:
            cell_name = self.locate_from_coordinates(coordinates)

            # found a cell for the tweet
            if cell_name is not None:
                # posts number counter
                self.posts_counter[cell_name] += 1

                # hashtags number counter
                self.hashtags_counter_table[cell_name].update(hashtags)

    def locate_from_coordinates(self, coordinates):
        """
        find corresponding cell of a point with its coordinates
        :param coordinates: X and Y value of a coordinate, in a list of size of 2
        :return: name of the cell
        """
        x = coordinates[0]
        y = coordinates[1]

        for cell in self.cell_list:
            if cell["xmin"] <= x <= cell["xmax"] and cell["ymin"] <= y <= cell["ymax"]:
                return cell["name"]
        return None


def sum_up_post_counter(posts_counter_list):
    """
    sum up the final posts result from the counter list
    :param posts_counter_list: list of posts counter result gathered from the communication domain
    :return: final posts counter
    """
    final_posts_counter = Counter()
    for counter in posts_counter_list:
        final_posts_counter.update(counter)
    return final_posts_counter


def sort_post_counter(counter):
    """
    sort the post counter in descendant order
    :param counter: post counter
    :return: list of tuples, in form of [(A1, 100), (A2, 90)]
    """
    posts_counter_list = sorted(counter.items(), key=lambda kv: kv[1])
    posts_counter_list.reverse()
    return posts_counter_list


def print_post_result(counter_list):
    """
    print the result of the counter in order
    :param counter_list: post counter list
    :return:
    """
    for cell_name, number in counter_list:
        print("{} : {} posts".format(cell_name, number))


def sum_up_hashtags_counter(post_counter, hashtags_counter_table_list):
    """
    sum up the hashtags counter
    :param post_counter: counter of posts of all the cell
    :param hashtags_counter_table_list: list of gathered hashtags counter table
    :return:
    """
    hashtags_counter_table = {name: Counter() for name in post_counter.keys()}
    for table in hashtags_counter_table_list:
        for cell_name, counter in table.items():
            hashtags_counter_table[cell_name].update(counter)
    return hashtags_counter_table


def print_hashtags_counter(hashtags_counter_table, posts_counter_list):
    """
    print the top 5 hashtags counter of every cell
    :param hashtags_counter_table: hashtag counter of all the cells
    :param posts_counter_list: sorted list of the posts counter of all the cells
    :return:
    """
    for cell_name, posts_number in posts_counter_list:
        # e.g. {"100", ["melb", "vic"], "99": ["baby"]}
        cell_result_table = dict()
        cell_value_rank = []

        for hashtag, number in hashtags_counter_table[cell_name].most_common():
            hashtag = "#" + hashtag
            if cell_result_table.get(number) is not None:
                cell_result_table[number].append(hashtag)
            elif len(cell_result_table.keys()) >= 5:
                break
            else:
                cell_value_rank.append(number)
                cell_result_table[number] = [hashtag]

        print_item = cell_name + " : "
        for number in cell_value_rank:
            print_item += "(" + ", ".join(cell_result_table[number]) + ", " + str(number) + "), "
        print(print_item[:-2])
