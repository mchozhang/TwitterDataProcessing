# -*- encoding: utf-8 -*-
#
# Utility to process geo data

import json


class Cell(object):
    """
    a cell in the grid
    """

    def __init__(self):
        self.xmin = 0
        self.xmax = 0
        self.ymin = 0
        self.ymax = 0

        self.name = ""

        # hash table of the presence number of the hashtags
        # in the form of <hashtag, number>
        self.hashtags_map = {}


class GridManager(object):
    """
    manage and store import data of the grid map
    """

    def __init__(self):
        self.xmin = 0
        self.xmax = 0
        self.ymin = 0
        self.ymax = 0

        # list of cells
        self.grid = []

        #  hash table of the post number of a cell
        # in the form of <cell_name, post number>
        self.posts_table = {}

    def initial_grid(self):
        """
        read melbGrid file, initial grid data
        """
        with open("melbGrid.json") as grid_file:
            grid = json.load(grid_file)

        for cell in grid.get('features'):
            properties = cell.get('properties')
            cell = Cell()
            cell.name = properties.get('id')
            cell.xmin = properties.get('xmin')
            cell.xmax = properties.get('xmax')
            cell.ymin = properties.get('ymin')
            cell.ymax = properties.get('ymax')

            if cell.xmin < self.xmin:
                self.xmin = cell.xmin

            if cell.xmax > self.xmax:
                self.xmax = cell.xmax

            if cell.ymin < self.ymin:
                self.ymin = cell.ymin

            if cell.ymax > self.ymax:
                self.ymax = cell.ymax

            self.grid.append(cell)

    def append_tweet(self, hashtags, coordinates):
        """
        append a tweet to the record
        :param hashtags: list of hashtags
        :param coordinates: list of coordinate, size of 2 or 0
        :return:
        """
        if len(coordinates) == 2:
            cell_name = self.get_cell_name_from_coordinates(coordinates)
            if cell_name is not None:
                self.posts_table[cell_name] += self.posts_table[cell_name]

    def get_cell_name_from_coordinates(self, coordinates):


    def print_grid(self):
        """
        print every cell's name and coordinates
        """
        for cell in self.grid:
            print("{} {} {} {} {}".format(cell.name, cell.xmin, cell.xmax, cell.ymin, cell.ymax))
