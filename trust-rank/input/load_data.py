from argparse import ArgumentParser
import os
import csv
import json
import numpy as np
from scipy import sparse
import unicodecsv


def load_csv_spark(sc, path):

    csv_rdd = sc.textFile(path)
    csv_header = sc.broadcast(csv_rdd.first().split(',')).value
    csv_rdd = csv_rdd.filter(lambda line: line != ','.join(csv_header))
    csv_rdd = csv_rdd.map(lambda x: unicodecsv.DictReader(iter([x.encode('utf-8')]), csv_header).next())

    return csv_rdd

def load_votes(csv_file, user_pos=0, item_pos=1, vote_pos=2, skip_lines = 1):

    rows,cols = [], []
    data = []
    items = {}
    users = {}

    def update_counts(obj, objs, counter):
        if obj not in objs:
            objs[obj] = counter
            counter += 1
        return objs, counter, objs[obj]

    with open(csv_file,'rU') as f:
        item_counter = 0
        user_counter = 0
        reader = csv.reader(f, delimiter=',')
        for ii,line in enumerate(reader):
            if ii < skip_lines:
                continue
            user = line[user_pos]
            item = line[item_pos]
            vote = line[vote_pos]

            items, item_counter, item_idx = update_counts(item, items, item_counter)
            users, user_counter, user_idx = update_counts(user, users, user_counter)

            rows.append(user_idx)
            cols.append(item_idx)
            data.append(int(vote))

    votes_mat = sparse.csc_matrix( (data,(rows, cols)), shape=(user_counter, item_counter), dtype=float)

    return votes_mat, items, users
