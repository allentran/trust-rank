from argparse import ArgumentParser

import os
import csv
import json
import numpy as np
from scipy import sparse                       
from operator import add

def load_csv_spark(sc, path):

    csv_rdd = sc.textFile(path)
    csv_header = sc.broadcast(csv_rdd.first().split(',')).value
    csv_rdd = csv_rdd.filter(lambda line: line != ','.join(csv_header)).map(lambda line: line.split(','))

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

    votes_mat = sparse.lil_matrix( (user_counter, item_counter), dtype=float)
    for ii in xrange(len(data)):
        votes_mat[rows[ii], cols[ii]] = data[ii]

    return votes_mat, items, users

def order_data(uservotes, n_u, n_i):

    item_order = {}
    user_order = {}
    user_counter = 0
    item_counter = 0
    for user in uservotes:
        for item in uservotes[user]:
            if item not in item_order:
                item_order[item] = item_counter
                item_place = item_counter
                item_counter += 1
            else:
                item_place = item_order[item]
        user_order[user] = user_counter
        user_counter += 1
           
    return item_order, user_order

def load_trust(csv_file, trust_idx, user_order, skip_lines = 1):

    T = sparse.lil_matrix( (len(user_order), len(user_order)), dtype=float)

    def update_data(u1,u2,t, T):
        i = user_order[u1]
        j = user_order[u2]
        T[i,j] = t
#T[j,i] = t

    with open(csv_file,'rU') as f:
        counter = 0
        reader = csv.reader(f, delimiter=',')
        for ii, line in enumerate(reader):
            if ii < skip_lines:
                continue
            liker = line[0]
            likee = line[1]
            t = int(line[trust_idx])
    
            update_data(liker, likee, t, T)

    return T

def normalize_trust_spark(trust):

    total_trust = trust.map(lambda (user, friend, t): (user, float(t))).reduceByKey(add)
    trust = trust.map(lambda (user, friend, t): (user, (friend, float(t)))).join(total_trust).map(lambda (user, ((friend, t),total_t)): (user, friend, t/total_t))
    trust = trust.map(lambda (user, friend, trust): (friend, (user, float(trust)))).distinct()
    trust = trust.groupByKey().cache()

    return trust
def normalize_trust(T, self_trust=1):

    n = T.shape[0]
    for ii in xrange(n):
        other_trust = T[ii,:].sum()
        T[ii,:] = T[ii,:] / other_trust

    return T


def main():

    parser = ArgumentParser()

    parser.add_argument("--data_dir",
          type     = str,
          required = False,
          default  = 'data',
          help     = "CSV of trust relations (can be incomplete)",
          )

    parser.add_argument("--trust",
          type     = str,
          required = True,
          help     = "CSV of trust relations (can be incomplete)",
          )

    parser.add_argument("--votes",
          type     = str,
          required = True,
          help     = "CSV of initial votes on items by users",
          )

    args = parser.parse_args()

    data_dir = args.data_dir

#    V0, items, users = load_votes(os.path.join(data_dir,args.votes) )
#    T = load_trust(os.path.join(data_dir, args.trust), 2, users)
#    T_normed = normalize_trust(T, self_trust=1)
#
#    delta = 0.5
#
#    for ii in xrange(V0.shape[0]):
#        tot_votes = V0[ii,:].sum()
#        V0[ii,:] = V0[ii,:]/np.log(tot_votes+1)
#
#    V0 = V0.tocsc()
#    V0.eliminate_zeros()
#    supnorm = 20
#    print V0.todense()
#    while supnorm > 0.01:
#        V1 = delta*T_normed*V0 + (1-delta)*V0
#        supnorm = (V1-V0).max()
#        V0 = V1
#    print V1.todense()


if __name__ == '__main__':
    main()
