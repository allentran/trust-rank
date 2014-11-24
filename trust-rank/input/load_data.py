from argparse import ArgumentParser
import os
import csv
import json
import numpy as np
from scipy import sparse

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

    votes_mat = sparse.csc_matrix( (data,(rows, cols)), shape=(user_counter, item_counter))

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




def load_trust(csv_file, trust_idx=2, skip_lines = 1):

    def update_user(user1, user2, t):
        if user1 in trust:
            user_data = trust.pop(user1)
            user_data.update({user2: t})
        else:
            user_data = {user2: t}
        trust[user1] = user_data

    trust= {}

    with open(csv_file,'rU') as f:
        counter = 0
        reader = csv.reader(f, delimiter=',')
        for line in reader:
            if counter < skip_lines:
                counter += 1
                continue
            user1 = line[0]
            user2 = line[1]
            t = line[trust_idx]
            update_user(user1, user2, t)
            update_user(user2, user1, t)
            
    return trust
    

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

    with open(os.path.join(args.data_dir, 'trust.json'), 'wb') as f:
        json.dump(load_trust(os.path.join(args.data_dir,args.trust)), f)

    votes, n_items = load_votes(os.path.join(args.data_dir,args.votes))
    print order_data(votes, n_u=len(votes), n_i=n_items)

if __name__ == '__main__':
    main()
