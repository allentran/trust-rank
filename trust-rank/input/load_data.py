from argparse import ArgumentParser
import csv

def load_votes(csv_file, user_idx=0, item_idx=1, vote_idx=2, skip_lines = 1):

    votes = {}

    with open(csv_file,'rU') as f:
        counter = 0
        reader = csv.reader(f, delimiter=',')
        for line in reader:
            if counter < skip_lines:
                counter += 1
                continue
            user = line[user_idx]
            item = line[item_idx]
            vote = line[vote_idx]

            if user in votes:
                user_data = votes.pop(user)
                user_data.update({item: vote})
            else:
                user_data = {item: vote}
            votes[user] = user_data
                
    return votes

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

if __name__ == '__main__':
    main()
