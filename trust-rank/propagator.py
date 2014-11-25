import os
from argparse import ArgumentParser
import numpy as np
from pyspark import SparkContext

from input.load_data import load_csv_spark

lambdas = {
    'sum': lambda x,y: x+y,
    'max': lambda x,y: max(x,y),
    'diffs': lambda x: ((x[0], x[1][0]), x[1][1]),
}

def update_votes(trust,v0):

    friend_trust_votes = trust.join(v0) # returns friend, ( (user,trust), (place,vote))
    friend_trust_votes = friend_trust_votes.map(lambda x: {
                                'friend': x[0],
                                'user': x[1][0][0],
                                'trust': x[1][0][1],
                                'place': x[1][1][0],
                                'vote': x[1][1][1]
                                })
    user_place_votes = friend_trust_votes.map(lambda x:
                                                    ((x['user'], x['place']),
                                                    x['trust']*x['vote'])
                                                )
    v1 = user_place_votes.reduceByKey(lambdas['sum']).map(lambda x: (x[0][0], (x[0][1], x[1])))

    return v1

def trust_normalizer(trust):

    # user: trust values pairs 
    user_totaltrust = trust.map(lambda x: (x[0], x[1][1])).reduceByKey(lambdas['sum'])

    return trust.join(user_totaltrust).map(lambda x: (x[0], (x[1][0][0], x[1][0][1]/x[1][1])))

def main():

    parser = ArgumentParser()

    parser.add_argument("--data_dir",
          type     = str,
          required = False,
          default  = 'data',
          help     = "data directory",
          )
    parser.add_argument("--trust",
          type     = str,
          required = True,
          help     = "CSV of trust relations",
          )
    parser.add_argument("--votes",
          type     = str,
          required = True,
          help     = "CSV of initial votes on items by users",
          )
    parser.add_argument("--outfile",
          type     = str,
          required = True,
          help     = "Final output file",
          )

    args = parser.parse_args()

    sc = SparkContext()
    
    votes = load_csv_spark(sc, os.path.join(args.data_dir, args.votes))
    trust = load_csv_spark(sc, os.path.join(args.data_dir, args.trust))

    # ensure trust relation is symmetric
    # although not necessary
    trust = trust.flatMap(lambda x: [
                            (x['user_1'], (x['user_2'], float(x['trust']))), 
                            (x['user_2'], (x['user_1'], float(x['trust']))), 
                            ]).distinct()
    trust = trust_normalizer(trust).filter(lambda x: x[1][1]>0)

    # key votes by user
    votes = votes.map(lambda x: (x['user'],(x['place'], float(x['vote'])))) 

    v0 = votes
    supnorm = 20
    kappa = 0.5

    while supnorm > 1:
        v1 = update_votes(trust, v0).map(lambda x: (x[0],(x[1][0], x[1][1]*kappa))) + v0.map(lambda x: (x[0],(x[1][0], x[1][1]*(1-kappa))))
        v1_v0 = v1.map(lambdas['diffs']).join(v0.map(lambdas['diffs']))
        diffs = v1_v0.map(lambda x: abs(x[1][0] - x[1][1])).reduce(lambdas['max'])
        supnorm = diffs
        time.sleep(5)
        v0 = v1

    v1.map(lambda x: (x[0], x[1][0], x[1][1])).saveAsTextFile(args.outfile) 

if __name__ == '__main__':
    main()

