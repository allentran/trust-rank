import os
from argparse import ArgumentParser
import numpy as np
from pyspark import SparkContext

from input.load_data import load_csv_spark

class Update_v:

    def mapper((r,c),(T_rc, vc)):
        return r,T_rc*vc

    def reducer(r, vs):
        return r,np.sum(vs)

    def map_arg_generator(T,v):

        for rr in xrange(T.shape[0]):
            for rr in xrange(T.shape[0]):
                if T[rr,jj] > 0:
                    yield (rr,cc),(T[rr,cc], v[cc])

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
    v1 = user_place_votes.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0], (x[0][1], x[1])))

    return v1

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

    sc = SparkContext()
    
    votes = load_csv_spark(sc, os.path.join(args.data_dir, args.votes))
    trust = load_csv_spark(sc, os.path.join(args.data_dir, args.trust))

    # ensure trust relation is symmetric
    trust = trust.flatMap(lambda x: [
                            (x['user_1'], (x['user_2'], float(x['trust']))), 
                            (x['user_2'], (x['user_1'], float(x['trust']))), 
                            ]).distinct()

    # key votes by user
    votes = votes.map(lambda x: (x['user'],(x['place'], float(x['vote'])))) 

    v0 = votes
    supnorm = 20
    kappa = 0.5

    while supnorm > 1:
        v1 = update_votes(trust, v0).map(lambda x: (x[0],(x[1][0], x[1][1]*kappa))) + v0.map(lambda x: (x[0],(x[1][0], x[1][1]*(1-kappa))))
        v1_v0 = v1.map(lambda x: ((x[0], x[1][0]), x[1][1])).join(v0.map(lambda x: ((x[0], x[1][0]), x[1][1])))
        diffs = v1_v0.map(lambda x: abs(x[1][0] - x[1][1])).reduce(lambda x,y: max(x,y))
        supnorm = diffs
        print 'hahaha' + str(supnorm)
        import time
        time.sleep(5)
        v0 = v1

if __name__ == '__main__':
    main()

