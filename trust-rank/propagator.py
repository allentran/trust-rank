from argparse import ArgumentParser
import os
import numpy as np
from pyspark import SparkContext
from operator import add

from input.load_data import load_csv_spark, normalize_trust_spark

def scale_votes(v):

    agg_v = v.map(lambda x: (x[0],x[1][1])).reduceByKey(lambdas['sum'])
    return v.join(agg_v).map(lambda x: (x[0], (x[1][0][0], x[1][0][1]/x[1][1])))
 
def FriendAdviceToUser(user_trusts, item, friend_score):    

    for user,trust in user_trusts:
        friends_weighted_advice = trust * friend_score
        yield ((user, item), friends_weighted_advice)
                                                         
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

    data_dir = args.data_dir
    
    delta = 0.5

    items = load_csv_spark(sc, os.path.join(data_dir,args.votes)) 
    trust = load_csv_spark(sc, os.path.join(data_dir,args.trust)) 

    # load trust and normalize
    trust = normalize_trust_spark(trust)
    own_advice = items.map(lambda (user, item, score): (user, (item, float(score))))

    epsilon

    while True:
        distinct_advice = trust.join(own_advice).flatMap(lambda (friend, (user_trusts, (item, friend_score))): FriendAdviceToUser(user_trusts, item, friend_score)) 
        aggregated_advice = distinct_advice.reduceByKey(add)

        updated_advice = aggregated_advice.leftOuterJoin(own_advice.map(lambda (user, (item, score)): ((user, item), score)))
        updated_advice = updated_advice.mapValues(lambda (new_rating, old_rating): (new_rating, old_rating or 0))
        updated_advice = updated_advice.mapValues(lambda (new_rating, old_rating): (delta*new_rating + (1-delta)*old_rating, old_rating))
        max_diff = updated_advice.mapValues(lambda (old, new): abs(old-new)).map(lambda ((user, item), d): d) \
                                                                            .reduce(lambda a,b: max(a,b))
        if max_diff < 0.05:
            print own_advice.collect()
            break
        own_advice = updated_advice.map(lambda ((user, item), (updated_score, old_score)): (user, (item, updated_score)))   
        iters += 1

    

if __name__ == '__main__':
    main()

