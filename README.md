trust-rank
==========

An Apache Spark implementation of PageRank style collaborative filtering with trust

- tested for a ~25m user, 100k item dataset on my 16gb MBP
- lazy loading of data
- Map Reduce friendly algorithm

see [explanation](http://sites.google.com/site/allentranucla/research)

## Instructions

Two input files are required + Spark.  See the data folder for an example of both types.  

1. a CSV file describing trust relations between users
2. a file describing user scores for items

With these files, run `bin/run` with the appropriate arguments pointing to your input files, directory and output file.  The output is a CSV with `user, item, rating`. 
