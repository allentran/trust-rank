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
