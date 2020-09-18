#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/18 14:41
# @Author  : Evan / Ethan
# @File    : implicit_usage.py
import time
import tqdm
import codecs
import argparse
import logging

from pyhive import hive
import pandas as pd
import numpy as np

from scipy.sparse import coo_matrix, csr_matrix
from implicit.als import AlternatingLeastSquares
from implicit.approximate_als import (AnnoyAlternatingLeastSquares, FaissAlternatingLeastSquares,
                                      NMSLibAlternatingLeastSquares)
from implicit.bpr import BayesianPersonalizedRanking
from implicit.lmf import LogisticMatrixFactorization
from implicit.nearest_neighbours import (BM25Recommender, CosineRecommender,
                                         TFIDFRecommender, bm25_weight,
                                         tfidf_weight, normalize)

# maps command line model argument to class name
MODELS = {"als":  AlternatingLeastSquares,
          "nmslib_als": NMSLibAlternatingLeastSquares,
          "annoy_als": AnnoyAlternatingLeastSquares,
          "faiss_als": FaissAlternatingLeastSquares,
          "tfidf": TFIDFRecommender,
          "cosine": CosineRecommender,
          "bpr": BayesianPersonalizedRanking,
          "lmf": LogisticMatrixFactorization,
          "bm25": BM25Recommender}

FILE_NAME = 'fuck_me.csv'
# FILE_NAME = '200-peop-applist.csv'


def get_model(model_name):
    print("getting model %s" % model_name)
    model_class = MODELS.get(model_name)
    if not model_class:
        raise ValueError("Unknown Model '%s'" % model_name)

    # some default params
    if issubclass(model_class, AlternatingLeastSquares):
        params = {'factors': 200, 'iterations': 100, 'calculate_training_loss': True, 'validate_step': -1}
    elif model_name == "bm25":
        params = {'K1': 100, 'B': 0.5}
    elif model_name == "bpr":
        params = {'factors': 200}
    elif model_name == "lmf":
        params = {'factors': 200, "iterations": 40, "regularization": 1.5}
    else:
        params = {}

    return model_class(**params)


def calculate_recommendations(output_filename, model_name="als"):
    """ Generates artist recommendations for each user in the dataset """
    print('calculate_recommendations')
    np.set_printoptions(suppress=True)
    # train the model based off input params
    artists, users, plays = read_dataframe()
    # print(artists.shape)
    # print(users[:10])
    # print(type(plays))
    # create a model from the input data
    model = get_model(model_name)
    # if we're training an ALS based model, weight input for last.fm
    # by bm25
    if issubclass(model.__class__, AlternatingLeastSquares):
        # lets weight these models by bm25weight.
        logging.debug("weighting matrix by bm25_weight")
        # plays = bm25_weight(plays, K1=100, B=0.8)
        plays = bm25_weight(plays, K1=1.2, B=0.75)
        # plays = normalize(tfidf_weight(plays))
        # also disable building approximate recommend index
        model.approximate_similar_items = False

    # this is actually disturbingly expensive:
    plays = plays.tocsr()
    # print(plays)
    logging.debug("training model %s", model_name)
    start = time.time()
    model.fit(plays)
    logging.debug("trained model '%s' in %0.2fs", model_name, time.time() - start)
    user_item_m = np.dot(model.user_factors, model.item_factors.T)

    print('\n', user_item_m)
    # print(model.user_factors)

    # generate recommendations for each user and write out to a file
    start = time.time()
    user_plays = plays.T.tocsr()
    with tqdm.tqdm(total=len(users)) as progress:
        with codecs.open(output_filename, "w", "utf8") as o:
            for userid, username in enumerate(users):
                for artistid, score in model.recommend(userid, user_plays):
                    o.write("%s\t%s\t%s\n" % (username, artists[artistid], score))
                progress.update(1)
    logging.debug("generated recommendations in %0.2fs",  time.time() - start)


def read_dataframe():
    """ Reads the original dataset TSV as a pandas dataframe """
    # delay importing this to avoid another dependency
    dd = data_base_2_df('select app_name, client_uid, 1 from temp.evany_userid_applist', 'xxx', 'xxx')
    print(dd)
    # dd.to_csv('xxx.csv', index=False, header=False, encoding="utf_8_sig")
    # read in triples of user/artist/playcount from the input dataset
    # get a model based off the input params
    # data = pd.read_csv(filename, delimiter='\t', header=-1)

    # map each artist and user to a unique numeric value
    # data[0] = data[0].astype("category")
    # data[1] = data[1].astype("category")
    #
    # plays = coo_matrix((data[2].astype(np.float32),
    #                     (data[0].cat.codes.copy(),
    #                      data[1].cat.codes.copy()))).tocsr()
    #
    # a = np.array(list(data[0].cat.categories))
    # b = np.array(list(data[1].cat.categories))
    #
    # c = csr_matrix((plays.data, plays.indices, plays.indptr))
    #
    # return a, b, c


def data_base_2_df(sql, username, password, chunksize=0):
    """
    Connect Hive through Python
    """
    try:
        conn = hive.Connection(username=username, password=password, database='dw',
                               host='10.200.1.11', port=10000, auth="LDAP")
        # df = pd.read_sql(sql, con=conn)
        # conn.close()

    except Exception as e:
        print('Error is:', e)
    else:
        return df


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Generates similar artists on the last.fm dataset"
    #                                  " or generates personalized recommendations for each user",
    #                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument('--output', type=str, default='similar-artists.tsv',
    #                     dest='outputfile', help='output file name')
    #
    # parser.add_argument('--model', type=str, default='als',
    #                     dest='model', help='model to calculate (%s)' % "/".join(MODELS.keys()))
    #
    # parser.add_argument('--recommend', default='True',
    #                     help='Recommend items for each user rather than calculate similar_items',
    #                     action="store_true")
    # parser.add_argument('--param', action='append',
    #                     help="Parameters to pass to the model, formatted as 'KEY=VALUE")
    #
    # args = parser.parse_args()
    #
    # logging.basicConfig(level=logging.DEBUG)
    #
    # if args.recommend:
    #     calculate_recommendations(args.outputfile, model_name=args.model)

    read_dataframe()


