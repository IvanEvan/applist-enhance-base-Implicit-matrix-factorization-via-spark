#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/13 18:27
# @Author  : Evan / Ethan
# @File    : 07_plot_PCA_data.py
import sys
import logging
import pandas as pd
from matplotlib import pyplot as plt

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)


if __name__ == '__main__':
    csv_file = sys.argv[1]
    img_file = sys.argv[2]

    reader = pd.read_csv(csv_file, delimiter=',', chunksize=10000)

    fig = plt.figure()
    for num, sub_obj in enumerate(reader):
        plt.plot(sub_obj['pcafeaturesx'], sub_obj['pcafeaturesy'], 'g*')
        logging.info('===== plot #%d data done =====' % num)

    plt.savefig(img_file)
