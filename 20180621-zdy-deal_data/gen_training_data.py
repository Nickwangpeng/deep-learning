import sys
import os
import argparse
from datetime import *
import pdb
import pickle
import multiprocessing as mp
import threading
import numpy as np

sys.path.append('../quant_impl/')
from gen_daily_care import *

print 'loading data'
DATES = pickle.load(open('../quant_impl/dates.pkl'))
DAILY_CARE = pickle.load(open('../quant_impl/csi800_daily_care.txt.pkl'))
ALPHA_CARE = pickle.load(open('../quant_impl/alpha_care.pkl'))
DAILY_CARE_RATIO = pickle.load(open('../quant_impl/csi800_daily_care.txt_ratio_value.pkl'))
RAW_NA_HIT = 18
IMPUTED_NA_HIT = 18
POS_RATIO = 0.35
NEG_RATIO = 0.65

"""
starting time: 2005-01-25(15 days), 2005-02-24(30 days)
"""
def get_period_dates(date, lasting_days):
    period_dates = []
    while True:
        date = get_former_trading_date(DATES, date)
        period_dates.append(date)
        if len(period_dates) == lasting_days:
            break
    return period_dates

def get_common_stocks(period_dates):
    common_stock = []
    for date in period_dates:
        if len(common_stock) == 0:
            common_stock = set(DAILY_CARE[date])
        common_stock = common_stock & set(DAILY_CARE[date])
    return list(common_stock)

def get_label(ratio):
    if 0.05 < ratio  < POS_RATIO:
        return 1
    elif NEG_RATIO < ratio  < 0.95:
        return 0
    else:
        return "None"

def get_period_dates_alphas(period_dates, alpha_npz_dir):
    period_dates_alphas = []
    for date in period_dates:
        data = np.load(os.path.join(alpha_npz_dir, date + '.npz'))
        period_dates_alphas.append(data['info'][()])
    return period_dates_alphas

#get alphas based on date specifed
def get_alphas(date_alphas, stock):
    alphas = np.zeros((156))
    sorted_date_alphas = sorted(date_alphas.items(), key=lambda item:item[0])
    raw_NA, imputed_NA = 0, 0
    for ind, stock_imputed in enumerate(sorted_date_alphas):
        if stock_imputed == 'NA':
            print 'no such txt'
            return 'None'
        if stock_imputed[1][stock][0] == 'NA':
            raw_NA += 1
        if stock_imputed[1][stock][1] == 'NA':
            #print 'alpha ind \t' + stock_imputed[0] +'\timputed_NA \t' + '1'
            return "None"
        alphas[ind] = float(stock_imputed[1][stock][1])
    if (raw_NA > RAW_NA_HIT):
        #print 'raw_NA \t' + str(raw_NA)
        return "None"
    return alphas

def get_alpha_feature(dates_list, alpha_npz_dir, lasting_days, save_npz_dir):
    for date in dates_list:
        #start time problem
        if date < "2005-01-25":
            continue
        print os.path.join(save_npz_dir, date)
        if not os.path.exists(os.path.join(save_npz_dir, date)):
            os.mkdir(os.path.join(save_npz_dir, date))
        # far to close
        period_dates = get_period_dates(date, lasting_days)[::-1]
        period_dates_alphas = get_period_dates_alphas(period_dates, alpha_npz_dir)
        common_stocks = get_common_stocks(period_dates + [date])
        #print 'len of common_stocks \t' + str(len(common_stocks))
        for stock in common_stocks:
            stock_train_data, useful = np.zeros((lasting_days, 156)), True
            ratio  = DAILY_CARE_RATIO[date][stock]
            label = get_label(ratio['yield_rank'])
            if label == "None":
                continue
            print 'pick one out: stock \t' + stock
            for ind, date_alphas in enumerate(period_dates_alphas):
                #print 'extracting feature at' + period_dates[ind]
                alpha_feature = get_alphas(date_alphas, stock) 
                if alpha_feature == 'None':
                    print 'feature extracted failed'
                    useful = False
                    break
                #save one slice
                stock_train_data[ind] = alpha_feature
            if useful:
                stock_train_dict = {"data":stock_train_data, "label":label, "ratio":ratio['yield_rank'], "value":ratio['yield_value']}

                npz_path = os.path.join(save_npz_dir, date, '{}.npz'.format(stock))
                np.savez(npz_path, info=stock_train_dict)
                print 'successful saved'

if __name__ == '__main__':
    p = argparse.ArgumentParser(
    description='generate training data',
    formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--alpha_npz_dir')
    p.add_argument('--save_npz_dir')
    p.add_argument('--lasting_days', type=int, default=15)
    p.add_argument('--num_processes', type=int, default=10)
    args = p.parse_args()

    if not os.path.exists(args.save_npz_dir):
        os.mkdir(args.save_npz_dir)
    dates_list = [[] for _ in range(min(len(DATES), args.num_processes))]
    for idx, date in enumerate(DATES):
        which_idx = idx % len(dates_list)
        dates_list[which_idx].append(date)
    processes = []
    for idx in range(len(dates_list)):
        p = mp.Process(target=get_alpha_feature,
                args=(dates_list[idx], args.alpha_npz_dir, args.lasting_days, args.save_npz_dir))
        p.daemon = True
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

