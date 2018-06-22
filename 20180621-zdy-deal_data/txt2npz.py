import sys
import os
import argparse
import pdb
import glob
import pickle
import multiprocessing as mp
import Queue
import threading
import numpy as np
'''
generate time-piece pkl, every pkl contain all alpha
'''
def get_daily_alpha(filename):
    fid = open(filename)
    lines = fid.readlines()
    fid.close()
    daily_alpha  = {}
    for line in lines:
        segs = line.strip().split(' ')
        if len(segs) < 5:
            continue
        index, inner_code, date, raw_alpha, alpha = segs
        daily_alpha[inner_code] = [raw_alpha, alpha]
    return daily_alpha

def process_date(convert_dir, npzs_path, date_list, filtered_alphas):
    for date in date_list:
        daily_alphas = {}
        print date
        key_search = date.replace('-','')
        for alpha_index in filtered_alphas:
            filename = alpha_index + '_' + key_search + '_' + key_search + '.txt'
            file_ = os.path.join(convert_dir, alpha_index, filename)
            if not os.path.exists(file_):
                daily_alphas[alpha_index] = 'NA'
                continue
            daily_alpha = get_daily_alpha(file_)
            daily_alphas[alpha_index] = daily_alpha
        npz_path = os.path.join(npzs_path, date + '.npz')
        np.savez(npz_path, info=daily_alphas)

def get_alphas_dict(convert_dir):
    alphas_list = os.listdir(convert_dir)
    print alphas_list
    alphas_dict = {}
    for alpha in alphas_list:
        alpha_dir = os.path.join(convert_dir, alpha)
        if not os.path.exists(alpha_dir):
            continue
        raw_NA_ratio, imputed_NA_ratio = get_NA_ratio(alpha_dir)
        alphas_dict[alpha] = {'raw':raw_NA_ratio, 'imputed':imputed_NA_ratio}
        print alpha + '\t' + str(raw_NA_ratio) + '\t' + str(imputed_NA_ratio)
    return alphas_dict

def get_NA_ratio(alpha_dir):
    regular = os.path.join(alpha_dir, '*.txt')
    files = glob.glob(regular)
    total, raw_NA, imputed_NA = 0, 0, 0
    for file_ in files:
        lines = open(file_).readlines()
        total += len(lines)
        for line in lines:
            segs = line.strip().split(' ')
            if segs[-1] == 'NA':
                imputed_NA += 1
            if segs[-2] == 'NA':
                raw_NA += 1
    return float(raw_NA)/total, float(imputed_NA)/total

def get_filtered_alphas(alphas_dict, raw_threshold, imputed_threshold):
    filtered_alphas = []
    for alpha in alphas_dict.keys():
        if (alphas_dict[alpha]['raw'] < raw_threshold) and (alphas_dict[alpha]['imputed'] < imputed_threshold):
            filtered_alphas.append(alpha)
    return filtered_alphas

if __name__ == '__main__':
    p = argparse.ArgumentParser(
    description='generate txt to daily_pkl data',
    formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--convert_dir')
    p.add_argument('--npzs_path')
    p.add_argument('--raw_threshold', type=float, default=0.1)
    p.add_argument('--imputed_threshold', type=float, default=0.001)
    p.add_argument('--num_processes', type=int, default=10)
    args = p.parse_args()

    if not os.path.exists(args.npzs_path):
        os.mkdir(args.npzs_path)
    alphas_care = open('alpha_care.pkl')
    alphas_dict = pickle.load(alphas_care)
    alphas_care.close()
    print 'alpha_care.pkl loaded'
    filtered_alphas = get_filtered_alphas(alphas_dict, args.raw_threshold, args.imputed_threshold)
    print 'pick out {} alphas'.format(len(filtered_alphas))
    dates = pickle.load(open('../quant_impl/dates.pkl'))
    print(len(dates))
    dates_list = [[] for _ in range(min(len(dates), args.num_processes))]
    for idx, date in enumerate(dates):
        which_idx = idx % len(dates_list)
        dates_list[which_idx].append(date)
    processes = []
    for idx in range(len(dates_list)):
        p = mp.Process(target=process_date,
                args=(args.convert_dir, args.npzs_path, dates_list[idx], filtered_alphas))
        p.daemon = True
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
