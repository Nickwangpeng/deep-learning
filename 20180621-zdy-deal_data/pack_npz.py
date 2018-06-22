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
import tarfile

'''
pack npz files into tar files
'''
def add2tar(files_list, tar_dir, idx):
    tarpath = os.path.join(tar_dir, 'splittar{}.tar'.format(idx))
    fid = tarfile.open(tarpath, 'w')
    for file_ in files_list:
        print file_
        fid.add(file_)
    fid.close()

if __name__ == '__main__':
    p = argparse.ArgumentParser(
    description='pack npz into tar files',
    formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--npzs_path')
    p.add_argument('--tarfiles_path')
    p.add_argument('--num_processes', type=int, default=20)
    args = p.parse_args()

    if not os.path.exists(args.tarfiles_path):
        os.mkdir(args.tarfiles_path)
    search_cmd = os.path.join(args.npzs_path, '*/*.npz')
    npz_files = glob.glob(search_cmd)
    files_list = [[] for _ in range(min(len(npz_files), args.num_processes))]
    for idx, file_ in enumerate(npz_files):
        which_idx = idx % len(files_list)
        files_list[which_idx].append(file_)
    processes = []
    for idx in range(len(files_list)):
        p = mp.Process(target=add2tar,
                args=(files_list[idx], args.tarfiles_path, idx))
        p.daemon = True
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
