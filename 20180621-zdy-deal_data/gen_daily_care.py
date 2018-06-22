import sys
import os
import argparse
import pickle
from datetime import *
import pdb

ONEDAY=timedelta(days=1)

def parse_ymd(ymd):
    year_s, mon_s, day_s = ymd.split('-')
    return datetime(int(year_s), int(mon_s), int(day_s))

def get_former_trading_date(dates, date):
    date_parsed = parse_ymd(date)
    while True:
        date_parsed = date_parsed - ONEDAY
        if date_parsed.strftime('%Y-%m-%d') in dates:
            break
    return date_parsed.strftime('%Y-%m-%d')

def get_daily_care(filename):
    lines = open(filename).readlines()
    daily_care  = {}
    for line in lines:
        segs = line.strip().split(' ')
        if len(segs) < 3:
            continue
        index, inner_code, date = segs
        if not daily_care.has_key(date):
            daily_care[date] = []
        daily_care[date].append(inner_code)
    return daily_care

def get_daily_close(filename):
    lines = open(filename).readlines()
    daily_close  = {}
    for line in lines:
        segs = line.strip().split(' ')
        if len(segs) < 7:
            continue
        index, inner_code, date, open_, high, low, close = segs
        if not daily_close.has_key(date):
            daily_close[date] = {}
        daily_close[date][inner_code] = close
    return daily_close

def get_daily_care_ratio(daily_care, daily_close):
    dates = daily_care.keys()
    daily_care_ratio = {}
    for date in dates:
        #the date start of close data
        if date == '2005-01-04':
            continue
        if date not in daily_care_ratio.keys():
            daily_care_ratio[date] = {}
        former_trading_date = get_former_trading_date(dates, date)
        #daily_close data may contain ignore stock.the stocks of daily_close > of daily_care
        stocks_care = daily_care[date]
        stocks_care_len = len(stocks_care)
        yield_rate = {}
        for stock in stocks_care:
            yield_rate[stock] = (float(daily_close[date][stock]) - float(daily_close \
            [former_trading_date][stock]))/float(daily_close[former_trading_date][stock])
        #yield_rate: large -> small
        sorted_yield_rate = sorted(yield_rate.items(), key=lambda item:item[1], reverse=True)
        for ind, content in enumerate(sorted_yield_rate):
            yield_rank, yield_value, stock = float(ind + 1)/stocks_care_len, content[1], content[0]
            daily_care_ratio[date][stock] = {'yield_value': yield_value, 'yield_rank':yield_rank}
    return daily_care_ratio

if __name__ == '__main__':
    p = argparse.ArgumentParser(
    description='generate daily-care data',
    formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--daily_care_list')
    p.add_argument('--daily_close')
    p.add_argument('--output_daily_care')
    p.add_argument('--save_daily_care_pkl',action="store_true")
    p.add_argument('--save_daily_care_ratio_pkl',action="store_true")
    args = p.parse_args()
    #generate daily_care stocks
    daily_care = get_daily_care(args.daily_care_list)
    fid = open(args.output_daily_care, 'wb')
    print 'save daily_care txt ... '
    for date in daily_care.keys():
        fid.write(date + '\t' + '\t'.join(daily_care[date]) + '\n')
    fid.close()
    if args.save_daily_care_pkl:
        f=open(args.output_daily_care + '.pkl','w')
        pickle.dump(daily_care,f,0)
    f.close()
    #generate daily_care stocks ranking ratio
    print 'save daily_close pkl'
    daily_close = get_daily_close(args.daily_close)
    f=open('daily_close.pkl','w')
    pickle.dump(daily_close,f,0)
    f.close()
    if  args.save_daily_care_ratio_pkl:
        print 'save daily_care_ratio pkl'
        daily_care_ratio = get_daily_care_ratio(daily_care, daily_close)
        f=open(args.output_daily_care + '_ratio_value.pkl', 'w')
        pickle.dump(daily_care_ratio,f,0)
        f.close()

