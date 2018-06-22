#########################################################################
# File Name: txt2pkl.sh
# Author: zhouduoyou
# Mail: zhouduoyou@bytedance.com
# Created Time: Thu 14 Jun 2018 08:20:56 PM CST
#########################################################################
#!/bin/bash
python txt2npz.py --convert_dir ../factors/ --npzs_path ../processed_data4 --num_processes 10
