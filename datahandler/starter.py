#!/usr/bin/env python3

import subprocess


if __name__ == '__main__':
    env = subprocess.run('source activate dividend && python data_pipeline.py', shell=True, check=True, stdout=subprocess.PIPE)
    if env.returncode == 0:
        print(env.stdout)
