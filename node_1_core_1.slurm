#!/bin/bash
#SBATCH --output=node_1_core_1.out
#SBATCH --nodes=1
#SBATCH --ntasks=1

ln -s -f /data/projects/COMP90024/bigTwitter.json
module load Python/3.5.2-intel-2017.u2
time mpiexec -n 1 python main.py
module unload Python/3.5.2-intel-2017.u2
