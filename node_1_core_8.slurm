#!/bin/bash
#SBATCH --output=node_1_core_8.out
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1

ln -s -f /data/projects/COMP90024/bigTwitter.json
module load Python/3.5.2-intel-2017.u2
time mpiexec -n 8 python main.py
module unload Python/3.5.2-intel-2017.u2
