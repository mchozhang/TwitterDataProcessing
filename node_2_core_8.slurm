#!/bin/bash
#SBATCH --partition=physical
#SBATCH --output=node_2_core_8.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

ln -s -f /data/projects/COMP90024/bigTwitter.json
module load Python/3.5.2-intel-2017.u2
time mpirun python main.py
module unload Python/3.5.2-intel-2017.u2
