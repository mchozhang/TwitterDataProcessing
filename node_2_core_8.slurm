#!/bin/bash
#SBATCH --partition=physical
#SBATCH --output=2_8.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1


module load Python/3.5.2-intel-2017.u2
time mpiexec -n 8 python main.py
module unload Python/3.5.2-intel-2017.u2
