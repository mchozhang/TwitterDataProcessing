#!/bin/bash
#SBATCH -partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

module load Python/3.5.2-intel-2017.u2
time mpiexec -n 8 python extract_json.py
module unload Python/3.5.2-intel-2017.u2
