#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8

module load Python/3.5.2-intel-2017.u2
time mpiexec -n 8 python extract_json.py
module unload Python/3.5.2-intel-2017.u2
