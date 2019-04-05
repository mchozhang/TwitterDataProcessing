#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

module load Python/3.5.2-intel-2017.u2
time mpiexec -n 1 python extract_json.py
module unload Python/3.5.2-intel-2017.u2
