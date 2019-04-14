#!/bin/bash
#SBATCH --partition=physical
#SBATCH --output=node_2_core_8.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

TWITTER=$1
MELB_GRID=$2
if [[ "$1" == "" ]];
then
    TWITTER="bigTwitter.json"
    MELB_GRID="melbGrid.json"
elif [[ "$2" == "" ]];
then
    MELB_GRID="melbGrid.json"
fi
module load Python/3.5.2-intel-2017.u2
time mpirun python main.py -t "$TWITTER" -m "$MELB_GRID"
module unload Python/3.5.2-intel-2017.u2
