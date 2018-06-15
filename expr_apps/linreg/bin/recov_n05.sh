#!/bin/bash
#PBS -P v29
#PBS -q normal
#PBS -l walltime=00:06:00
#PBS -l mem=50GB
#PBS -l wd
#PBS -l ncpus=80


ulimit -c 0
source ~/prof/x10_ulfm2.profile
PROG_DIR=$X10_HOME/x10.gml/examples/linreg

NODES=5
P=64
S=16

X10_NPL=$((P+S))
X10_NTHR=1

MPIOPTS="-np $X10_NPL --bind-to core --map-by node --mca btl openib,vader,self"

export X10_EXIT_BY_SIGKILL=1
export X10_NUM_IMMEDIATE_THREADS=1
export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0
export OPENBLAS_NUM_THREADS=1

ITERS=30

CKPTITERS=10
V1=$((P/2))
S1=15

ROWS_PER_PLACE=50000
COLS=100
ROWS=$((ROWS_PER_PLACE*P))


for run in `seq 1 3`;
do
    ARGS="--iterations $ITERS -m $ROWS -n $COLS --density 1.0 --checkpointFreq $CKPTITERS --spare $S"
    POSTFIX="iter$ITERS"
        
    date
    echo "P=$P mode1 run$run"
    KILL_PLACES="${V1}" KILL_STEPS="${S1}" X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=1 mpirun $MPIOPTS $PROG_DIR/RunLinReg_mpi_double $ARGS &> ../out/linregrecov.$POSTFIX.${P}x${X10_NTHR}.mode1.run$run.txt

    date
    echo "P=$P mode2 run$run"
    KILL_PLACES="${V1}" KILL_STEPS="${S1}" X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=2 mpirun $MPIOPTS $PROG_DIR/RunLinReg_mpi_double $ARGS &> ../out/linregrecov.$POSTFIX.${P}x${X10_NTHR}.mode2gc.run$run.txt

    date
    echo "P=$P mode13 run$run"
    KILL_PLACES="${V1}" KILL_STEPS="${S1}" X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=13 mpirun $MPIOPTS $PROG_DIR/RunLinReg_mpi_double $ARGS &> ../out/linregrecov.$POSTFIX.${P}x${X10_NTHR}.mode13.run$run.txt

    date
    echo "P=$P mode14 run$run"
    KILL_PLACES="${V1}" KILL_STEPS="${S1}" X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=14 mpirun $MPIOPTS $PROG_DIR/RunLinReg_mpi_double $ARGS &> ../out/linregrecov.$POSTFIX.${P}x${X10_NTHR}.mode14gc.run$run.txt

done
