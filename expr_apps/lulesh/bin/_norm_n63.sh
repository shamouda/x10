#!/bin/bash
#PBS -P v29
#PBS -q normal
#PBS -l walltime=00:06:00
#PBS -l mem=630GB
#PBS -l wd
#PBS -l ncpus=1008

##Takes 00:04:31 for a single iteration on raijin of the 5 modes

ulimit -c 0
source ~/prof/x10_ulfm2.profile
PROG_DIR=$X10_HOME/../x10-applications/lulesh2_resilient/bin

NODES=63
P=1000
S=8

X10_NPL=$((P+S))
X10_NTHR=1

MPIOPTS="-np $X10_NPL --bind-to core --map-by node --mca btl openib,vader,self"

export X10_EXIT_BY_SIGKILL=1
export X10_NUM_IMMEDIATE_THREADS=1
export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0
export LULESH_SYNCH_GHOSTS=1

SIZE=30
ITERS=30

for run in `seq 1 3`;
do
    ARGS="-p -i $ITERS -e $S -s $SIZE"
    POSTFIX="s$SIZE.iter$ITERS"

        
    date
    echo "P=$P mode0gc run$run"
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=0 mpirun $MPIOPTS $PROG_DIR/lulesh2.0 $ARGS &> ../out/lulesh.$POSTFIX.${P}x${X10_NTHR}.mode0gc.run$run.txt

    date
    echo "P=$P mode1 run$run"
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=1 mpirun $MPIOPTS $PROG_DIR/lulesh2.0 $ARGS &> ../out/lulesh.$POSTFIX.${P}x${X10_NTHR}.mode1.run$run.txt

    date
    echo "P=$P mode2 run$run"
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=2 mpirun $MPIOPTS $PROG_DIR/lulesh2.0 $ARGS &> ../out/lulesh.$POSTFIX.${P}x${X10_NTHR}.mode2gc.run$run.txt

    date
    echo "P=$P mode13 run$run"
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=13 mpirun $MPIOPTS $PROG_DIR/lulesh2.0 $ARGS &> ../out/lulesh.$POSTFIX.${P}x${X10_NTHR}.mode13.run$run.txt

    date
    echo "P=$P mode14 run$run"
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=14 mpirun $MPIOPTS $PROG_DIR/lulesh2.0 $ARGS &> ../out/lulesh.$POSTFIX.${P}x${X10_NTHR}.mode14gc.run$run.txt

done
