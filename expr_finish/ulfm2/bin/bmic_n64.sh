#!/bin/bash
#PBS -P v29
#PBS -q normal
#PBS -l walltime=00:10:00
#PBS -l mem=640GB
#PBS -l wd
#PBS -l ncpus=1024

source ~/prof/x10_ulfm2.profile
PROG_DIR=$X10_HOME/x10.dist/samples/resiliency

NODES=64
P=1024
S=0

X10_NPL=$((P+S))
X10_NTHR=1

MPIOPTS="-np $X10_NPL --bind-to core --map-by node --mca btl openib,vader,self"

export X10_EXIT_BY_SIGKILL=1
export X10_NUM_IMMEDIATE_THREADS=1
export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0
export TEST_BASED_FROM=$((P/2))
export BENCHMICRO_ITER=10

for run in `seq 1 1`;
do 
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=0  mpirun $MPIOPTS $PROG_DIR/BenchMicro.mpi &> ../out/bmic.${P}x${X10_NTHR}.mode0gc.run${run}.txt
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=2  mpirun $MPIOPTS $PROG_DIR/BenchMicro.mpi &> ../out/bmic.${P}x${X10_NTHR}.mode2gc.run${run}.txt
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=14 mpirun $MPIOPTS $PROG_DIR/BenchMicro.mpi &> ../out/bmic.${P}x${X10_NTHR}.mode14gc.run${run}.txt
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=1  mpirun $MPIOPTS $PROG_DIR/BenchMicro.mpi &> ../out/bmic.${P}x${X10_NTHR}.mode1.run${run}.txt
    X10_NTHREADS=$X10_NTHR X10_RESILIENT_MODE=13 mpirun $MPIOPTS $PROG_DIR/BenchMicro.mpi &> ../out/bmic.${P}x${X10_NTHR}.mode13.run${run}.txt
done
