#!/bin/bash
#PBS -P v29
#PBS -q normal
#PBS -l walltime=00:04:00
#PBS -l mem=220GB
#PBS -l wd
#PBS -l ncpus=352

source ~/prof/x10_ulfm2.profile
PROG_DIR=$X10_HOME/x10.tests/tests/MicroBenchmarks

NODES=22
P=348
S=0

X10_NPL=$((P+S))
X10_NTHR=1

MPIOPTS="-np $X10_NPL --bind-to core --map-by node --mca btl openib,vader,self"

export X10_EXIT_BY_SIGKILL=1
export X10_NUM_IMMEDIATE_THREADS=1
export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0

for run in `seq 1 1`;
do 
X10_RESILIENT_MODE=0  X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkBcast.mpi &> ../out/bcast.${P}x${X10_NTHR}.mode0gc.mpi.run${run}.txt
X10_RESILIENT_MODE=1  X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkBcast.mpi &> ../out/bcast.${P}x${X10_NTHR}.mode1.mpi.run${run}.txt

X10_RESILIENT_MODE=0  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkBcast.mpi &> ../out/bcast.${P}x${X10_NTHR}.mode0gc.emu.run${run}.txt
X10_RESILIENT_MODE=1  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkBcast.mpi &> ../out/bcast.${P}x${X10_NTHR}.mode1.emu.run${run}.txt
 
done
