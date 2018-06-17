#!/bin/bash
PROG_DIR=$X10_HOME/x10.tests/tests/MicroBenchmarks

NODES=29
P=348
S=0
X10_NPL=$((P+S))
X10_NTHR=1

export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0

HOSTS_FILE=hosts_29.txt
MPIOPTS="-np $X10_NPL --hostfile $HOSTS_FILE --bind-to core --map-by node"

export X10_EXIT_BY_SIGKILL=1
export X10_NUM_IMMEDIATE_THREADS=1
export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0

for run in `seq 1 1`;
do 

X10_RESILIENT_MODE=0  X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkAllreduce.mpi &> ../out/allred.${P}x${X10_NTHR}.mode0gc.mpi.run${run}.txt
X10_RESILIENT_MODE=1  X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkAllreduce.mpi &> ../out/allred.${P}x${X10_NTHR}.mode1.mpi.run${run}.txt

X10_RESILIENT_MODE=0  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkAllreduce.mpi &> ../out/allred.${P}x${X10_NTHR}.mode0gc.emu.run${run}.txt
X10_RESILIENT_MODE=1  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR mpirun $MPIOPTS $PROG_DIR/BenchmarkAllreduce.mpi &> ../out/allred.${P}x${X10_NTHR}.mode1.emu.run${run}.txt

done

