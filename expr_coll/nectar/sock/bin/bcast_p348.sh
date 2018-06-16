PROG_DIR=$X10_HOME/x10.tests/tests/MicroBenchmarks

export X10_HOSTFILE=hosts_29.txt
NODES=29
P=348
S=0
X10_NPL=$((P+S))
X10_NTHR=1

export X10_MAX_THREADS=2250
export X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE=0

for run in `seq 1 1`;
do
X10_NPLACES=$X10_NPL X10_RESILIENT_MODE=0  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR $PROG_DIR/BenchmarkBcast.sock &> ../out/bcast.${P}x${X10_NTHR}.mode0gc.emu.run${run}.txt
X10_NPLACES=$X10_NPL X10_RESILIENT_MODE=1  X10RT_X10_FORCE_COLLECTIVES=1 X10_NTHREADS=$X10_NTHR $PROG_DIR/BenchmarkBcast.sock &> ../out/bcast.${P}x${X10_NTHR}.mode1.emu.run${run}.txt
done