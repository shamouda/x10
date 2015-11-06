NPL=6
for K in 1 2 3 4 5
do
echo "Testing with $NPL places, Killing place $K ..."

for I in {0..100}
do

##echo "Test-$I" 
##X10_TEAM_DEBUG_INTERNALS=0 \
##X10_TEAM_COMMENT_EXCEPTION=1 \
##X10_NTHREADS=1 \
##X10_RESILIENT_MODE=1 \
##mpirun -np $((NPL)) -am ft-enable-mpi ./ResilientBarrierTest.mpi $K


echo "socket $I"
X10_TEAM_DEBUG_INTERNALS=1 \
X10_TEAM_COMMENT_EXCEPTION=1 \
X10_NTHREADS=1 \
X10_RESILIENT_MODE=1 \
X10_NPLACES=$((NPL)) ./ResilientBarrierTest.sock $((K))

 done

done
~                                                                                                                                                             
~                                                                                                                                                             
~                                                                                                                                                             
~                                                                                                                                                             
~      
