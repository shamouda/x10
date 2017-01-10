----Socket compilation----

x10c++ Bank.x10 -o Bank.o
x10c++ BankBlocking.x10 -o BankBlocking.o
x10c++ BankResilient.x10 -o BankResilient.o
x10c++ BankLocking.x10 -o BankLocking.o

x10c++ Increment.x10 -o Increment.o
x10c++ IncrementLocking.x10 -o IncrementLocking.o

x10c++ RA.x10 -o RA.o
x10c++ RABlocking.x10 -o RABlocking.o
x10c++ RAResilient.x10 -o RAResilient.o
x10c++ RALocking.x10 -o RALocking.o

----MPI compilation----

x10c++ -x10rt mpi Bank.x10 -o Bank.mpi
x10c++ -x10rt mpi BankBlocking.x10 -o BankBlocking.mpi
x10c++ -x10rt mpi BankResilient.x10 -o BankResilient.mpi
x10c++ -x10rt mpi BankLocking.x10 -o BankLocking.mpi

x10c++ -x10rt mpi Increment.x10 -o Increment.mpi
x10c++ -x10rt mpi IncrementLocking.x10 -o IncrementLocking.mpi


x10c++ -x10rt mpi RASTMMoveWork.x10 -o RASTMMoveWork.mpi
x10c++ -x10rt mpi RASTMMoveData.x10 -o RASTMMoveData.mpi
x10c++ -x10rt mpi RALocking.x10 -o RALocking.mpi

x10c++ -x10rt mpi RAResilient.x10 -o RAResilient.mpi

----bank---- (success)
TM_DEBUG=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RL_EA_UL.txt
TM_DEBUG=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RL_EA_WB.txt
TM_DEBUG=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RL_LA_WB.txt  
TM_DEBUG=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RV_EA_UL.txt
TM_DEBUG=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RV_EA_WB.txt
TM_DEBUG=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./Bank.o 10 10 2000 &> bank.RV_LA_WB.txt

----bankblocking---- (success)
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RL_EA_UL.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RL_EA_WB.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RL_LA_WB.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RV_EA_UL.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RV_EA_WB.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./BankBlocking.o 10 10 2000 &> bankblocking.RV_LA_WB.txt


----banklocking---- (success)
TM_DEBUG=1 TM_DISABLED=1 X10_NTHREADS=1 X10_NPLACES=5 ./BankLocking.o 10 10 2000 &> banklocking.txt

----bankresilient---- (success)
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RL_EA_UL.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RL_EA_WB.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RL_LA_WB.txt  
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RV_EA_UL.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RV_EA_WB.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=6 ./BankResilient.o 10 10 2000 1 &> bankres.RV_LA_WB.txt

----------------------------------------------------------------------------------------------------
----ra---- (success)
TM_DEBUG=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RL_EA_UL.txt
TM_DEBUG=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RL_EA_WB.txt
TM_DEBUG=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RL_LA_WB.txt  
TM_DEBUG=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RV_EA_UL.txt
TM_DEBUG=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RV_EA_WB.txt
TM_DEBUG=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RA.o 10 10 2000 &> ra.RV_LA_WB.txt


----rablocking----
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RL_EA_UL.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RL_EA_WB.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RL_LA_WB.txt  
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RV_EA_UL.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RV_EA_WB.txt
TM_DEBUG=1 DISABLE_NON_BLOCKING=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=5 ./RABlocking.o 10 10 2000 &> rablocking.RV_LA_WB.txt

----ralocking---- (success)
TM_DEBUG=1 TM_DISABLED=1 X10_NTHREADS=1 X10_NPLACES=5 ./RALocking.o 10 10 2000 &> ralocking.txt

----raresilient---- 
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_EA_UL X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RL_EA_UL.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_EA_WB X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RL_EA_WB.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RL_LA_WB X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RL_LA_WB.txt  
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_EA_UL X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RV_EA_UL.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_EA_WB X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RV_EA_WB.txt
TM_DEBUG=1 KILL_TIMES=2 KILL_PLACES=2 X10_RESILIENT_MODE=1 TM=RV_LA_WB X10_NTHREADS=1 X10_NPLACES=6 ./RAResilient.o 10 10 2000 1 &> rares.RV_LA_WB.txt

