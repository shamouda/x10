#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

#define REPS 1000

int main(int argc, char *argv[]) {
  int rank, nprocs;
  MPI_Status status;
  int *otherBuf, *homeBuf, otherLen, homeLen;
  int i,j;
  double time0, time1;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  otherLen = 7;
  homeLen = 5;
  otherBuf = malloc(otherLen*sizeof(int));
  homeBuf = malloc(homeLen*sizeof(int));

  if (rank == 0) {
      for (j = 1; j < nprocs; j++) {
          time0 = MPI_Wtime();
          for (i=0; i < REPS; i++) {
              //sending a 28 byte message to other
              MPI_Send(otherBuf, otherLen, MPI_INTEGER, j, 0, MPI_COMM_WORLD);
              //sending a 20 byte message to home
              MPI_Recv(homeBuf,  homeLen,  MPI_INTEGER, j, 1, MPI_COMM_WORLD, &status);
          }
          time1 = MPI_Wtime();
          printf("ping pong time:%d:%d: %.2e seconds\n", 0, j, ((time1-time0)/REPS));
      }
  } else {
      for (i=0; i < REPS; i++) {
          MPI_Recv(otherBuf, otherLen, MPI_INTEGER, 0, 0, MPI_COMM_WORLD, &status);
          MPI_Send(homeBuf,  homeLen, MPI_INTEGER,  0, 1, MPI_COMM_WORLD);
      }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  free(otherBuf);
  free(homeBuf);
  MPI_Finalize( );
  return 0;
}
