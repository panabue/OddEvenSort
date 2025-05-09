/**************************************************
* Nome do estudante: Theo Torminn Neto Nasser
* Trabalho 1
* Disciplina: Programação Paralela
* Objetivo: Aplicar o algoritmo Odd-Even Sort em uma máquina com
memória distribuída, usando múltiplos processos que se comunicam por meio da biblioteca
MPI, para ordenar grandes volumes de dados.*/

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
    int npes, myrank;
    MPI_Status status;

    typedef struct {
        char sensor_id[10];
        int data;
        int hora;
        char cidade[50];
        char bairro[50];
        float temperatura;
    } Sensor;
    

 
    // Inicializa o MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    for(int i = 0; i<10; i++){
        a[i] = myrank;
    }

    if(myrank%2==1){
        MPI_Send(a, 10, MPI_INT, (myrank+1)%npes, 1, MPI_COMM_WORLD);
        MPI_Recv(b, 10, MPI_INT, (myrank-1+npes)%npes, 1, MPI_COMM_WORLD, &status);
    }
    else{
        MPI_Recv(b, 10, MPI_INT, (myrank-1+npes)%npes, 1, MPI_COMM_WORLD, &status);
        MPI_Send(a, 10, MPI_INT, (myrank+1)%npes, 1, MPI_COMM_WORLD);
    }

    for(int i=0; i<10; i++){
        printf("%d ", b[i]);
    }

    // Finaliza o MPI
    MPI_Finalize();
    return 0;
}