/**************************************************
* Nome do estudante: Theo Torminn Neto Nasser
* Trabalho 1
* Disciplina: Programação Paralela
* Objetivo: Aplicar o algoritmo Odd-Even Sort em uma máquina com
memória distribuída, usando múltiplos processos que se comunicam por meio da biblioteca
MPI, para ordenar grandes volumes de dados.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE 128
#define MAX_REGISTROS 100000000

// Registro dos sensores
typedef struct {
    char sensor_id[10];
    int data;
    int hora;
    char cidade[25];
    char bairro[25];
    float temperatura;
} Sensor;

// Converte a linha lida para um objeto do tipo Sensor.
void parse_line(const char *line, Sensor *s) {
    sscanf(line, "%s %d %d %24s %24s %f",
           s->sensor_id, &s->data, &s->hora,
           s->cidade, s->bairro, &s->temperatura);
}

int main(int argc, char *argv[]) {
    int npes, myrank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    // Aviso para executar o programa corretamente
    if (argc < 2) {
        if (myrank == 0) {
            printf("Use: %s <arquivo_entrada>\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    Sensor *todos_registros = NULL; 
    int total_registros = 0;

    // Leitura feita apenas pelo processo 0
    // Lê todas as linhas do arquivo e conta o numero de registros dos sensores (linhas)
    if (myrank == 0) {
        FILE *fp = fopen(argv[1], "r");
        if (!fp) {
            perror("Erro ao abrir o arquivo");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        todos_registros = malloc(sizeof(Sensor) * MAX_REGISTROS);
        if (!todos_registros) {
            perror("Erro de alocação");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char line[MAX_LINE];
        // Interpreta uma linha e armazena no vetor de struct
        while (fgets(line, MAX_LINE, fp)) {
            parse_line(line, &todos_registros[total_registros]);
            total_registros++;
        }
        
        fclose(fp);
        //printf("Processo 0 leu %d registros do arquivo.\n", total_registros);
    }

    // Comunica o número total de registros aos demais processos
    MPI_Bcast(&total_registros, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Calcula quantos registros cada processo deve receber
    int *sendcounts = malloc(npes * sizeof(int));
    int *displs = malloc(npes * sizeof(int));
    int base = total_registros / npes;
    int resto = total_registros % npes;

    for (int i = 0; i < npes; i++) {
        // Se resto > 0, os primeiros <resto> processos recebem 1 elemento extra.
        sendcounts[i] = base + (i < resto ? 1 : 0);
        // Define a posição que cada processo deve começar a ler
        displs[i] = (i == 0) ? 0 : displs[i-1] + sendcounts[i-1];
    }

    // Aloca memória para armazenar os registros que serão enviados.
    int local_count = sendcounts[myrank];
    Sensor *meus_registros = malloc(sizeof(Sensor) * local_count);

    // Define o tipo MPI para Sensor (estruturado)
    MPI_Datatype MPI_Sensor;
    int blocklengths[6] = {10, 1, 1, 25, 25, 1};
    MPI_Aint disps[6];
    Sensor dummy;
    MPI_Aint base_addr;

    // Calcula os endereços dos campos dentro da struct
    MPI_Get_address(&dummy, &base_addr);
    MPI_Get_address(&dummy.sensor_id, &disps[0]);
    MPI_Get_address(&dummy.data, &disps[1]);
    MPI_Get_address(&dummy.hora, &disps[2]);
    MPI_Get_address(&dummy.cidade, &disps[3]);
    MPI_Get_address(&dummy.bairro, &disps[4]);
    MPI_Get_address(&dummy.temperatura, &disps[5]);

    // Transforma os endereços absolutos em deslocamentos relativos
    for (int i = 0; i < 6; i++) disps[i] -= base_addr;

    MPI_Datatype types[6] = {MPI_CHAR, MPI_INT, MPI_INT, MPI_CHAR, MPI_CHAR, MPI_FLOAT};
    MPI_Type_create_struct(6, blocklengths, disps, types, &MPI_Sensor);
    MPI_Type_commit(&MPI_Sensor);

    // Distribui os registros igualmente
    MPI_Scatterv(todos_registros, sendcounts, displs, MPI_Sensor,
                 meus_registros, local_count, MPI_Sensor, 0, MPI_COMM_WORLD);

    //printf("Processo %d recebeu %d registros.\n", myrank, local_count);

    // Libera recursos
    free(sendcounts);
    free(displs);
    if (myrank == 0) free(todos_registros);

    // Aqui você pode aplicar a ordenação local nos "meus_registros"
    if(myrank==1){
        printf("Antes de ordenar:\n");
        for(int i=0; i<local_count; i++){
            printf("%s %d %06d %s %s %.1f\n",
               meus_registros[i].sensor_id,
               meus_registros[i].data,
               meus_registros[i].hora,
               meus_registros[i].cidade,
               meus_registros[i].bairro,
               meus_registros[i].temperatura);
        }

        printf("Depois de ordenar:\n");
        for(int i=0; i<local_count; i++){
            printf("%s %d %06d %s %s %.1f\n",
               meus_registros[i].sensor_id,
               meus_registros[i].data,
               meus_registros[i].hora,
               meus_registros[i].cidade,
               meus_registros[i].bairro,
               meus_registros[i].temperatura);
        }
    }

    free(meus_registros);
    MPI_Type_free(&MPI_Sensor);
    MPI_Finalize();
    return 0;
}
