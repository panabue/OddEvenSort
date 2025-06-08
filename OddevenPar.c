/**************************************************
* Nome do estudante: Theo Torminn Neto Nasser
* Trabalho 1
* Disciplina: Programação Paralela
* Objetivo: Aplicar o algoritmo Odd-Even Sort em uma máquina com
memória distribuída, usando múltiplos processos que se comunicam por meio da biblioteca
MPI, para ordenar grandes volumes de dados e compara-lo com a implementação sequencial do Odd-Even Sort, e
com o algoritmo Quicksort, também na forma sequencial, visando avaliar ganhos de desempenho com a abordagem paralela.
* Como executar: mpirun -np 'x' ./a.out <arquivo de entrada.txt>
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

// Odd-Even Sort sequencial
void odd_even_sort_sequencial(Sensor *dados, int n) {
    int trocou = 1;
    while (trocou) {
        trocou = 0;

        // Fase ímpar
        for (int i = 1; i < n - 1; i += 2) {
            if (dados[i].data > dados[i + 1].data ||
               (dados[i].data == dados[i + 1].data && dados[i].temperatura > dados[i + 1].temperatura)) {
                Sensor tmp = dados[i];
                dados[i] = dados[i + 1];
                dados[i + 1] = tmp;
                trocou = 1;
            }
        }

        // Fase par
        for (int i = 0; i < n - 1; i += 2) {
            if (dados[i].data > dados[i + 1].data ||
               (dados[i].data == dados[i + 1].data && dados[i].temperatura > dados[i + 1].temperatura)) {
                Sensor tmp = dados[i];
                dados[i] = dados[i + 1];
                dados[i + 1] = tmp;
                trocou = 1;
            }
        }
    }
}

void quick_sort_sequencial(Sensor *dados, int q, int r) {
    if(q<r){
        Sensor atual = dados[q];
        int pivo = q;
        for(int i=q+1; i<=r; i++){
            if(dados[i].data < atual.data || 
              (dados[i].data == atual.data && dados[i].temperatura < atual.temperatura)){
                pivo ++;
                Sensor tmp = dados[pivo];
                dados[pivo] = dados[i];
                dados[i] = tmp;
            }
        }
        Sensor tmp = dados[q];
        dados[q] = dados[pivo];
        dados[pivo] = tmp;
        quick_sort_sequencial(dados, q, pivo);
        quick_sort_sequencial(dados, pivo+1, r);
    }
}

int main(int argc, char *argv[]) {
    // Inicializa o MPI
    int npes, myrank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    // Aviso para executar o programa corretamente
    if (argc < 2) {
        if (myrank == 0) {
            printf("Use: %s <arquivo_entrada>\n para executar!!!", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    Sensor *todos_registros = NULL; 
    int total_registros = 0;

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
    int *disloc = malloc(npes * sizeof(int));
    int base = total_registros / npes;
    int resto = total_registros % npes;

    for (int i = 0; i < npes; i++) {
        // Se resto > 0, os primeiros <resto> processos recebem 1 elemento extra.
        sendcounts[i] = base + (i < resto ? 1 : 0);
        // Define a posição que cada processo deve começar a ler
        disloc[i] = (i == 0) ? 0 : disloc[i-1] + sendcounts[i-1];
    }

    // Aloca memória para armazenar os registros que serão enviados.
    int local_count = sendcounts[myrank];
    Sensor *meus_registros = malloc(sizeof(Sensor) * local_count);

    // Ensina o MPI como é a estrutura do tipo Sensor na memória.
    MPI_Datatype MPI_Sensor;
    int blocklengths[6] = {10, 1, 1, 25, 25, 1};
    // Vetor com os deslocamentos (em bytes) dos campos da struct em relação ao seu início.
    MPI_Aint deslocamento[6];
    Sensor dummy;
    MPI_Aint base_addr;

    // Calcula os endereços dos campos dentro da struct
    MPI_Get_address(&dummy, &base_addr);
    MPI_Get_address(&dummy.sensor_id, &deslocamento[0]);
    MPI_Get_address(&dummy.data, &deslocamento[1]);
    MPI_Get_address(&dummy.hora, &deslocamento[2]);
    MPI_Get_address(&dummy.cidade, &deslocamento[3]);
    MPI_Get_address(&dummy.bairro, &deslocamento[4]);
    MPI_Get_address(&dummy.temperatura, &deslocamento[5]);

    // Transforma os endereços absolutos em deslocamentos relativos
    for (int i = 0; i < 6; i++) deslocamento[i] -= base_addr;

    // Cria o sensor estruturado para o MPI
    MPI_Datatype types[6] = {MPI_CHAR, MPI_INT, MPI_INT, MPI_CHAR, MPI_CHAR, MPI_FLOAT};
    MPI_Type_create_struct(6, blocklengths, deslocamento, types, &MPI_Sensor);
    MPI_Type_commit(&MPI_Sensor);

    // Distribui os registros igualmente
    MPI_Scatterv(todos_registros, sendcounts, disloc, MPI_Sensor,
                 meus_registros, local_count, MPI_Sensor, 0, MPI_COMM_WORLD);

    //printf("Processo %d recebeu %d registros.\n", myrank, local_count);

    // Libera recursos
    free(sendcounts);
    free(disloc);

    // Funcao complementar do qsort
    int comparar_sensor(const void *a, const void *b) {
        const Sensor *s1 = (const Sensor *)a;
        const Sensor *s2 = (const Sensor *)b;

        // Ordena por data
        if (s1->data != s2->data)
            return s1->data - s2->data;

        // Em caso de empate, ordena por temperatura
        if (s1->temperatura < s2->temperatura)
            return -1;
        else if (s1->temperatura > s2->temperatura)
            return 1;
        else
            return 0;
    }

    // Ordenação local dos "meus_registros"
    qsort(meus_registros, local_count, sizeof(Sensor), comparar_sensor);

    // Funcao merge para a transposicao Odd-Even Paralela
    void merge(Sensor *local, int local_n, Sensor *recv, int recv_n, int keep_min) {
        Sensor *temp = malloc(sizeof(Sensor) * (local_n + recv_n));
        int i = 0, j = 0, k = 0;

        // Mescla dois blocos de dados (local e recv) no vetor temp que contém todos os elementos ordenados
        while (i < local_n && j < recv_n) {
            if (comparar_sensor(&local[i], &recv[j]) <= 0)
                temp[k++] = local[i++];
            else
                temp[k++] = recv[j++];
        }
        while (i < local_n) temp[k++] = local[i++];
        while (j < recv_n) temp[k++] = recv[j++];

        // Se keep_min for verdadeiro, esse processo deve manter os menores valores
        if (keep_min) {
            memcpy(local, temp, sizeof(Sensor) * local_n);
        } else { // Se keep_min for falso, o processo deve manter os maiores valores
            memcpy(local, &temp[recv_n], sizeof(Sensor) * local_n);
        }

        free(temp);
    }

    // Inicicializa o temporizador
    double inicio, fim;
    MPI_Barrier(MPI_COMM_WORLD);
    inicio = MPI_Wtime();

    // OddEvenSort Paralelizado
    for (int fase = 0; fase < npes; fase++) {
        int parceiro = -1;

        // Fase par: processos pares falam com ímpares à direita
        if (fase % 2 == 0) {
            if (myrank % 2 == 0 && myrank + 1 < npes)
                parceiro = myrank + 1;
            else if (myrank % 2 == 1)
                parceiro = myrank - 1;
        } else { // Fase ímpar: processos ímpares falam com pares à direita
            if (myrank % 2 == 1 && myrank + 1 < npes)
                parceiro = myrank + 1;
            else if (myrank % 2 == 0 && myrank > 0)
                parceiro = myrank - 1;
        }

        // Executa a troca se tem um parceiro válido e o processo tem dados para trocar.
        if (parceiro >= 0 && local_count > 0) {
            // Numero de registros que o processo parceiro possui
            int parceiro_count = 0;
            // Cada processo envia seu número de registros (local_count) e recebe o do parceiro
            MPI_Sendrecv(&local_count, 1, MPI_INT, parceiro, 1,
                        &parceiro_count, 1, MPI_INT, parceiro, 1,
                        MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (parceiro_count > 0) {
                Sensor *recv_buff = malloc(sizeof(Sensor) * parceiro_count);
                // Cada processo envia seus dados locais e recebe os do parceiro no recv_buff
                MPI_Sendrecv(meus_registros, local_count, MPI_Sensor, parceiro, 0,
                            recv_buff,     parceiro_count, MPI_Sensor, parceiro, 0,
                            MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                int keep_min = myrank < parceiro;
                // Mesclagem ordenada dos dados com os do parceiro
                merge(meus_registros, local_count, recv_buff, parceiro_count, keep_min);
                free(recv_buff);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD); // Evita que algum processo inicie uma nova troca antes que os outros estejam prontos.
    }
    
    // Finaliza o temporizador
    MPI_Barrier(MPI_COMM_WORLD);
    fim = MPI_Wtime();

    if (myrank != 0) {
        MPI_Send(&local_count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(meus_registros, local_count, MPI_Sensor, 0, 0, MPI_COMM_WORLD);
    }

    if (myrank == 0) {
        printf("Tempo de execucao do OddEven Paralelo: %.6f segundos\n", fim - inicio);
    }

    // Gera o arquivo de saida com os dados ordenados do OddEven paralelo
    if (myrank == 0) {
        // Abre o arquivo para escrita
        // MUDE O NOME DO ARQUIVO DE SAIDA DEPENDENDO DA QUANTIDADE DE ENTRADA!
        FILE *fp = fopen("saida_100K.txt", "w");
        if (fp == NULL) {
            perror("Erro ao abrir o arquivo de saída");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Escreve seus próprios registros ordenados
        for (int j = 0; j < local_count; j++) {
            fprintf(fp, "%s %d %06d %s %s %.1f\n",
                    meus_registros[j].sensor_id,
                    meus_registros[j].data,
                    meus_registros[j].hora,
                    meus_registros[j].cidade,
                    meus_registros[j].bairro,
                    meus_registros[j].temperatura);
        }

        // Recebe os registros dos outros processos
        for (int i = 1; i < npes; i++) {
            int count;
            MPI_Recv(&count, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (count > 0) {
                Sensor *temp = malloc(sizeof(Sensor) * count);
                MPI_Recv(temp, count, MPI_Sensor, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for (int j = 0; j < count; j++) {
                    fprintf(fp, "%s %d %06d %s %s %.1f\n",
                            temp[j].sensor_id,
                            temp[j].data,
                            temp[j].hora,
                            temp[j].cidade,
                            temp[j].bairro,
                            temp[j].temperatura);
                }
                free(temp);
            }
        }
        fclose(fp);
    }

    // OddEvenSort Sequencial (USE SÓ PARA ENTRADAS < 1.000.000)
    /*if (myrank == 0) {
        // Faz uma cópia de todos_registros antes de ordenar
        Sensor *copia_para_oddeven = malloc(sizeof(Sensor) * total_registros);
        memcpy(copia_para_oddeven, todos_registros, sizeof(Sensor) * total_registros);

        double inicio = MPI_Wtime();
        odd_even_sort_sequencial(copia_para_oddeven, total_registros);
        double fim = MPI_Wtime();
        
        printf("Tempo de execucao do OddEven Sequencial: %.6f segundos\n", fim - inicio);

        FILE *fp_seq = fopen("saida_odd_even_sequencial.txt", "w");
        if (fp_seq == NULL) {
            perror("Erro ao abrir arquivo de saída");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        // Gera o arquivo de saida com os dados ordenados do OddEven sequencial
        for (int i = 0; i < total_registros; i++) {
            fprintf(fp_seq, "%s %d %06d %s %s %.1f\n",
                copia_para_oddeven[i].sensor_id,
                copia_para_oddeven[i].data,
                copia_para_oddeven[i].hora,
                copia_para_oddeven[i].cidade,
                copia_para_oddeven[i].bairro,
                copia_para_oddeven[i].temperatura);
        }
        fclose(fp_seq);
        free(copia_para_oddeven);
    }*/

    //QuickSort Sequencial
    if (myrank == 0) {
        // Faz uma cópia de todos_registros antes de ordenar
        Sensor *copia_para_qsort = malloc(sizeof(Sensor) * total_registros);
        memcpy(copia_para_qsort, todos_registros, sizeof(Sensor) * total_registros);

        double inicio = MPI_Wtime();

        quick_sort_sequencial(copia_para_qsort, 0, total_registros - 1);

        double fim = MPI_Wtime();
        printf("Tempo de execucao do Quicksort Sequencial: %.6f segundos\n", fim - inicio);

        // Gera o arquivo de saida com os dados ordenados do Quicksort sequencial
        FILE *fp_Qseq = fopen("saida_quicksort_sequencial.txt", "w");
        for (int i = 0; i < total_registros; i++) {
            fprintf(fp_Qseq, "%s %d %06d %s %s %.1f\n",
                copia_para_qsort[i].sensor_id,
                copia_para_qsort[i].data,
                copia_para_qsort[i].hora,
                copia_para_qsort[i].cidade,
                copia_para_qsort[i].bairro,
                copia_para_qsort[i].temperatura);
        }
        fclose(fp_Qseq);
        free(copia_para_qsort);
    }

    free(todos_registros);
    free(meus_registros);
    MPI_Type_free(&MPI_Sensor);
    MPI_Finalize();
    return 0;
}
