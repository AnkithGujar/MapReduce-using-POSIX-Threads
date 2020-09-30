#include<stdio.h>
#include<unistd.h>
#include<sys/wait.h>
#include<errno.h>
#include<stdlib.h>
#include<limits.h>
#include<string.h>
#include<sys/types.h>
#include<stdbool.h>
#include<pthread.h>
#include<sys/mman.h>


#define numTup 100
#define maxStrLen 25
#define tupLen 3
#define maxTupValLen 16
#define maxIdLen 5


struct buff_struct
{
    char id[maxIdLen];
    char topic[maxTupValLen];
    int score;
};


struct buff_struct **buffer = {0};

pthread_mutex_t *mtx;
pthread_cond_t *empty;
pthread_cond_t *fill;

pthread_mutexattr_t mtx_attr;
pthread_condattr_t empty_attr;
pthread_condattr_t fill_attr;

int num_slots, num_reducer;
int *pointer = {0};
int *in_use = {0};
char **ids = {0};
int *FLAG;


void mapper(int num_reducer, int num_slots)
{    
    FILE *fp1 = stdin;
    
    char ch[maxStrLen];
    
    char arrfull[tupLen][maxTupValLen];
    
    char x, y;
    
    x = getc(fp1);
    
    int i = 0;
    int j = 0;
    int s;
    
    char arr1[maxTupValLen], arr2[maxTupValLen], temp[maxTupValLen];
    int arr3;
    
    while(x != EOF)
    {
        fscanf(fp1, "%s", ch);
        
        char temp[maxStrLen] = "";        
        *temp = x;
        
        strcat(temp, ch);

        char* token1;
        char* rest1 = temp;
        
        while ((token1 = strtok_r(rest1, ",", &rest1)))
        {
            char* token2;
            char* rest2 = token1;
            
            while ((token2 = strtok_r(rest2, "(", &rest2)))
            {
                char* token3;
                char* rest3 = token2;
                
                while ((token3 = strtok_r(rest3, ")", &rest3)))
                {
                    char* token4;
                    char* rest4 = token3;
                    
                    while ((token4 = strtok_r(rest4, " ", &rest4)))
                    {
                        int reset = 0;
                        
                        strcpy(arrfull[i], token4);
                        
                        if(i == 2)
                        {
                            strcpy(arr1, arrfull[0]);
                            
                            char temp[maxTupValLen];
                            strcpy(temp, arrfull[2]);            
                            for(int k=strlen(arrfull[2]); k<maxTupValLen-1; k++)
                            {
                                strcpy(temp, strcat(temp, " "));
                            }
                            strcpy(arr2, temp);
                            
                            if(!strcmp(arrfull[1], "P"))
                                arr3 = 50;
                            else if(!strcmp(arrfull[1], "L"))
                                arr3 = 20;
                            else if(!strcmp(arrfull[1], "D"))
                                arr3 = -10;
                            else if(!strcmp(arrfull[1], "C"))
                                arr3 = 30;
                            else if(!strcmp(arrfull[1], "S"))
                                arr3 = 40;
                            
                            pthread_mutex_lock(mtx);

                            for(int abc=0; abc<num_reducer; abc++)
                            {
                                if(strcmp(ids[abc], arr1)==0)
                                {
                                    while(pointer[abc] == num_slots)
                                    {
                                        pthread_cond_wait(fill, mtx);
                                    }
                                    break;
                                }
                            }
                            
                            if(j==0)
                            {
                                strcpy(buffer[0][pointer[0]].id, arr1);
                                strcpy(buffer[0][pointer[0]].topic, arr2);
                                buffer[0][pointer[0]].score = arr3;
                                pointer[0] += 1;
                                in_use[0] = 1;
                                strcpy(ids[0], arr1);
                            }
                            else
                            {                                
                                int a=0;
                                while(a < num_reducer)
                                {                               
                                    if(((strcmp(ids[a], arr1)!=0)) && (in_use[a]==0))
                                    {
                                        strcpy(buffer[a][pointer[a]].id, arr1);
                                        strcpy(buffer[a][pointer[a]].topic, arr2);
                                        buffer[a][pointer[a]].score = arr3;
                                        pointer[a] += 1;
                                        in_use[a] = 1;
                                        strcpy(ids[a], arr1);                                                                           
                                        break;
                                    }
                                    else if(strcmp(ids[a], arr1)==0)
                                    {
                                        strcpy(buffer[a][pointer[a]].id, arr1);
                                        strcpy(buffer[a][pointer[a]].topic, arr2);
                                        buffer[a][pointer[a]].score = arr3;
                                        pointer[a] += 1;                                
                                        break;
                                    }
                                    a += 1;
                                }
                            }

                            pthread_cond_broadcast(empty);
                            pthread_mutex_unlock(mtx); 
                            
                            j += 1;
                            i = 0;
                            reset = 1;
                        }
                        if(!reset)
                        {
                            i += 1;
                        }
                    }
                }
            }
        }
        x = getc(fp1);
        FILE *fptemp = fp1;
        y = getc(fptemp);
        if(y == EOF)
            x = y;
    }
    
    pthread_mutex_lock(mtx);
    *FLAG = 0;
    pthread_cond_broadcast(empty);
    pthread_mutex_unlock(mtx);
}


void reducer(int tid)
{
    char arr1[numTup][maxTupValLen], arr2[numTup][maxTupValLen];
    int arr3[numTup];
    
    int j=0;
    
    while(*FLAG || pointer[tid])
    {       
        pthread_mutex_lock(mtx);
        
        while(*FLAG && (pointer[tid]==0))
        {   
            pthread_cond_wait(empty, mtx);
        }
        
        int countmadhu = 0;
        
        while(pointer[tid] > 0)
        {
            strcpy(arr1[j], buffer[tid][pointer[tid]-1].id);
            strcpy(arr2[j], buffer[tid][pointer[tid]-1].topic);
            arr3[j] = buffer[tid][pointer[tid]-1].score;

            pointer[tid] -= 1;
            j += 1;
        }
        
        pthread_cond_signal(fill);
        
        pthread_mutex_unlock(mtx);
    }
    
    pthread_mutex_lock(mtx);
    
    if(j != 0)
    {
        for(int k=0; k<j; k++)
        {
            for(int l=k+1; l<j; l++)
            {
                if((strcmp(arr2[k], arr2[l])==0) && ((strcmp(arr1[k], arr1[l])==0)))
                {
                    arr3[k] += arr3[l]; 
                }
            }
        }

        for(int k=0; k<j; k++)
        {
            if(k>0)
            {   
                int flag = 1;
                for(int l=k-1;l>=0;l--)
                {
                    if((strcmp(arr2[k], arr2[l])==0) && ((strcmp(arr1[k], arr1[l])==0)))
                        flag = 0;
                }
                if(flag)
                {
                    printf("(%s,", arr1[k]);
                    printf("%s,", arr2[k]);
                    printf("%d)\n", arr3[k]);
                }
            }
            else
            {
                printf("(%s,", arr1[k]);
                printf("%s,", arr2[k]);
                printf("%d)\n", arr3[k]);
            }
        }
    }
    pthread_mutex_unlock(mtx);
}


int main(int argc, char *argv[])
{    
    setbuf(stdout, NULL);

    num_slots = atoi(argv[1]);
    num_reducer = atoi(argv[2]);
    
    FLAG = (int *)mmap(NULL, sizeof(int),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    *FLAG = 1;
    pointer = (int *)mmap(NULL, num_reducer*sizeof(int),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    in_use = (int *)mmap(NULL, num_reducer*sizeof(int),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);

    ids = (char **)mmap(NULL, num_reducer*sizeof(char *),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    buffer = (struct buff_struct **)mmap(NULL, num_reducer*sizeof(struct buff_struct *),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    for(int i=0; i<num_reducer; i++)
    {
        ids[i] = (char *)mmap(NULL, maxIdLen*sizeof(char),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
        buffer[i] = (struct buff_struct *)mmap(NULL, num_slots*sizeof(struct buff_struct),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    }

    mtx = mmap(NULL, sizeof(pthread_mutex_t),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    pthread_mutexattr_init(&mtx_attr);
    pthread_mutexattr_setpshared(&mtx_attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(mtx, &mtx_attr);

    empty = mmap(NULL, sizeof(pthread_cond_t),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    pthread_condattr_init(&empty_attr);
    pthread_condattr_setpshared(&empty_attr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(empty, &empty_attr);

    fill = mmap(NULL, sizeof(pthread_cond_t),PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
    pthread_condattr_init(&fill_attr);
    pthread_condattr_setpshared(&fill_attr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(fill, &fill_attr);
    
    
    for(int i=0; i<num_reducer; i++)
    {
        if(fork()==0)
        {
            reducer(i);
            pthread_exit(NULL);
        }
    }

    mapper(num_reducer, num_slots);

    for(int i=0; i<num_reducer; i++)
    {
        wait(NULL);
    }
    pthread_exit(NULL);
}
