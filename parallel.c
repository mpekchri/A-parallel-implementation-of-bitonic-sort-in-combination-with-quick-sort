/*
Bitonic and quick sort parallel combination , using pthreads library












@author : Christophoros Bekos (mpekchri@auth.gr)

*/


#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <math.h>

struct timeval startwtime, endwtime;
double seq_time,parallel_time;


int N = 0;          // data array size
int *a = NULL;      // data array to be sorted
int p =0 ;          // p threads active

const int ASCENDING  = 1;
const int DESCENDING = 0;

struct Pthread_data_sec{
  int lo; 
  int k;
  int dir;
  int length;
};

struct Pthread_data{
  int thread_id;
  int* a;
  int N;
  int step;
  int num_of_threads;
  int no_use_0;       // no use_i integers are used here in order
  int no_use_1;       // Pthread_data struct's size to be a power of 2 (8 floats)
  int no_use_2;       // so it fits better in cache and thus we gain better performance
};

void init(void);
void print(void);
void test(void);
inline void exchange(int i, int j);
void compare(int i, int j, int dir);
int asc(const void *a, const void *b);
int desc(const void *a, const void *b);
void* pre_sort_parallel(void* arg);
void* my_sort_parallel(void* arg);
void my_sort(int N,int step,int num_of_threads);
void pre_sort(int* a,int N,int step,int num_of_threads);
void* compare_parallel(void* arg);
void bitonicMerge(int lo, int cnt, int dir,int num_of_threads);

/** the main program **/ 
int main(int argc, char **argv) {

  if (argc != 3) {
    printf("Usage: %s q p\n  where n=2^q is problem size (power of two) and p is 2^p threads\n", argv[0]);
    exit(1);
  }

  N = 1<<atoi(argv[1]);
  p = atoi(argv[2]);
  printf("Running for %d size matrix and %d threads. . .\n", N,p);

  a = (int *) malloc(N * sizeof(int));
  
  init();
  gettimeofday( &startwtime, NULL );
  qsort( a, N, sizeof( int ), asc );
  gettimeofday( &endwtime, NULL );
  seq_time = (double)( ( endwtime.tv_usec - startwtime.tv_usec ) / 1.0e6 + endwtime.tv_sec - startwtime.tv_sec );
  printf( "Qsort time   = %f\n", seq_time );
  test();

  init();
  /* 2*p active threads , each one of them uses qsort,
    in order to sort a part of array (a) in ascending or descending order
    threads with even thread_id -> ascending order
    threads with odd thread_id -> descending order
    This procedure is done by pre_sort() function
  */
  int num_of_threads = 2*p;           
  int step = N/num_of_threads;
  gettimeofday( &startwtime, NULL );
  pre_sort(a,N,step,num_of_threads);
  /* Since pre_sort() finished we now use p active threads
    each one of them will use bitonicMerge() function in order to sort a part of our array (a),
    whose elements consist of a bitonic sequence (check report.pdf)
    Then number of active threads is divided by two (until we reach 1 active thread)
    This procedure is done recursively by my_sort() function
  */
  num_of_threads = p;
  step = N/num_of_threads;  // step here is 2 * previous_step
  my_sort(N,step,num_of_threads);
  gettimeofday( &endwtime, NULL );
  parallel_time = (double)( ( endwtime.tv_usec - startwtime.tv_usec ) / 1.0e6 + endwtime.tv_sec - startwtime.tv_sec );
  printf( "My sort time = %f\n", parallel_time );
  test();

  printf( "Accelaration ratio =  %f % \n", ( (2*(seq_time-parallel_time) / (seq_time+parallel_time) )*100 ));
  
}

/** -------------- MAIN SUB-PROCEDURES  ----------------- **/ 


/** Procedure bitonicMerge() 
   It recursively sorts a bitonic sequence in ascending order, 
   if dir = ASCENDING, and in descending order otherwise. 
   The sequence to be sorted starts at index position lo,
   the parameter cnt is the number of elements to be sorted. 
 **/

 void bitonicMerge(int lo, int cnt, int dir,int num_of_threads) {
    int k = cnt/2;
    if(k<1){
      return;
    }
    for (int i=lo; i<lo+k; i++){
      compare(i, i+k, dir);
    }
    bitonicMerge(lo, k, dir,0);
    bitonicMerge(lo+k, k, dir,0);   
  }


/** Procedure compare_parallel() 
   It is used by each thread , in order to compare and sort 
   a number of elements of array a
 **/
void* compare_parallel(void* arg){
  struct Pthread_data_sec* data = (struct Pthread_data_sec*) arg;
  for (int i=data->lo; i<data->length; i++){
    compare(i, i+data->k, data->dir);
  }
  pthread_exit(NULL);
}

/** function my_sort() 
    Starts num_of_threads different threads , each one will be responsible
    to sort (in ascending/descending order) a part of array a,
    in an recursive way , calling my_sort_parallel() function .
    In each call , function divides num_of_thread by two , in order to avoid
    having a huge amount of active threads , which ,ofcourse , would cause poor pefrormance
 **/

void my_sort(int N,int step,int num_of_threads){
  // num_of_threads must be equal to N/step
  struct Pthread_data data[num_of_threads];
  for(int i=0; i<num_of_threads; i++){
    data[i].thread_id = i;
    data[i].a = a;
    data[i].N = N;
    data[i].step = step;
    data[i].num_of_threads = num_of_threads;
  }
  
  pthread_t* threads;
  pthread_attr_t attr;
  
  while(step <= N){
    threads = (pthread_t*)malloc(num_of_threads*sizeof(pthread_t));
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    for(int i=0; i<num_of_threads; i++){
      pthread_create(&threads[i], &attr, my_sort_parallel, (void *)(&data[i]));
    }
    
    for(int i=0; i<num_of_threads; i++){
      pthread_join(threads[i], NULL);
    }
    

    step = step*2;
    for(int i=0; i<num_of_threads; i++){
      data[i].step = step;
    }

    num_of_threads = num_of_threads/2;
    pthread_attr_destroy(&attr);
    free(threads);
  }
  
}

/** function my_sort() 
    Calls recursively bitonicMerge() to sort the appropriate parts of array a
    in ascending or descending order
 **/
void* my_sort_parallel(void* arg){
	struct Pthread_data* data = (struct Pthread_data*) arg;
  int i = data->thread_id;
  int step = data->step;
  int N = data->N;
  
  if(step>N){
		pthread_exit(NULL);
  }
	if(i%2==0){
    bitonicMerge(i*step,step,ASCENDING,data->num_of_threads);
  }else{
    bitonicMerge(i*step,step,DESCENDING,data->num_of_threads);
  }
  
  pthread_exit(NULL);
}

/** function my_sort() 
    Creates an amount of threads to sort some individual parts of array a
 **/
void pre_sort(int* a,int N,int step,int num_of_threads){
  // num_of_threads must be equal to N/step
  struct Pthread_data data[num_of_threads];
  for(int i=0; i<num_of_threads; i++){
    data[i].thread_id = i;
    data[i].a = a;
    data[i].N = N;
    data[i].step = step;
  }
  
  void* status;
  pthread_t threads[num_of_threads];
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  
  for(int i=0; i<num_of_threads; i++){
    pthread_create(&threads[i], &attr, pre_sort_parallel, (void *)(&data[i]));
  }
  
  pthread_attr_destroy(&attr);
  for(int i=0; i<num_of_threads; i++){
    pthread_join(threads[i], &status);
  }
  
}

/** function my_sort() 
    Is used by each thread to sort a part of array a, in ascending/descending order
    in accordance with thread_id
 **/
void* pre_sort_parallel(void* arg){
  
  struct Pthread_data* data = (struct Pthread_data*) arg;
  int i = data->thread_id;
  int step = data->step;
  int N = data->N;
  if(i%2 == 0){
    // asceding 
    // quisort(a,index*step,index*step+step)
    qsort(&a[i*step],step, sizeof( int ), asc );
  }
  else{
    // descending
    // quisort(a,index*step,index*step+step)
    qsort(&a[i*step],step, sizeof( int ), desc );
  }
  
  pthread_exit(NULL);
}


/** -------------- HELPFUL SUB-PROCEDURES  ----------------- **/ 

/** procedure test() : verify sort results **/
void test() {
  int pass = 1;
  int i;
  for (i = 1; i < N; i++) {
    pass &= (a[i-1] <= a[i]);
  }

  printf(" TEST %s\n",(pass) ? "PASSed" : "FAILed");
}


/** procedure init() : initialize array "a" with data **/
void init() {
  int i;
  for (i = 0; i < N; i++) {
    a[i] = rand() % N; // (N - i);
  }
}

/** procedure  print() : print array elements **/
void print() {
  int i;
  for (i = 0; i < N; i++) {
    printf("%d\n", a[i]);
  }
  printf("\n");
}


/** INLINE procedure exchange() : pair swap **/
inline void exchange(int i, int j) {
  int t;
  t = a[i];
  a[i] = a[j];
  a[j] = t;
}

/** procedure compare() 
   The parameter dir indicates the sorting direction, ASCENDING 
   or DESCENDING; if (a[i] > a[j]) agrees with the direction, 
   then a[i] and a[j] are interchanged.
**/
inline void compare(int i, int j, int dir) {
  if (dir==(a[i]>a[j])) 
    exchange(i,j);
}

inline int asc( const void *a, const void *b ){
  int* arg1 = (int *)a;
  int* arg2 = (int *)b;
  if( *arg1 < *arg2 ) return -1;
  else if( *arg1 == *arg2 ) return 0;
  return 1;
}

inline int desc( const void *a, const void *b ){
  int* arg1 = (int *)a;
  int* arg2 = (int *)b;
  if( *arg1 > *arg2 ) return -1;
  else if( *arg1 == *arg2 ) return 0;
  return 1;
}
