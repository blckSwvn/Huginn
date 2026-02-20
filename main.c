#include "liburing/io_uring.h"
#include "version.h"
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <liburing.h>
#include <pthread.h>
#include <threads.h>

enum{
	WAKEUP = 0,
};

volatile sig_atomic_t stop = 0;
void signal_handler(int signum){
	stop = 1;
}

thread_local struct io_uring *ring;
void work(){
	while(1){
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(ring, &cqe);
		if(cqe->user_data == WAKEUP){
			return;
		}
	}
}

void *worker(void *arg){
	ring = arg;
	work();
	return NULL;
}

int main(){
	printf("Huginn %s\n", VERSION);
	signal(SIGINT, signal_handler);
	int n = 1;
	struct io_uring rings[n];
	pthread_t tid[n];
	for(uint32_t i = 0; i < n; i++){
		pthread_create(&tid[i], NULL, worker, &rings[n]);
	}

	while(!stop){
	}

	for(uint32_t i = 0; i < n; i++)
		pthread_join(tid[n], NULL);
	printf("Huginn Closed\n");
}
