#include "bits/time.h"
#include "liburing/io_uring.h"
#include "netinet/in.h"
#include "sys/socket.h"
#include "time.h"
#include "version.h"
#include <signal.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdint.h>
#include <liburing.h>
#include <pthread.h>
#include <string.h>
#include <threads.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdatomic.h>
#include <unistd.h>

#define PORT 1234
#define DEBUG 1

enum{
	WAKEUP = 0,
	LISTEN = 1,
	CLIENT = 2,
};

enum{
	LOG,
	TIMER
};

volatile sig_atomic_t stop = 0;
void signal_handler(int signum){
	stop = 1;
}

typedef struct {
	char msg[64];
}log_entry;
struct log_buffer{
	log_entry buf[UINT8_MAX];
	uint8_t tail;
};

thread_local struct log_buffer logs;
struct io_uring log_ring;

void write_log(const char *format, ...){
#ifdef DEBUG
	va_list args;
	va_start(args, format);
	vsnprintf(logs.buf[logs.tail].msg, sizeof(logs.buf[logs.tail].msg), format, args);
	logs.tail++;
	va_end(args);
	if(logs.tail == UINT8_MAX-1){
		struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
		sqe->user_data = LOG;
		io_uring_prep_write(sqe, STDOUT_FILENO, &logs.buf, sizeof(log_entry)*logs.tail, 0);
		io_uring_submit(&log_ring);
		logs.tail = 0;
	}
#endif
}

static const char msg[] = "hello world!";
_Atomic uint64_t now = 0;

thread_local struct io_uring *ring;
#define ENTRIES 256
void work(){
	int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	int one = 1;
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(PORT);
	addr.sin_addr.s_addr = INADDR_ANY;
	if(bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr))){
		perror("bind");
		return;
	}
	if(listen(listen_fd, SOMAXCONN)){
		perror("listen");
		return;
	}

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	sqe->user_data = LISTEN;
	io_uring_prep_multishot_accept(sqe, listen_fd, NULL, NULL, 0);
	io_uring_submit(ring);
	while(1){
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(ring, &cqe);
		if(cqe->user_data == LISTEN){
			if(cqe->res < 0){
				write_log("res:%s\n",strerror(-cqe->res));
				continue;
			}
			sqe = io_uring_get_sqe(ring);
			sqe->user_data = CLIENT;
			io_uring_prep_send(sqe, cqe->res, &msg, strlen(msg), 0);
		}else if(cqe->user_data == WAKEUP){
			sqe = io_uring_get_sqe(ring);
			io_uring_prep_cancel(sqe, (uint64_t*)LISTEN, 0);
			io_uring_submit(ring);
			sqe = io_uring_get_sqe(&log_ring);
			io_uring_prep_write(sqe, STDOUT_FILENO, &logs.buf, sizeof(log_entry)*logs.tail, 0);
			io_uring_submit(&log_ring);
			return;
		}
		io_uring_cqe_seen(ring, cqe);
		io_uring_submit(ring);
	}
}

void *worker(void *arg){
	ring = arg;
	io_uring_queue_init(ENTRIES, ring, 0);
	work();
	return NULL;
}

int main(){
	printf("Huginn %s\n", VERSION);
	signal(SIGINT, signal_handler);

	io_uring_queue_init(ENTRIES, &log_ring, 0);

	int n = 1;
	struct io_uring rings[n];
	pthread_t tid[n];
	for(uint32_t i = 0; i < n; i++){
		pthread_create(&tid[i], NULL, worker, &rings[i]);
	}

#define NSEC_IN_SEC 1000000000ULL
#define TIMEOUT_NSEC (10ULL * NSEC_IN_SEC) //every 10seconds
#define TICK_NSEC    100000000ULL //every 0.1seconds
	struct __kernel_timespec ts;
	ts.tv_sec = TICK_NSEC / NSEC_IN_SEC;
	ts.tv_nsec =  TICK_NSEC % NSEC_IN_SEC;
	struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
	io_uring_prep_timeout(sqe, &ts, 0, 0);
	io_uring_submit(&log_ring);

	while(!stop){
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(&log_ring, &cqe);
		if(!cqe)continue;
		if(cqe->user_data == TIMER){
			struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
			if(!sqe)return 1;
			io_uring_prep_timeout(sqe, &ts, 0, 0);
			io_uring_submit(&log_ring);
			struct timespec tp = {0};
			clock_gettime(CLOCK_MONOTONIC, &tp);
			atomic_store(&now, tp.tv_sec * NSEC_IN_SEC + tp.tv_nsec);
		}
		io_uring_cqe_seen(&log_ring, cqe);
	}

	for(uint32_t i = 0; i < n; i++){
		struct io_uring_sqe *sqe = io_uring_get_sqe(&rings[i]);
		sqe->user_data = WAKEUP;
		io_uring_prep_nop(sqe);
		io_uring_submit(&rings[i]);
		pthread_join(tid[i], NULL);
	}
	printf("Huginn Closed\n");
}
