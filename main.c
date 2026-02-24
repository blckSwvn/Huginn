#include "bits/time.h"
#include "liburing/io_uring.h"
#include "picohttpparser/picohttpparser.h"
#include "netinet/in.h"
#include "strings.h"
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
#include <stdlib.h>
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
	IGNORE,
	LOG,
	TIMER
};

#define C2B_MAX 32000
#define B2C_MAX 16000
struct conn{
	struct conn *next;
	struct conn *prev;
	void *c2b_buf;
	void *b2c_buf;
	uint64_t timestamp;
	size_t c2b_used;
	size_t c2b_off;
	size_t b2c_used;
	size_t b2c_off;
	int fd;
	int b_fd;
	enum{
		CLOSE = 1 >> 0,
		KEEP_ALIVE = 1 >> 1,
		CANCEL = 1 >> 2,
	}status;
};


enum conn_tag{//stored in the 3 lowest bits of cqe->res
	CONNECT = 0, //000
	C2B_READ= 1, //001
	C2B_WRITE=2, //010
	B2C_READ= 3, //011
	B2C_WRITE=4, //100
};
#define TAG_BITS 3
#define TAG_MASK ((1UL << TAG_BITS) -1)
inline uint64_t tag_conn(struct conn *ptr, uint64_t tag){
	return ((uint64_t)(uintptr_t)ptr & ~TAG_MASK) | (tag & TAG_MASK);
}

inline uint64_t get_tag(uint64_t ptr){
	return (uintptr_t)ptr & TAG_MASK;
}

inline struct conn *untag_conn(uint64_t ptr){
	return (struct conn *)((uintptr_t)ptr & ~TAG_MASK);
}

thread_local struct conn *used_head = NULL;
thread_local struct conn *used_tail = NULL;
thread_local struct conn *free_head = NULL;
void pop_used(struct conn *c){
	if(c->next)c->next->prev = c->prev;
	if(c->prev)c->prev->next = c->next;
	if(used_head == c)used_head = c->next;
	if(used_tail == c)used_tail = c->prev;
}

void insert_used(struct conn *c){
	c->prev = NULL;
	c->next = used_head;
	if(used_head)used_head->prev = c;
	else used_tail = c;
}

void insert_free(struct conn *c){
	c->next = free_head;
	free_head = c;
	//freelist is singly linked new nodes only insert to head and we only pop from head so c->prev should not be used for freelist
}
//pop_free is not needed as a function

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
	if(logs.tail == 1){
		struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
		sqe->user_data = LOG;
		io_uring_prep_write(sqe, STDOUT_FILENO, &logs.buf, sizeof(log_entry)*logs.tail, 0);
		io_uring_submit(&log_ring);
		logs.tail = 0;
	}
#endif
}

_Atomic uint64_t now = 0;
_Atomic uint64_t timeout = 0;

thread_local struct io_uring *ring;
void cancel_conn(struct conn *c){
	close(c->fd);
	close(c->b_fd);
	for(uint32_t i = 0; i < B2C_WRITE; i++){
		struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
		sqe->user_data = IGNORE;
		uint64_t user_data = tag_conn(c, i);
		io_uring_prep_cancel(sqe, &user_data, 0);
	}
	io_uring_submit(ring);
}

#define NSEC_IN_SEC 1000000000ULL
#define TIMEOUT_NSEC (10ULL * NSEC_IN_SEC) //every 10seconds
#define TICK_NSEC    100000000ULL //every 0.1seconds
#define SEC_TIMEOUT 10
#define NSEC_TIMEOUT 0
static struct __kernel_timespec ts = {
	.tv_sec = TICK_NSEC / NSEC_IN_SEC,
	.tv_nsec =  TICK_NSEC % NSEC_IN_SEC
};

#define ENTRIES 256
void work(){
	int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	int one = 1;
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	struct sockaddr_in l_addr;
	memset(&l_addr, 0, sizeof(l_addr));
	l_addr.sin_family = AF_INET;
	l_addr.sin_port = htons(PORT);
	l_addr.sin_addr.s_addr = INADDR_ANY;
	if(bind(listen_fd, (struct sockaddr*)&l_addr, sizeof(l_addr))){
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

	sqe = io_uring_get_sqe(ring);
	sqe->user_data = TIMER;
	io_uring_prep_timeout(sqe, &ts, 0, 0);

	io_uring_submit(ring);
	while(1){
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(ring, &cqe);
		if(!cqe){
			write_log("%zu:cqe:NULL\n",now);
			return;
		}
		if(cqe->user_data == LISTEN){
			if(cqe->res < 0){
				write_log("%zu:LISTEN:res:%s\n",now,strerror(-cqe->res));
				continue;
			}
			struct conn *c;
			if(free_head){
				c = free_head;
				free_head = free_head->next;
			}else{
				c = malloc(sizeof(struct conn));
				if(!c){
					write_log("malloc failed\n");
					return;
				}
				c->b2c_buf = malloc(B2C_MAX);
				if(!c->b2c_buf){
					write_log("malloc failed\n");
					return;
				}
				c->c2b_buf = malloc(C2B_MAX);
				if(!c->c2b_buf){
					write_log("malloc failed\n");
					return;
				}
			}
			c->c2b_used = 0;
			c->b2c_used = 0;
			c->c2b_off = 0;
			c->b2c_off = 0;
			c->status = CLOSE;
			c->fd = cqe->res;
			insert_used(c);

			c->b_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
			if(c->b_fd < 0){
				write_log("%zu:LISTEN:b_fd:%s",now,strerror(-c->b_fd));
				continue;
			}
			struct sockaddr_in addr;
			memset(&addr, 0, sizeof(addr));
			addr.sin_family = AF_INET;
			addr.sin_port = htons(8080);
			inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr.s_addr);

			sqe = io_uring_get_sqe(ring);
			sqe->user_data = tag_conn(c, CONNECT);
			io_uring_prep_connect(sqe, c->b_fd, (struct sockaddr*)&addr, sizeof(addr));
		}else if(cqe->user_data == WAKEUP){
			sqe = io_uring_get_sqe(ring);
			io_uring_prep_cancel(sqe, (uint64_t*)LISTEN, 0);
			io_uring_submit(ring);
			sqe = io_uring_get_sqe(&log_ring);
			io_uring_prep_write(sqe, STDOUT_FILENO, &logs.buf, sizeof(log_entry)*logs.tail, 0);
			io_uring_submit(&log_ring);
			struct conn *c = used_head;
			while(c){
				struct conn *next = c->next;
				free(c->b2c_buf);
				free(c->c2b_buf);
				free(c);
				c = next;
			}
			c = free_head;
			while(c){
				struct conn *next = c->next;
				free(c->b2c_buf);
				free(c->c2b_buf);
				free(c);
				c = next;
			}
			return;
		}else if(cqe->user_data == TIMER){
			struct conn *c = used_tail;
			while(c && c->timestamp < now){
				write_log("%zu:TIMER\n",now);
				struct conn *prev = c->prev;
				close(c->fd);
				close(c->b_fd);
				for(uint32_t i = 0; i < B2C_WRITE; i++){
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = IGNORE;
					io_uring_prep_cancel(sqe, (void*)tag_conn(c, i), 0);
				}
				pop_used(c);
				insert_free(c);
				c = prev;
			}
			sqe = io_uring_get_sqe(ring);
			sqe->user_data = cqe->user_data;
			io_uring_prep_timeout(sqe, &ts, 0, 0);
		}else if(cqe->user_data == IGNORE){
			io_uring_cqe_seen(ring, cqe);
			continue;
		}else{
			struct conn *c = untag_conn(cqe->user_data);
			uint64_t tag = get_tag(cqe->user_data);
			if(c->status & CANCEL){
				write_log("CANCEL\n");
				io_uring_cqe_seen(ring, cqe);
				continue;
			}else if(tag == CONNECT){
				write_log("CONNECT\n");
				if(cqe->res < 0){
					write_log("%zu:CONNECTING:res:%s\n",strerror(-cqe->res));
					io_uring_cqe_seen(ring, cqe);
					continue;
				}
				sqe = io_uring_get_sqe(ring);
				sqe->user_data = tag_conn(c, C2B_READ);
				io_uring_prep_recv(sqe, c->fd, c->c2b_buf, C2B_MAX, 0);
				c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
			}else if(tag == C2B_READ){
			write_log("C2B_READ\n");
				if(cqe->res <0){
					write_log("%zu:C2B_READ:res:%s\n",strerror(-cqe->res));
					io_uring_cqe_seen(ring, cqe);
					continue;
				}
				c->c2b_used += cqe->res;
				int minor_version;
				const char *method, *path;
				size_t method_len, path_len;
				size_t num_headers = 16;
				struct phr_header headers[num_headers];
				int pret = phr_parse_request(c->c2b_buf, c->c2b_used,
				 			&method, &method_len,
				 			&path, &path_len,
							&minor_version, headers, &num_headers, 0);
				if(pret == -1){
					write_log("pret==-1\n");
					c->status |= CANCEL;
					cancel_conn(c);
				}else if(pret == -2){
					write_log("pret==-2\n");
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = cqe->user_data;
					io_uring_prep_recv(sqe, c->fd, c->c2b_buf, C2B_MAX-c->c2b_used, 0);
				}else{
					for(uint32_t i = 0; i < num_headers; i++){
						if(headers[i].name_len == 10 &&
						strncasecmp(headers[i].value, "Connection", 10) == 0){
							if(strncasecmp(headers[i].value, "Close", 5) == 0)
								c->status |= CLOSE;
							else
								c->status |= KEEP_ALIVE;
							break;
						}
					}
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = tag_conn(c, C2B_WRITE);
					io_uring_prep_send(sqe, c->b_fd, c->c2b_buf, c->c2b_used, 0);
					c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
				}
			}else if(tag == C2B_WRITE){
			write_log("C2B_WRITE\n");
				if(cqe->res < 0){
					write_log("%zu:C2B_WRITE:res:%s\n",now,strerror(-cqe->res));
					io_uring_cqe_seen(ring, cqe);
					continue;
				}
				c->c2b_off += cqe->res;
				sqe = io_uring_get_sqe(ring);
				if(c->c2b_off < c->c2b_used){
					sqe->user_data = cqe->user_data;
					io_uring_prep_send(sqe, c->fd, c->c2b_buf+c->c2b_off, c->c2b_used-c->c2b_off, 0);
					c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
				}else{
					sqe->user_data = tag_conn(c, B2C_READ);
					io_uring_prep_recv(sqe, c->b_fd, c->b2c_buf, B2C_MAX, 0);
					c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
				}
			}else if(tag == B2C_READ){
				write_log("B2C_READ\n");
				if(cqe->res < 0){
					write_log("%zu:B2C_READ:res:%s\n",now,strerror(-cqe->res));
					io_uring_cqe_seen(ring, cqe);
					continue;
				}
				c->b2c_used += cqe->res;
				int minor_version, status;
				const char *msg;
				size_t msg_len;
				size_t num_headers = 100;
				struct phr_header headers[num_headers];
				int pret = phr_parse_response(c->b2c_buf, c->b2c_used,
							  &minor_version, &status,
							  &msg, &msg_len, headers, &num_headers, 0);
				if(pret == -2){
					write_log("pret==-2\n");
					c->b2c_off += cqe->res;
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = cqe->user_data;
					io_uring_prep_recv(sqe, c->b_fd, c->b2c_buf+c->b2c_off, B2C_MAX-c->b2c_off, 0);
					io_uring_cqe_seen(ring, cqe);
					c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
					continue;
				}
				else if(pret < 0){
					write_log("pret==-1\n");
					c->status |= CANCEL;
					cancel_conn(c);
					io_uring_cqe_seen(ring, cqe);
					continue;
				}

				ssize_t cont_len = 0;
				for(size_t i = 0; i < num_headers; i++){
					if(headers[i].name_len == 14 &&
					strncasecmp(headers[i].name, "Content-Length", 14) == 0){
						const char *v = headers[i].value;
						size_t vlen = headers[i].value_len;
						for(size_t n = 0; n < vlen; n++){
							if(v[n] < '0' || v[n] > '9'){//condition should never happen due to picohttpparser pret beeing == -1 if invalid header
								write_log("failed Cont_len\n");
								cont_len = -1;
								break;
							}
							cont_len = cont_len * 10 + (v[n] - '0');
						}
					}
				}
				if(cont_len > c->b2c_used - pret){
					c->b2c_off += cqe->res;
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = cqe->user_data;
					io_uring_prep_recv(sqe, c->b_fd, c->b2c_buf+c->b2c_off, B2C_MAX-c->b2c_off, 0);
				}else{
					c->b2c_off = 0;
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = tag_conn(c, B2C_WRITE);
					io_uring_prep_send(sqe, c->fd, c->b2c_buf, c->b2c_used, 0);
				}
				c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
			}else if(tag == B2C_WRITE){
				write_log("B2C_WRITE\n");
				if(cqe->res < 0){
					write_log("%zu:B2C_WRITE:res:%s\n",now,strerror(-cqe->res));
					io_uring_cqe_seen(ring, cqe);
					continue;
				}
				c->b2c_off += cqe->res;
				if(c->b2c_used > c->b2c_off){
					sqe = io_uring_get_sqe(ring);
					sqe->user_data = cqe->user_data;
					io_uring_prep_send(sqe, c->fd, c->b2c_buf+c->b2c_off, c->b2c_used-c->b2c_off, 0);
					c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
				}else{
					if(c->status & KEEP_ALIVE){
						sqe = io_uring_get_sqe(ring);
						sqe->user_data = C2B_READ;
						io_uring_prep_recv(sqe, c->fd, c->c2b_buf, C2B_MAX, 0);
						c->c2b_used = 0;
						c->c2b_off = 0;
						c->b2c_used = 0;
						c->c2b_off = 0;
						c->timestamp = atomic_load_explicit(&timeout, memory_order_relaxed);
					}else{
						c->status |= CANCEL;
						cancel_conn(c);
						io_uring_cqe_seen(ring, cqe);
						continue;
					}
				}
			}
		}
		io_uring_cqe_seen(ring, cqe);
		io_uring_submit(ring);
	}
}

void *worker(void *arg){
	logs.tail = 0;
	ring = arg;
	io_uring_queue_init(ENTRIES, ring, 0);
	work();
	return NULL;
}

int main(){
	printf("Huginn %s\n", VERSION);
	signal(SIGINT, signal_handler);

	io_uring_queue_init(ENTRIES, &log_ring, 0);
	int n = sysconf(_SC_NPROCESSORS_ONLN);
	struct io_uring rings[n];
	pthread_t tid[n];
	for(uint32_t i = 0; i < n; i++){
		pthread_create(&tid[i], NULL, worker, &rings[i]);
	}

	struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
	sqe->user_data = TIMER;
	io_uring_prep_timeout(sqe, &ts, 0, 0);
	io_uring_submit(&log_ring);

	while(!stop){
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(&log_ring, &cqe);
		if(!cqe)return 1;
		if(cqe->user_data == TIMER){
			struct io_uring_sqe *sqe = io_uring_get_sqe(&log_ring);
			if(!sqe)return 1;
			sqe->user_data = TIMER;
			io_uring_prep_timeout(sqe, &ts, 0, 0);
			io_uring_submit(&log_ring);
			struct timespec tp = {0};
			clock_gettime(CLOCK_MONOTONIC, &tp);
			atomic_store(&now, tp.tv_sec * NSEC_IN_SEC + tp.tv_nsec);
			atomic_store(&now, (tp.tv_sec+SEC_TIMEOUT) * NSEC_IN_SEC + (tp.tv_nsec+NSEC_TIMEOUT));
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
