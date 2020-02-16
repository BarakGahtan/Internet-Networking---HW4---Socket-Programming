
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>

#include <time.h>

#define PORT "80"
#define BACKLOG 10 

void sigchld_handler(int s);
void *get_in_addr(struct sockaddr *sa);
int calc_process_time(int server, char type, int time);
//int choose_best_server(const char *task);
//int choose_best_server(int* load, const char *task);
void init_arr(int* arr);
void *clients_handler(void* fd);
void update_load(const char *task, int server_num);


pthread_mutex_t load_lock;
pthread_mutex_t task1_lock;
pthread_mutex_t task2_lock;
pthread_mutex_t task3_lock;

int load[3]; // = {0,0,0};
int tasks_server1[10000];
int tasks_server2[10000];
int tasks_server3[10000];
int r1=0;
int r2=0;
int r3=0;
int w1=0;
int w2=0;
int w3=0;
int servers_sockets_fd[3];

	
int main(){

	/* Step 1: Initializing connections with servers */
	struct addrinfo hints;
	struct addrinfo* servers_info[3];

	char* servers[3] = {
    "192.168.0.101",
    "192.168.0.102",
    "192.168.0.103",
	};
	pthread_mutex_init(&load_lock, NULL);
	pthread_mutex_init(&task1_lock, NULL);
	pthread_mutex_init(&task2_lock, NULL);
	pthread_mutex_init(&task3_lock, NULL);

	init_arr(tasks_server1);
	init_arr(tasks_server2);
	init_arr(tasks_server3);

	int j;
	for (j=0; j<3;j++){
		load[j]=0;
	}
	
	// first, load up address structs with getaddrinfo():
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	
	int i;
	for (i=0; i<=2; i++){
		getaddrinfo(servers[i],PORT, &hints,&servers_info[i]); 
		// make a socket for server
		servers_sockets_fd[i] = socket(servers_info[i]->ai_family, servers_info[i]->ai_socktype, 0);
		
		// connectto the server
		connect(servers_sockets_fd[i], servers_info[i]->ai_addr, servers_info[i]->ai_addrlen);
	}
	
	/* Step 2: Establishing socket for listening to clients */
	
	// first, load up address structs with getaddrinfo():
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t addr_size;
	struct addrinfo c_hints, *servinfo, *p;
	int main_clients_socket_fd, new_fd; // listen on main_clients_socket_fd, new connection on new_fd
	 
	 memset(&c_hints, 0, sizeof c_hints);
	 c_hints.ai_family = AF_UNSPEC; // use IPv4 or IPv6, whichever
	 c_hints.ai_socktype = SOCK_STREAM;
	 c_hints.ai_flags = AI_PASSIVE; // fill in my IP for me
	 int rv = getaddrinfo("10.0.0.1", PORT, &c_hints, &servinfo);

	 socklen_t sin_size;
	 struct sigaction sa;
	 int yes=1;
	 char s[INET6_ADDRSTRLEN];
	
	// Shir: not sure whats this loop for
	// loop through all the results and bind to the first we can
	 for(p = servinfo; p != NULL; p = p->ai_next) {
		 if ((main_clients_socket_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
			 perror("server: socket");
			 continue;
		 }
		 if (setsockopt(main_clients_socket_fd, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
			 perror("setsockopt");
			 exit(1);
		 }
		 if (bind(main_clients_socket_fd, p->ai_addr, p->ai_addrlen) == -1) {
			 close(main_clients_socket_fd);
			 perror("server: bind");
			 continue;
		 }
		 break;
	 }
	 
	 freeaddrinfo(servinfo); // all done with this structure
	if (p == NULL) {
		 fprintf(stderr, "server: failed to bind\n");
		 exit(1);
	 }

	 if (listen(main_clients_socket_fd, BACKLOG) == -1) {
		 perror("listen");
		 exit(1);
	 }
	 sa.sa_handler = sigchld_handler; // reap all dead processes
	 sigemptyset(&sa.sa_mask);
	 sa.sa_flags = SA_RESTART;
	 if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		 perror("sigaction");
		 exit(1);
	 }
	 puts("server: waiting for connections...\n");
	 
	 while(1) { // main accept() loop
		 sin_size = sizeof their_addr;
		 new_fd = accept(main_clients_socket_fd, (struct sockaddr *)&their_addr, &sin_size);
		 if (new_fd == -1) {
		 perror("accept");
		 continue;
		}
		 inet_ntop(their_addr.ss_family,get_in_addr((struct sockaddr *)&their_addr),s, sizeof s);
		
		int *i = malloc(sizeof(*i));
		*i=new_fd;		 
		pthread_t thread_id;
		if( pthread_create(&thread_id, NULL, clients_handler ,(void *)i) < 0)
		{
			perror("could not create thread");
			return 1;
		}
	 }
}

void sigchld_handler(int s)
{
	 // waitpid() might overwrite errno, so we save and restore it:
	 int saved_errno = errno;
	 while(waitpid(-1, NULL, WNOHANG) > 0);
	 errno = saved_errno;
}

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
	 if (sa->sa_family == AF_INET) {
	 return &(((struct sockaddr_in*)sa)->sin_addr);
	 }
	 return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

// the main scheduling function
int choose_best_server(const char *task){	
	
	int server1, server2, server3;
	int time=atoi(&task[1]);
	char type=task[0];
	
	pthread_mutex_lock(&load_lock);
		
	server1=load[0]+calc_process_time(0,type,time);
	server2=load[1]+calc_process_time(1,type,time);
	server3=load[2]+calc_process_time(2,type,time);		
	
	if (server1 <= server2 && server1 <= server3){
		// 1 is min
		load[0] = load[0] + calc_process_time(0,type,time);
		pthread_mutex_unlock(&load_lock);
		return 0;
	} else if (server2 <= server3){
		// 2 is min
		load[1] = load[1] + calc_process_time(1,type,time);
		pthread_mutex_unlock(&load_lock);
		return 1;
	} else { 
		// 3 is min
		load[2] = load[2] +calc_process_time(2,type,time);
		pthread_mutex_unlock(&load_lock);
		return 2;
	}
}

int calc_process_time(int server, char type, int time){
	int ans;
	if (server == 2){
		ans = type=='M' ? time : (type=='V' ? time*3 : time*2);
		return ans;
	} else{
		ans = type=='M' ? time*2 : time;
		return ans;
	}
}

void init_arr(int* arr){
	int i;
	for (i=0; i<10000;i++){
		arr[i]=0;
	}
}


/*
 * This is the thread that handles new clients
 * */
void *clients_handler(void* fd){
    int new_fd = *((int *) fd);
    free(fd);

	char buffer[6];
	// 1. Recieve request from client
	recv(new_fd, (void*)buffer, 2, 0);
	buffer[2]='\0';
	int server_num = choose_best_server(buffer);
	int chosen_server = servers_sockets_fd[server_num];

	if(server_num==0){
		pthread_mutex_lock(&task1_lock);
		send(chosen_server, (const void*)buffer, 2, 0);
		tasks_server1[w1]=new_fd;
		w1++;
		pthread_mutex_unlock(&task1_lock);
	}else if (server_num==1){
		pthread_mutex_lock(&task2_lock);
		send(chosen_server, (const void*)buffer, 2, 0);
		tasks_server2[w2]=new_fd;
		w2++;
		pthread_mutex_unlock(&task2_lock);
	}else{
		pthread_mutex_lock(&task3_lock);
		send(chosen_server, (const void*)buffer, 2, 0);
		tasks_server3[w3]=new_fd;
		w3++;
		pthread_mutex_unlock(&task3_lock);
	}
		
	while (recv(chosen_server, (void*)buffer, 2, MSG_PEEK)>0){		
		buffer[2]='\0';
		if(server_num==0){
			pthread_mutex_lock(&task1_lock);
			if (tasks_server1[r1]==new_fd){
				recv(chosen_server, (void*)buffer, 2, 0);
				buffer[2]='\0';
				r1++;
				pthread_mutex_unlock(&task1_lock);
				break;
			}
			pthread_mutex_unlock(&task1_lock);
		} else if (server_num==1){
			pthread_mutex_lock(&task2_lock);
			if (tasks_server2[r2]==new_fd){
				recv(chosen_server, (void*)buffer, 2, 0);
				buffer[2]='\0';
				r2++;
				pthread_mutex_unlock(&task2_lock);
				break;
			}
			pthread_mutex_unlock(&task2_lock);
		} else{
			pthread_mutex_lock(&task3_lock);
			if (tasks_server3[r3]==new_fd){
				recv(chosen_server, (void*)buffer, 2, 0);
				buffer[2]='\0';
				r3++;
				pthread_mutex_unlock(&task3_lock);
				break;
			}
			pthread_mutex_unlock(&task3_lock);
		}
	}

	// 4. Send answer to client and update loads
	update_load(buffer,server_num);
	send(new_fd,(const void*)buffer,2, 0);
	 
	//printf("Closing client socket %d\n", new_fd);
	close(new_fd);
}


void update_load(const char *task, int server_num){	
	int time=atoi(&task[1]);
	char type=task[0];
		pthread_mutex_lock(&load_lock);
	load[server_num]= load[server_num]-calc_process_time(server_num,type,time);
	pthread_mutex_unlock(&load_lock);
}