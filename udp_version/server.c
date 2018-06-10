/* Systemy Operacyjne 2018 Patryk Wegrzyn */

#define _BSD_SOURCE
#define _DEFAULT_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <ctype.h>
#include <endian.h>
#include <sys/epoll.h>
#include <signal.h>
#include <time.h>

#ifndef UNIX_PATH_MAX
#define UNIX_PATH_MAX 108
#endif

#define MAX_NO_LOCAL_CLIENTS 15
#define MAX_NO_NETWORK_CLIENTS 15
#define MAX_NO_CLIENTS (MAX_NO_LOCAL_CLIENTS + MAX_NO_NETWORK_CLIENTS)

#define MAX_CLIENT_NAME 16

#define MAX_LINE 1024

// Message type
typedef enum message_type_tag {
    ASSIGNMENT,
    RESULT,
    INTRO,
    ERROR,
    UNREGISTER,
    PING,
    PING_RESPONSE,
    OK
} message_type;

// Socket type
typedef enum socket_type_tag {
    UNIX,
    INET
} socket_type;

// Server assignment sent to clients
typedef struct __attribute__((__packed__)) assignment_tag {
    uint8_t type;
    uint16_t length;
    char op;
    int arg1;
    int arg2;
    int id;
} assignment;

// Server assignment sent to clients
typedef struct __attribute__((__packed__)) assignment_partial_tag {
    char op;
    int arg1;
    int arg2;
    int id;
} assignment_partial;

// Message struct (TLV style)
typedef struct __attribute__((__packed__)) message_tag {
    uint8_t type;
    uint16_t length;
    void* value;
} message;

// Message header struct (TLV style)
typedef struct __attribute__((__packed__)) message_header_tag {
    uint8_t type;
    uint16_t length;
} message_header;

// Represents a connection with a particular client
typedef struct client_tag {
    char name[MAX_CLIENT_NAME];
    struct sockaddr_in inet_addr;
    struct sockaddr_un unix_addr;
    socket_type type;
    int responded;
} client;

// Assingment queue item
struct queue_item {
	assignment *job;
	struct queue_item *next;
};

// Assignment queue
typedef struct assignment_queue_tag {
	struct queue_item *head;
	struct queue_item *tail;
} assignment_queue;

//Globals
int unix_socketfd;
int inet_socketfd;
int epoll_fd;
int assignment_id = 0;
int current_no_clients = 0;
client clients[MAX_NO_CLIENTS];
char *unix_socket_path;
pthread_t connection_check_thread;
pthread_t input_handle_thread;
pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t assignment_mutex = PTHREAD_MUTEX_INITIALIZER;
assignment_queue queue;

// Helper function used to signalize argument errors
void sig_arg_err()
{
    printf("Wrong argument format.\n"
           "Usage: server <UDP port number> <UNIX socket path>\n");
    exit(EXIT_FAILURE);
}

// Assignment queue initialization
void init_assignment_queue(assignment_queue *q)
{
	q->head = q->tail = NULL;
}

// Assignment queue push
void push_assignment_queue(assignment_queue *q, assignment *a)
{
	struct queue_item *item = malloc(sizeof(item));
    if(item == NULL)
    {
        fprintf(stderr, "Error while allocating memory\n");
        exit(EXIT_FAILURE);
    }
	item->job = a;
	item->next = NULL;
	if (q->head == NULL) q->head = q->tail = item;
    else q->tail = q->tail->next = item;
}

// Assignment queue pop
assignment* pop_assignment_queue(assignment_queue *q)
{
	assignment *top;
	if (q->head == NULL) return NULL;
    else 
    {
		top = q->head->job;
		 struct queue_item *next = q->head->next;
		free(q->head);
		q->head = next;
		if (q->head == NULL) q->tail = NULL;
	}
	return top;
}
// Helper function
client* get_unix_client(struct sockaddr_un *addr)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        if(clients[i].type == UNIX && strcmp(clients[i].unix_addr.sun_path, addr->sun_path) == 0 && addr->sun_family == AF_UNIX)
        {
            return &clients[i];
        }
    }
    return NULL;
}

// Helper function
client* get_inet_client(struct sockaddr_in *addr)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        if(clients[i].type == INET && clients[i].inet_addr.sin_addr.s_addr == addr->sin_addr.s_addr && addr->sin_port == clients[i].inet_addr.sin_port)
        {
            return &clients[i];
            printf("Got the same!\n");
        }
    }
    return NULL;
}

// Helper function
struct sockaddr_in* get_inet_client_addr(const char *name)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        if(clients[i].type == INET)
        {
            if(strcmp(clients[i].name, name) == 0)
            {
                return &(clients[i].inet_addr);
            }
        }
    }
    return NULL;
}

// Helper function
struct sockaddr_un* get_unix_client_addr(const char *name)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        if(clients[i].type == UNIX)
        {
            if(strcmp(clients[i].name, name) == 0)
            {
                return &(clients[i].unix_addr);
            }
        }
    }
    return NULL;
}

// Helper function
void copy_inet_addr(struct sockaddr_in *src, struct sockaddr_in *dest)
{
    memcpy((void *)dest, (void *)src, sizeof(*src));
}

// Helper function
void copy_unix_addr(struct sockaddr_un *src, struct sockaddr_un *dest)
{
    memcpy((void *)dest, (void *)src, sizeof(*src));
}

// Peforms clean up of unused resources at the exit of the server process
void perform_cleanup(void)
{
    pthread_mutex_destroy(&client_mutex);
    pthread_mutex_destroy(&assignment_mutex);
    close(epoll_fd);
    shutdown(unix_socketfd, SHUT_RDWR);
    shutdown(inet_socketfd, SHUT_RDWR);
    close(inet_socketfd);
    close(unix_socketfd);
    unlink(unix_socket_path);
}

// Forget a client
void forget_client(const char *name)
{
    printf("Unregistering client %s.\n", name);

    int client_index;

    for(int i = 0; i < current_no_clients; i++) 
    {
        if(strcmp(clients[i].name, name) == 0)
        {
            client_index = i;
            break;
        }
    }
    for(int i = client_index+1; i < current_no_clients; i++)
    {
        clients[i-1] = clients[i]; 
    }

    current_no_clients--;
}

// Thread task responsible for checking if presumably connected clients are still active
// (i.e. they respond to 'pings')
void* connection_check_task(void *args)
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching connection check thread");
        exit(EXIT_FAILURE);
    }

    message_header msg, rsp;
    msg.type = (uint8_t)PING;
    msg.length = (uint16_t)0;

    while(1) 
    {
        sleep(15);
        
        pthread_mutex_lock(&client_mutex);
    
        for(int i = 0; i < current_no_clients; i++)
        {
            clients[i].responded = 0;
            if (clients[i].type == UNIX) 
            {
                if(sendto(unix_socketfd, &msg, sizeof(msg), 0, (struct sockaddr *)&clients[i].unix_addr, sizeof(clients[i].unix_addr)) != sizeof(msg))
                {
                    fprintf(stderr, "Error while sending a ping to a client\n");
                }
            }
            else if(clients[i].type == INET)
            {
                if(sendto(inet_socketfd, &msg, sizeof(msg), 0, (struct sockaddr *)&clients[i].inet_addr, sizeof(clients[i].inet_addr)) != sizeof(msg))
                {
                    fprintf(stderr, "Error while sending a ping to a client\n");
                }
            }
        }

        sleep(1);

        struct sockaddr_in inet_client_addres;
        struct sockaddr_un unix_client_addres;
        socklen_t client_address_size;

        for(int i = 0; i < current_no_clients; i++)
        {
            if(clients[i].type == UNIX) 
            {
                client_address_size = sizeof(unix_client_addres);
                if(recvfrom(unix_socketfd, &rsp, sizeof(rsp), 0, (struct sockaddr *)&unix_client_addres, &client_address_size) == 3 && rsp.type == PING_RESPONSE)
                {
                    get_unix_client(&unix_client_addres)->responded = 1;
                }
            }
            else
            {
                client_address_size = sizeof(inet_client_addres);
                if(recvfrom(inet_socketfd, &rsp, sizeof(rsp), 0, (struct sockaddr *)&inet_client_addres, &client_address_size) == 3 && rsp.type == PING_RESPONSE)
                {
                    get_inet_client(&inet_client_addres)->responded = 1;
                }
            }
        }

        for(int i = 0; i < current_no_clients; i++)
        {
            if(clients[i].responded == 0)
            {
                forget_client(clients[i].name);
            }
        }

        pthread_mutex_unlock(&client_mutex);
    }

    return NULL;
}

// Parses a given mathematical expression
assignment* parse_assignment(const char *as_text)
{
    assignment *job;   
    int num1, num2;
    char op;

    job = (assignment *)malloc(sizeof(assignment));
    if(job == NULL)
    {
        fprintf(stderr, "Error while allocating memory\n");
        exit(EXIT_FAILURE);
    }

    if(sscanf(as_text, "%d%c%d", &num1, &op, &num2) != 3)
    {
        fprintf(stderr, "Could not parse given expression!\n");
        return NULL;
    }

    job->arg1 = num1;
    job->arg2 = num2;
    job->op = op;
    job->id = assignment_id++;
    job->type = (uint8_t)ASSIGNMENT;
    job->length = (uint16_t)(sizeof(float) * 2 + sizeof(char));

    printf("Expression %d %c %d set to ID %d.\n", num1, op, num2, job->id);

    return job;   
}

// Thread task responsible for reading input from the server user 
void* input_handle_task(void *args)
{   
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching input handle thread");
        exit(EXIT_FAILURE);
    }

    char user_input[128];
    assignment *job;

    while(1) 
    {
        fgets(user_input, 128, stdin);
        if(user_input[strlen(user_input) - 1] == '\n')
        {
            user_input[strlen(user_input) - 1] = '\0';
        }
    
        if((job=parse_assignment(user_input)) == NULL)
        {
            continue;
        }
    
        pthread_mutex_lock(&assignment_mutex);
        push_assignment_queue(&queue, job);
        pthread_mutex_unlock(&assignment_mutex);
    }
    
    return NULL;
}

// Checks if the client is already registered
int is_already_registered(const char *client_name)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        if(strcmp(clients[i].name, client_name) == 0)
        {
            return 1;
        }
    }
    return 0;
}

// Receives the name from the client, checks if its already in the register and responds
void check_name_availability(int msg_length, socket_type sock_type)
{
    message msg;
    message_header response;
    char client_name[MAX_CLIENT_NAME];
    struct sockaddr_un unix_client_addr;
    struct sockaddr_in inet_client_addr;
    socklen_t size;

    msg.value = malloc(msg_length);

    if(sock_type == INET) 
    {
        size = sizeof(inet_client_addr);
        if(recvfrom(inet_socketfd, msg.value, msg_length, MSG_WAITALL, (struct sockaddr *)&inet_client_addr, &size) == msg_length)
        {
            strcpy(client_name, msg.value);
            strcpy(clients[current_no_clients].name, msg.value);
            clients[current_no_clients].type = INET;
            free(msg.value);
            if(!is_already_registered(client_name))
            {   
                response.type = (uint8_t)OK;
                response.length = (uint16_t)0;
                printf("Registering client %s.\n", client_name);
                if(sendto(inet_socketfd, &response, sizeof(response), 0, (const struct sockaddr *)&inet_client_addr, sizeof(inet_client_addr)) == -1)
                {
                    fprintf(stderr, "Error while sending the OK response\n");
                }
                else
                {
                    current_no_clients++;
                }
            }
            else
            {
                response.type = (uint8_t)ERROR;
                response.length = (uint16_t)0;
                if(sendto(inet_socketfd, &response, sizeof(response), 0, (const struct sockaddr *)&inet_client_addr, sizeof(inet_client_addr)) == -1)
                {
                    fprintf(stderr, "Error while sending the ERROR message\n");
                }
            }
        }
    }
    else if(sock_type == UNIX)
    {
        if(recvfrom(unix_socketfd, msg.value, msg_length, MSG_WAITALL, (struct sockaddr *)&unix_client_addr, &size) == msg_length)
        {
            strcpy(client_name, msg.value);
            strcpy(clients[current_no_clients].name, msg.value);
            clients[current_no_clients].type = UNIX;
            free(msg.value);
            if(!is_already_registered(client_name))
            {   
                response.type = (uint8_t)OK;
                response.length = (uint16_t)0;
                printf("Registering client %s.\n", client_name);
                if(sendto(unix_socketfd, &response, sizeof(response), 0, (const struct sockaddr *)&unix_client_addr, sizeof(unix_client_addr)) == -1)
                {
                    fprintf(stderr, "Error while sending the OK response\n");
                }
                else
                {
                    current_no_clients++;
                }
            }
            else
            {
                response.type = (uint8_t)ERROR;
                response.length = (uint16_t)0;
                if(sendto(unix_socketfd, &response, sizeof(response), 0, (const struct sockaddr *)&unix_client_addr, sizeof(unix_client_addr)) == -1)
                {
                    fprintf(stderr, "Error while sending the ERROR message\n");
                }
            }
        }
    }
}

// SIGINT handler
void handler_sigint(int signo)
{
    const char *msg = "Server received a SIGINT interrupt. Aborting the server process...\n";
    
    if(signo == SIGINT)
    {
        write(1, msg, strlen(msg));
        exit(EXIT_SUCCESS);
    }
}

// If there are any jobs to be done, assign them to a random client
void calculation_assignment(void)
{
    pthread_mutex_lock(&client_mutex);

    if(current_no_clients < 1) 
    {
        pthread_mutex_unlock(&client_mutex);
        return;
    }
    else
    {
        pthread_mutex_lock(&assignment_mutex);
        assignment *job = pop_assignment_queue(&queue);
        if(job == NULL)
        {
            pthread_mutex_unlock(&assignment_mutex);
            pthread_mutex_unlock(&client_mutex);
            return;
        }
        else
        {
            int to_who_send = rand() % current_no_clients;
            message_header ass_header;
            assignment_partial ass_rest;
            ass_header.type = (uint8_t)ASSIGNMENT;
            ass_header.length = (uint16_t)sizeof(ass_rest);
            ass_rest.id = job->id;
            ass_rest.op = job->op;
            ass_rest.arg1 = job->arg1;
            ass_rest.arg2 = job->arg2;
            if(clients[to_who_send].type == UNIX) 
            {
                if(sendto(unix_socketfd, &ass_header, sizeof(ass_header), MSG_MORE, (const struct sockaddr *)&(clients[to_who_send].unix_addr), sizeof(clients[to_who_send].unix_addr)) != -1)
                {
                    if(sendto(unix_socketfd, &ass_rest, sizeof(ass_rest), 0, (const struct sockaddr *)&(clients[to_who_send].unix_addr), sizeof(clients[to_who_send].unix_addr)) != -1)
                    {
                        printf("Assigned client %s to the task %d.\n", clients[to_who_send].name, job->id);
                        free(job);
                    }
                    else
                    {
                        perror("Error while sending job to a client (rest)");
                    }
                }
                else
                {
                    perror("Error while sending job to a client (header)");
                }

            }
            else
            {
                if(sendto(inet_socketfd, &ass_header, sizeof(ass_header), 0, (const struct sockaddr *)&(clients[to_who_send].inet_addr), sizeof(clients[to_who_send].inet_addr)) != -1)
                {
                    if(sendto(inet_socketfd, &ass_rest, sizeof(ass_rest), 0, (const struct sockaddr *)&(clients[to_who_send].inet_addr), sizeof(clients[to_who_send].inet_addr)) != -1)
                    {
                        printf("Assigned client %s to the task %d.\n", clients[to_who_send].name, job->id);
                        free(job);
                    }
                    else
                    {
                        perror("Error while sending job to a client (rest)");
                    }
                }
                else
                {
                    perror("Error while sending job to a client (header)");
                }
            }
        }
    }

    pthread_mutex_unlock(&assignment_mutex);
    pthread_mutex_unlock(&client_mutex);
}

// Main listening function
void listen_for_clients(void)
{
    pthread_mutex_lock(&client_mutex);
    
    struct epoll_event events[10];
    int epoll_res;
    struct sockaddr_un unix_client_addr;
    struct sockaddr_in inet_client_addr;
    message msg;
    socklen_t size;

    epoll_res = epoll_wait(epoll_fd, events, 10, 0);
    if(epoll_res == -1)
    {
        perror("Error while waiting for epoll");
        exit(EXIT_FAILURE);
    }
    for(int i = 0; i < epoll_res; i++)
    {
        if(events[i].data.fd == inet_socketfd)
        {
            size = sizeof(inet_client_addr);
            if(recvfrom(inet_socketfd, &msg, 3, MSG_WAITALL, (struct sockaddr *)&inet_client_addr, &size) == -1)
            {
                if (errno != EAGAIN && errno != EWOULDBLOCK) 
                {
                    perror("Error while accepting connection from a local client");
                    exit(EXIT_FAILURE);
                }
            }
            else
            {
                if(msg.type == INTRO) 
                {
                    copy_inet_addr(&inet_client_addr, &clients[current_no_clients].inet_addr);
    
                    printf("Accepting a connection from a new host.\n");
    
                    check_name_availability(msg.length, INET);
                }
                else if(msg.type == RESULT)
                {
                    size = sizeof(inet_client_addr);
                    msg.value = malloc(msg.length);
                    if(msg.value == NULL)
                    {
                        fprintf(stderr, "Error while allocating memory\n");
                        exit(EXIT_FAILURE);
                    }
                    if(recvfrom(inet_socketfd, msg.value, msg.length, MSG_WAITALL, (struct sockaddr *)&inet_client_addr, &size) == msg.length)
                    {
                        printf("Received the result to assignment %s from %s.\n", (char*)msg.value, get_inet_client(&inet_client_addr)->name);
                    }
                    else
                    {
                        printf("Packets were lost - the client promised to send more data.\n");
                    }
                    free(msg.value);
                }
                else if(msg.type == UNREGISTER)
                {
                    forget_client(get_inet_client(&inet_client_addr)->name);
                }
            }
        }
        else if(events[i].data.fd == unix_socketfd)
        {
            size = sizeof(unix_client_addr);
            if(recvfrom(unix_socketfd, &msg, 3, MSG_WAITALL, (struct sockaddr *)&unix_client_addr, &size) == -1)
            {
                if (errno != EAGAIN && errno != EWOULDBLOCK) 
                {
                    perror("Error while accepting connection from a local client");
                    exit(EXIT_FAILURE);
                }
            }
            else
            {
                if(msg.type == INTRO) 
                {
                    copy_unix_addr(&unix_client_addr, &clients[current_no_clients].unix_addr);
    
                    printf("Accepting a connection from a new host.\n");
    
                    check_name_availability(msg.length, UNIX);
                }
                else if(msg.type == RESULT)
                {
                    size = sizeof(unix_client_addr);
                    msg.value = malloc(msg.length);
                    if(msg.value == NULL)
                    {
                        fprintf(stderr, "Error while allocating memory\n");
                        exit(EXIT_FAILURE);
                    }
                    if(recvfrom(unix_socketfd, msg.value, msg.length, MSG_WAITALL, (struct sockaddr *)&unix_client_addr, &size) == msg.length)
                    {
                        printf("Received the result to assignment %s from %s.\n", (char*)msg.value, get_unix_client(&unix_client_addr)->name);
                    }
                    free(msg.value);
                }
                else if(msg.type == UNREGISTER)
                {
                    forget_client(get_unix_client(&unix_client_addr)->name);
                }
            }
        }
    }

    pthread_mutex_unlock(&client_mutex);
    return;
}

// Main listening loop of the server
void main_loop(void)
{
    while(1) 
    {
        listen_for_clients();
        calculation_assignment();
    }
}

// MAIN Function
int main(int argc, char** argv)
{
    int UDP_port_id;
    struct sockaddr_un unix_addr;
    struct sockaddr_in inet_addr;
    union epoll_data unix_epoll_data, inet_epoll_data;
    struct epoll_event unix_epoll_event, inet_epoll_event;
    
    if(argc < 3) sig_arg_err();

    unix_socket_path = argv[2];
    UDP_port_id = (int)strtol(argv[1], NULL, 10);

    if(UDP_port_id < 1024 || UDP_port_id > 65536)
    {
        fprintf(stderr, "Wrong port format!\n");
        exit(EXIT_FAILURE);
    }
    if(strlen(unix_socket_path) > UNIX_PATH_MAX)
    {
        fprintf(stderr, "Wrong UNIX socket path!\n");
        exit(EXIT_FAILURE);
    }

     // Register a cleanup function
    atexit(perform_cleanup);

    // Register the Ctrl+C interrupt signal
    if(signal(SIGINT, handler_sigint) == SIG_ERR)
    {
        perror("Error while setting the SIGINT handler");
        exit(EXIT_FAILURE);
    }

    // Init assignment queue;
    init_assignment_queue(&queue);

    srand(time(NULL));

    // Prepare the address structs
    unix_addr.sun_family = AF_UNIX;
    strcpy(unix_addr.sun_path, unix_socket_path);
    inet_addr.sin_family = AF_INET;
    inet_addr.sin_port = htons(UDP_port_id);
    inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // Create UNIX socket
    if((unix_socketfd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK, 0)) == -1)
    {
        perror("Error while creating UNIX socket");
        exit(EXIT_FAILURE);
    }

    // Bind UNIX socket to the given address
    if(bind(unix_socketfd, (const struct sockaddr *)&unix_addr, sizeof(unix_addr)) == -1)
    {
        perror("Error while binding UNIX socket to the provided address");
        exit(EXIT_FAILURE);
    }

    // Create network socket
    if((inet_socketfd=socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, IPPROTO_UDP)) == -1)
    {
        perror("Error while creating network socket");
        exit(EXIT_FAILURE);
    }

    // Bind the network socket to the given port and any address
    if(bind(inet_socketfd, (const struct sockaddr *)&inet_addr, sizeof(inet_addr)) == -1)
    {
        perror("Error while binding network socket to the port");
        exit(EXIT_FAILURE);
    }

    printf("Sockets created and bound successfully. Now waiting for input and clients...\n");

    // Create epoll instance
    if((epoll_fd=epoll_create(1)) == -1)
    {
        perror("Error while creating epoll instance");
        exit(EXIT_FAILURE);
    }

    // Init of epoll controls structures
    unix_epoll_data.fd = unix_socketfd;
    inet_epoll_data.fd = inet_socketfd;
    unix_epoll_event.events = EPOLLIN;
    inet_epoll_event.events = EPOLLIN;
    unix_epoll_event.data = unix_epoll_data;
    inet_epoll_event.data = inet_epoll_data;

    // Adding sockets to epolls watch list
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, unix_socketfd, &unix_epoll_event) == -1)
    {
        perror("Error while performing control operations for unix socket on epoll");
        exit(EXIT_FAILURE);
    }
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, inet_socketfd, &inet_epoll_event) == -1)
    {
        perror("Error while performing control operations for inet socket on epoll");
        exit(EXIT_FAILURE);
    }

    // Create the connection-check and input-handle threads
    pthread_create(&connection_check_thread, NULL, connection_check_task, NULL);
    pthread_create(&input_handle_thread, NULL, input_handle_task, NULL);

    main_loop();

    exit(EXIT_SUCCESS);
}