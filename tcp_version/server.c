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

// Server assignment sent to clients
typedef struct __attribute__((__packed__)) assignment_tag {
    uint8_t type;
    uint16_t length;
    char op;
    int arg1;
    int arg2;
    int id;
} assignment;

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
    int socketfd;
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
           "Usage: server <TCP port number> <UNIX socket path>\n");
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

// Get client name by socket_fd
char* get_client_name(int socket)
{
    char *res;
    for(int i = 0; i < current_no_clients; i++)
    {
        if(clients[i].socketfd == socket)
        {
            res = clients[i].name;
        }
    }
    return res;
}

// Peforms clean up of unused resources at the exit of the server process
void perform_cleanup(void)
{
    for(int i = 0; i < current_no_clients; i++)
    {
        close(clients[i].socketfd);
    }
    pthread_mutex_destroy(&client_mutex);
    pthread_mutex_destroy(&assignment_mutex);
    close(epoll_fd);
    shutdown(unix_socketfd, SHUT_RDWR);
    shutdown(inet_socketfd, SHUT_RDWR);
    close(inet_socketfd);
    close(unix_socketfd);
    unlink(unix_socket_path);
}

// Get client socket FD by his name
int get_client_socket(const char *name)
{
    int res = 1;
    for(int i = 0; i < current_no_clients; i++)
    {
        if(strcmp(clients[i].name, name) == 0)
        {
            res = clients[i].socketfd;
        }
    }
    return res;
}

// Forget a client
void forget_client(const char *name, int socket)
{
    int client_socketfd;
    if(socket == -1 && name != NULL) 
    {
        client_socketfd = get_client_socket(name);
        if(client_socketfd == -1) return;
    }
    else
    {
        client_socketfd = socket;
    }
    
    if(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_socketfd, NULL) == -1)
    {
        perror("Error while removing client from epoll watchlist");
        exit(EXIT_FAILURE);
    }

    printf("Unregistering client %s.\n", get_client_name(client_socketfd));

    int client_index;

    for(int i = 0; i < current_no_clients; i++) 
    {
        if(clients[i].socketfd == client_socketfd)
        {
            client_index = i;
            break;
        }
    }
    for(int i = client_index+1; i < current_no_clients; i++)
    {
        clients[i-1] = clients[i]; 
    }

    close(client_socketfd);
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
            if(send(clients[i].socketfd, &msg, sizeof(msg), 0) != sizeof(msg))
            {
                fprintf(stderr, "Error while sending a ping to a client\n");
            }
        }

        sleep(1);

        for(int i = 0; i < current_no_clients; i++)
        {
            if(recv(clients[i].socketfd, &rsp, 3, MSG_WAITALL) == 3 && rsp.type == PING_RESPONSE)
            {
                continue;
            }
            else
            {
                forget_client(NULL, clients[i].socketfd);
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
void check_name_availability(void)
{
    int return_code;
    message msg;
    message_header response;
    union epoll_data epoll_tmp_data;
    struct epoll_event epoll_tmp_event;
    char client_name[MAX_CLIENT_NAME];

    if((return_code=recv(clients[current_no_clients].socketfd, &msg, 3, MSG_WAITALL)) == 3)
    {
        if(msg.type == INTRO) 
        {
            msg.value = malloc(msg.length);
            if(recv(clients[current_no_clients].socketfd, msg.value, msg.length, MSG_WAITALL) == msg.length)
            {
                strcpy(client_name, msg.value);
                strcpy(clients[current_no_clients].name, msg.value);
                free(msg.value);
    
                if(!is_already_registered(client_name))
                {
                    epoll_tmp_data.fd = clients[current_no_clients].socketfd;
                    epoll_tmp_event.events = EPOLLIN;
                    epoll_tmp_event.data = epoll_tmp_data;
                    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, clients[current_no_clients].socketfd, &epoll_tmp_event) == -1)
                    {
                        perror("Error while adding client to the epoll watchlist");
                        exit(EXIT_FAILURE);
                    }
                    else
                    {
                        response.type = (uint8_t)OK;
                        response.length = (uint16_t)0;

                        printf("Registering client %s.\n", client_name);

                        if(send(clients[current_no_clients].socketfd, &response, sizeof(response), 0) == -1)
                        {
                            fprintf(stderr, "Error while sending the OK response");
                        }
                        current_no_clients++;
                    }
                }
                else
                {
                    response.type = (uint8_t)ERROR;
                    response.length = (uint16_t)0;
    
                    if(send(clients[current_no_clients].socketfd, &response, sizeof(response), MSG_MORE) == -1)
                    {
                        fprintf(stderr, "Error while sending the beginning of the message");
                    }
    
                    close(clients[current_no_clients].socketfd);
                }
            }
        }
    }
    else if(return_code == 0)
    {
        fprintf(stderr, "The client has closed the connection");
        close(clients[current_no_clients].socketfd);
    }
    else
    {
        perror("Error while receiving client name");
        close(clients[current_no_clients].socketfd);
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
            if(send(clients[to_who_send].socketfd, job, sizeof(*job), 0) == sizeof(*job))
            {
                printf("Assigned client %s to the task %d.\n", clients[to_who_send].name, job->id);
                free(job);
            }
            else
            {
                perror("Error while sending job to a client");
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
            if((clients[current_no_clients].socketfd=accept(inet_socketfd, (struct sockaddr *)&inet_client_addr, &size)) == -1)
            {
                if (errno != EAGAIN && errno != EWOULDBLOCK) 
                {
                    perror("Error while accepting connection from a local client");
                    exit(EXIT_FAILURE);
                }
            }
            else
            {
                clients[current_no_clients].inet_addr = inet_client_addr;

                printf("Accepting a connection from a new host.\n");

                check_name_availability();
            }
        }
        else if(events[i].data.fd == unix_socketfd)
        {
            size = sizeof(unix_client_addr);
            if((clients[current_no_clients].socketfd=accept(unix_socketfd, (struct sockaddr *)&unix_client_addr, &size)) == -1)
            {
                if (errno != EAGAIN && errno != EWOULDBLOCK) 
                {
                    perror("Error while accepting connection from a local client");
                    exit(EXIT_FAILURE);
                }
            }
            else
            {
                clients[current_no_clients].unix_addr = unix_client_addr;

                printf("Accepting a connection from a new host.\n");

                check_name_availability();
            }
        }
        else
        {
            int recv_res;
            message msg;
            if ((recv_res=recv(events[i].data.fd, &msg, 3, MSG_WAITALL)) == 3 && msg.type == RESULT) 
            {
                msg.value = malloc(msg.length);
                if(msg.value == NULL)
                {
                    fprintf(stderr, "Error while allocating memory\n");
                    exit(EXIT_FAILURE);
                }
                if (recv(events[i].data.fd, msg.value, msg.length, MSG_WAITALL) == msg.length)
                {
                    printf("Received the result to assignment %s from %s.\n", (char*)msg.value, get_client_name(events[i].data.fd));
                }
                free(msg.value);
                pthread_mutex_unlock(&client_mutex);
                return;
            }
            else if(recv_res < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
            {
                fprintf(stderr, "Error while receving result from the client\n");
                forget_client(NULL, events[i].data.fd);
            }
            else if(recv_res == 3 && msg.type == UNREGISTER)
            {
                forget_client(NULL, events[i].data.fd);
            }
        }
    }

    pthread_mutex_unlock(&client_mutex);
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
    in_port_t TCP_port_id;
    struct sockaddr_un unix_addr;
    struct sockaddr_in inet_addr;
    union epoll_data unix_epoll_data, inet_epoll_data;
    struct epoll_event unix_epoll_event, inet_epoll_event;
    
    if(argc < 3) sig_arg_err();

    unix_socket_path = argv[2];
    TCP_port_id = (in_port_t)strtol(argv[1], NULL, 10);

    if(TCP_port_id < 1024 || TCP_port_id > 65536)
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
    inet_addr.sin_port = TCP_port_id;
    inet_addr.sin_addr.s_addr = INADDR_ANY;

    // Create UNIX socket
    if((unix_socketfd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1)
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

    // Listen for incoming transmissions
    if(listen(unix_socketfd, MAX_NO_LOCAL_CLIENTS) == -1)
    {
        perror("Error while trying to listen for incoming local clients");
        exit(EXIT_FAILURE);
    }

    // Create network socket
    if((inet_socketfd=socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1)
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

    // Listen for incoming client netowrk requests
    if(listen(inet_socketfd , MAX_NO_NETWORK_CLIENTS) == -1)
    {
        perror("Error while trying to listen for incoming netowrk clients");
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