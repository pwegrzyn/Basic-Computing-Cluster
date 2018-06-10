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

// Server assignment sent to clients
typedef struct __attribute__((__packed__)) assignment_partial_tag {
    char op;
    int arg1;
    int arg2;
    int id;
} assignment_partial;

// Type of the connection to the server
typedef enum connection_t_tag {
    LOCAL,
    NETWORK
} connection_t;

// Globals
int unix_socketfd, inet_socketfd;
connection_t connection;

// Helper function used to signalize argument errors
void sig_arg_err()
{
    printf("Wrong argument format.\n"
           "Usage: client <name> <connection type> <server address>\n");
    exit(EXIT_FAILURE);
}

// SIGINT handler
void handler_sigint(int signo)
{
    const char *info = "Client received a SIGINT interrupt. Unregistering from the server and aborting the client process...\n";
    
    if(signo == SIGINT)
    {
        write(1, info, strlen(info));

        message_header msg;
        msg.type = (uint8_t)UNREGISTER;
        msg.length = (uint16_t)0;

        if(connection == LOCAL) 
        {
            send(unix_socketfd, &msg, sizeof(msg), MSG_DONTWAIT);
        }
        else if(connection == NETWORK)
        {
            send(inet_socketfd, &msg, sizeof(msg), MSG_DONTWAIT);
        }
        exit(EXIT_SUCCESS);
    }
}

// Helper function used for clean-up
void perform_cleanup(void)
{
    shutdown(unix_socketfd, SHUT_RDWR);
    shutdown(inet_socketfd, SHUT_RDWR);
    close(inet_socketfd);
    close(unix_socketfd);
}

// Peform the actual calculation
int calculate_assignemnt(assignment_partial *job)
{
    switch(job->op)
    {
        case '+':
            return job->arg1 + job->arg2;
        case '-':
           return job->arg1 - job->arg2;
        case '*':
            return job->arg1 * job->arg2;
        case '/':
            if(job->arg2 == 0)
            {
                return 0;
            }
            else
            {
                return job->arg1 / job->arg2;
            }
        default:
            return 0;
    }
}

// MAIN Function
int main(int argc, char** argv)
{
    char *client_name, *connection_type, *ip_addr_human, *unix_socket_path;
    char ip_addr_port[32];
    in_port_t server_port;
    struct in_addr server_ip_addr;

    if (argc < 4) sig_arg_err();

    client_name = argv[1];
    if(strlen(client_name) > MAX_CLIENT_NAME)
    {
        fprintf(stderr, "The provided client name is too long!\n");
        exit(EXIT_FAILURE);
    }

    connection_type = argv[2];
    if(strcmp(connection_type, "local") == 0)
        connection = LOCAL;
    else if(strcmp(connection_type, "network") == 0)
        connection = NETWORK;
    else
    {
        fprintf(stderr, "Wrong connection type argument format!\n");
        exit(EXIT_FAILURE);
    }

    if(connection == LOCAL)
    {
        unix_socket_path = argv[3];
        if(strlen(unix_socket_path) > UNIX_PATH_MAX)
        {
            fprintf(stderr, "Wrong UNIX socket path!\n");
            exit(EXIT_FAILURE);
        }
    }
    else if(connection == NETWORK)
    {
        strcpy(ip_addr_port, argv[3]);
        char *token;
        token = strtok(ip_addr_port, ":");
        ip_addr_human = token;
        token = strtok(NULL, ":");
        server_port = (in_port_t)strtol(token, NULL, 10);
        if(server_port < 1024 || server_port > 65536)
        {
            fprintf(stderr, "Wrong server port format!\n");
            exit(EXIT_FAILURE);
        }
        if(inet_aton(ip_addr_human, &server_ip_addr) == 0)
        {
            fprintf(stderr, "Wrong server ip address format!\n");
            exit(EXIT_FAILURE);
        }
    }

    // Register a clean-up function
    atexit(perform_cleanup);

    // Register the Ctrl+C interrupt signal
    if(signal(SIGINT, handler_sigint) == SIG_ERR)
    {
        perror("Error while setting the SIGINT handler");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serv_inet_addr;
    struct sockaddr_un serv_unix_addr;
    
    if(connection == LOCAL)
    {
        bzero((char *)&serv_unix_addr, sizeof(serv_unix_addr));
        serv_unix_addr.sun_family = AF_UNIX;
        strcpy(serv_unix_addr.sun_path, unix_socket_path);

        // Create the socket
        if((unix_socketfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
        {
            perror("Error while creating UNIX socket");
            exit(EXIT_FAILURE);
        }

        printf("Socket successfully created.\n");

        // Try establishing a connection to the server
        if(connect(unix_socketfd, (struct sockaddr *)&serv_unix_addr, sizeof(serv_unix_addr)) < 0) 
        {
            perror("Error while connecting to the server");
            exit(EXIT_FAILURE);
        }

        printf("Connection with the server successfully established.\n");

        message_header msg;
        msg.type = (uint8_t)INTRO;
        msg.length = (uint16_t)strlen(client_name)+1;
        char *value = (char*)malloc(msg.length);
        strcpy(value, client_name);
        if(value == NULL)
        {
            fprintf(stderr, "Error while allocating memory\n");
            exit(EXIT_FAILURE);
        }
        
        // Send the intro message
        if(send(unix_socketfd, &msg, sizeof(msg), MSG_MORE) == -1)
        {
            perror("Error while sending intro message to the server");
            exit(EXIT_FAILURE);
        }
        if(send(unix_socketfd, value, msg.length, MSG_MORE) == -1)
        {
            perror("Error while sending intro message (name) to the server");
            exit(EXIT_FAILURE);
        }

        printf("Intro message successfully sent to the server.\n");

        message_header response;
        int return_code;

        // Receive the response to the intro message
        if((return_code=recv(unix_socketfd, &response, 3, MSG_WAITALL)) == 3 && response.type == OK)
        {
            printf("The server has accepted this client - now waiting for assignemnts...\n");
        }
        else if(return_code <= 0)
        {
            perror("Error while receiving the response from the server\n");
            exit(EXIT_FAILURE);
        }
        else if(response.type == ERROR)
        {
            fprintf(stderr, "The server has rejected this client. Reason: name already taken.\n");
            exit(EXIT_FAILURE);
        }

        while(1)
        {
            message_header req;
            if((return_code=recv(unix_socketfd, &req, 3, MSG_WAITALL)) == 3)
            {
                if(req.type == PING)
                {
                    message_header pong;
                    pong.type = (uint8_t)PING_RESPONSE;
                    pong.length = (uint16_t)0;
                    if(send(unix_socketfd, &pong, sizeof(pong), 0) != sizeof(pong))
                    {
                        fprintf(stderr, "Error while sending the ping response\n");
                        exit(EXIT_FAILURE);
                    }
                }
                else if(req.type == ASSIGNMENT)
                {
                    printf("Received a new assignment. Performing calculations and sending back the result...\n");
                    assignment_partial job;
                    if(recv(unix_socketfd, &job, sizeof(job), MSG_WAITALL) == sizeof(job))
                    {
                        // Do the calculation
                        int res = calculate_assignemnt(&job);
                        char buffer[64];
                        sprintf(buffer, "%d - %d", job.id, res);
                        message_header calc_resp;
                        calc_resp.type = (uint8_t)RESULT;
                        calc_resp.length = (uint16_t)(strlen(buffer)+1);
                        if(send(unix_socketfd, &calc_resp, sizeof(calc_resp), 0) != sizeof(calc_resp))
                        {
                            fprintf(stderr, "Error while sending the calculation response header\n");
                            exit(EXIT_FAILURE);
                        }
                        else
                        {
                            if(send(unix_socketfd, buffer, calc_resp.length, 0) != calc_resp.length)
                            {
                                fprintf(stderr, "Error while sending the calculation response value\n");
                                exit(EXIT_FAILURE);
                            }
                        }

                    }
                    else
                    {
                        fprintf(stderr, "Error while receiving the assignment value\n");
                        exit(EXIT_FAILURE);
                    }
                }
            }
            else
            {
                fprintf(stderr, "Error while receiving a request from the server\n");
                exit(EXIT_FAILURE);
            }
        }
    }

    else if(connection == NETWORK)
    {
        bzero((char *)&serv_inet_addr, sizeof(serv_inet_addr));
        serv_inet_addr.sin_family = AF_INET;
        serv_inet_addr.sin_port = server_port;
        serv_inet_addr.sin_addr = server_ip_addr;

        if((inet_socketfd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        {
            perror("Error while creating network socket");
            exit(EXIT_FAILURE);
        }

        printf("Socket successfully created.\n");

        if(connect(inet_socketfd, (struct sockaddr *)&serv_inet_addr, sizeof(serv_inet_addr)) < 0) 
        {
            perror("Error while connecting to the server");
            exit(EXIT_FAILURE);
        }

        printf("Connection with the server successfully established.\n");

        message_header msg;
        msg.type = (uint8_t)INTRO;
        msg.length = (uint16_t)strlen(client_name)+1;
        char *value = (char*)malloc(msg.length);
        strcpy(value, client_name);
        if(value == NULL)
        {
            fprintf(stderr, "Error while allocating memory\n");
            exit(EXIT_FAILURE);
        }
        
        // Send the intro message
        if(send(inet_socketfd, &msg, sizeof(msg), MSG_MORE) == -1)
        {
            perror("Error while sending intro message to the server");
            exit(EXIT_FAILURE);
        }
        if(send(inet_socketfd, value, msg.length, MSG_MORE) == -1)
        {
            perror("Error while sending intro message (name) to the server");
            exit(EXIT_FAILURE);
        }

        printf("Intro message successfully sent to the server.\n");

        message_header response;
        int return_code;

        // Receive the response to the intro message
        if((return_code=recv(inet_socketfd, &response, 3, MSG_WAITALL)) == 3 && response.type == OK)
        {
            printf("The server has accepted this client - now waiting for assignemnts...\n");
        }
        else if(return_code <= 0)
        {
            perror("Error while receiving the response from the server\n");
            exit(EXIT_FAILURE);
        }
        else if(response.type == ERROR)
        {
            fprintf(stderr, "The server has rejected this client. Reason: name already taken\n");
            exit(EXIT_FAILURE);
        }

        while(1)
        {
            message_header req;
            if((return_code=recv(inet_socketfd, &req, 3, MSG_WAITALL)) == 3)
            {
                if(req.type == PING)
                {
                    message_header pong;
                    pong.type = (uint8_t)PING_RESPONSE;
                    pong.length = (uint16_t)0;
                    if(send(inet_socketfd, &pong, sizeof(pong), 0) != sizeof(pong))
                    {
                        fprintf(stderr, "Error while sending the ping response\n");
                        exit(EXIT_FAILURE);
                    }
                }
                else if(req.type == ASSIGNMENT)
                {
                    printf("Received a new assignment. Performing calculations and sending back the result...\n");
                    assignment_partial job;
                    if(recv(inet_socketfd, &job, sizeof(job), MSG_WAITALL) == sizeof(job))
                    {
                        // Do the calculation
                        int res = calculate_assignemnt(&job);
                        char buffer[64];
                        sprintf(buffer, "%d - %d", job.id, res);
                        message_header calc_resp;
                        calc_resp.type = (uint8_t)RESULT;
                        calc_resp.length = (uint16_t)(strlen(buffer)+1);
                        if(send(inet_socketfd, &calc_resp, sizeof(calc_resp), 0) != sizeof(calc_resp))
                        {
                            fprintf(stderr, "Error while sending the calculation response header\n");
                            exit(EXIT_FAILURE);
                        }
                        else
                        {
                            if(send(inet_socketfd, buffer, calc_resp.length, 0) != calc_resp.length)
                            {
                                fprintf(stderr, "Error while sending the calculation response value\n");
                                exit(EXIT_FAILURE);
                            }
                        }

                    }
                    else
                    {
                        fprintf(stderr, "Error while receiving the assignment value\n");
                        exit(EXIT_FAILURE);
                    }
                }
            }
            else
            {
                fprintf(stderr, "Error while receiving a request from the server\n");
                exit(EXIT_FAILURE);
            }
        }
    }

    exit(EXIT_SUCCESS);
}