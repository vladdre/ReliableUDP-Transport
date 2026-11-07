#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "./include/lib.h"
#include "./include/utils.h"
#include "./include/protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <cstring>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// Declaram un vector care tine legatura intre indexul din poll_fds si id ul conexiunii reale
int poll_conn_map[MAX_CONNECTIONS];

// Fd ul socketului pe care se asculta conexiuni noi
static int listen_sockfd = -1;

// Capacit max a bufferului de receive
static int recv_buffer_bytes = 0;

// Luam un contor pt ID ul clientilor
static int next_conn_id = 0;

// Facem o functie care seteaza un timer pt un fd dat
static void set_timer_interval (int fd) {
    int timer_ms = TIMEOUT_SEND(2) * 2;

    struct itimerspec specifications;
    // Impartim timer ul la 1000 pt a avea secundele din ms
    specifications.it_value.tv_sec = timer_ms / 1000;
    // Luam restul impartirii la 1000 pt a avea milisecundele ramase si le transformam in nanosecunde
    specifications.it_value.tv_nsec = (timer_ms % 1000) * 1000000L;

    // Daca timer ul e mai mic de 1000 ms, atunci nu punem secunde, ci doar milisecunde
    if (timer_ms < 1000) {
        specifications.it_value.tv_sec = 0;
        specifications.it_value.tv_nsec = (long)timer_ms * 1000000L;
    }

    // Setam timer ul sa fie periodic, cu aceeasi valoare ca cea a timeout ului
    specifications.it_interval = specifications.it_value;
    // Setam timer ul pentru fd ul dat
    timerfd_settime(fd, 0, &specifications, NULL);
}

// Facem o functie care returneaza cat spatiu liber mai are receiverul in fereastra de receive
static uint16_t recv_window_available_size (struct connection *c) {
    // Calculam cat spatiu liber mai are receiverul in fereastra de receive
    int size_available = c->recv_buf_cap - c->recv_buf_used;

    // Daca size_available e negativ,
    // atunci nu mai avem spatiu in fereastra de receive
    if (size_available < 0) {
        size_available = 0;
    }

    // Daca size_available e mai mare de 65535, atunci il limitam la 65535,
    // deoarece campul len din header e de 16 biti
    if (size_available > 65535) {
        size_available = 65535;
    }

    return (uint16_t)size_available;
}

// Facem o functie care trimite un ACK inapoi la sender, cu numarul de ACK egal cu urmatorul numar de seq asteptat
static void send_ack (struct connection *c) {
    poli_tcp_ctrl_hdr ack;
    char packet[sizeof(ack)];

    // Construim un pachet de tip ACK, cu numarul de ACK egal cu urmatorul numar de seq asteptat
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = c->conn_id;
    ack.type = POLI_ACK;
    ack.ack_num = htons(c->next_recv_seq_num);
    ack.recv_window = htons(recv_window_available_size(c));

    // Copiem header ul de control in packet
    memcpy(packet, &ack, sizeof(ack));

    // Trimitem pachetul de ACK inapoi la sender
    sendto(c->sockfd, packet, sizeof(packet), 0, (struct sockaddr *)&c->servaddr, sizeof(c->servaddr));
}

// Facem o functie care compara doua numere de seq (returneaza 1 daca a e mai mare decat b, si daca nu 0)
static int seq_greater (uint16_t a, uint16_t b) {
    // Verificam daca a e mai mare decat b, tinand cont ca numerele de seq sunt modulo 65536
    if ((uint16_t) (a - b) < 32768) {
        // Daca a e si diefrit de b returnam 1
        if (a != b) {
            return 1;
        }
    }

    // Altfel, a nu e mai mare decat b si returnam 0
    return 0;
}

// Facem o functie care adauga pachetele din out_of_order care pot fi livrate acum la buffer ul de receive in ordine
static void deliver_in_order_packets (struct connection *c) {
    // Verificam daca avem pachete in out_of_order care pot fi livrate acum
    while (c->out_of_order.count(c->next_recv_seq_num)) {
        // Daca avem un astfel de pachet, il adaugam la buffer ul de receive in ordine
        // si il scoatem din out_of_order
        auto &packet = c->out_of_order[c->next_recv_seq_num];
        c->app_recv_buf.insert(c->app_recv_buf.end(), packet.begin(), packet.end());
        c->out_of_order.erase(c->next_recv_seq_num);
        c->next_recv_seq_num++;
    }
}

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    // Daca buffer ul de receive e gol, atunci asteptam sa primim date noi
    while (cons[conn_id]->app_recv_buf.empty()) {
        pthread_cond_wait(&cons[conn_id]->con_cond, &cons[conn_id]->con_lock);
    }

    // Daca len e mai mic decat dimensiunea buffer ului de receive, atunci copiem doar len bytes in buffer ul dat ca parametru
    if (len < (int)cons[conn_id]->app_recv_buf.size()) {
        size = len;
    } else {
        // Altfel, copiem tot buffer ul de receive in buffer ul dat ca parametru
        size = (int)cons[conn_id]->app_recv_buf.size();
    }

    // Facem copierea propriu zisa a datelor din buffer ul de receive in buffer ul dat ca parametru
    // si apoi stergem datele copiate din buffer ul de receive
    memcpy(buffer, cons[conn_id]->app_recv_buf.data(), size);
    cons[conn_id]->app_recv_buf.erase(cons[conn_id]->app_recv_buf.begin(), cons[conn_id]->app_recv_buf.begin() + size);
    cons[conn_id]->recv_buf_used -= size;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    // DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        // Daca res e -1 inseamn ca e timeout, deci daca conn_id e valid si exista conex in cons, dam continue
        if (res == -1) {
            if (conn_id == -1 || cons.find(conn_id) == cons.end()) {
                continue;
            }

            // Daca nu respecta cond din ultimul if, trimitem un ack inapoi
            pthread_mutex_lock(&cons[conn_id]->con_lock);
            send_ack(cons[conn_id]);
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        // Daca res nu are macar dimensiunea unui header de control, sau daca conn_id nu e valid sau daca conexiunea cu conn_id nu exista in cons,
        // atunci continuam
        if (res < (int)sizeof(poli_tcp_ctrl_hdr) || conn_id == -1 || cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        // Verificam daca segmentul primit e de tip data, daca are protocol_id corect si daca are o dimensiune valida
        poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;
        if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == POLI_DATA) {
            uint16_t seq = ntohs(hdr->seq_num);
            uint16_t data_len = ntohs(hdr->len);

            if (data_len <= MAX_DATA_SIZE && res >= (int)(sizeof(poli_tcp_data_hdr) + data_len)) {
                char *payload = segment + sizeof(poli_tcp_data_hdr);

                // Daca pachetul primit are numarul de seq egal cu urmatorul numar de seq asteptat, atunci il adaugam la buffer ul de receive in ordine
                if (seq == cons[conn_id]->next_recv_seq_num) {
                    // Daca avem spatiu in buffer ul de receive, adaugam datele primite la buffer ul de receive,
                    // incrementam next_recv_seq_num si incercam sa livram si pachete din out_of_order care pot fi livrate acum
                    if (cons[conn_id]->recv_buf_used + data_len <= cons[conn_id]->recv_buf_cap) {
                        cons[conn_id]->app_recv_buf.insert(cons[conn_id]->app_recv_buf.end(), payload, payload + data_len);
                        cons[conn_id]->recv_buf_used += data_len;
                        cons[conn_id]->next_recv_seq_num++;

                        deliver_in_order_packets(cons[conn_id]);

                        // Daca am adaugat date noi in buffer ul de receive, atunci trezim recv_data daca asteapta
                        pthread_cond_broadcast(&cons[conn_id]->con_cond);
                    }
                } else if (seq_greater(seq, cons[conn_id]->next_recv_seq_num)) {
                    // Daca pachetul primit are numarul de seq mai mare decat urmatorul numar de seq asteptat, atunci il adaugam in out_of_order daca nu e deja acolo
                    // si daca avem spatiu in buffer ul de receive
                    if (cons[conn_id]->out_of_order.find(seq) == cons[conn_id]->out_of_order.end() && cons[conn_id]->recv_buf_used + data_len <= cons[conn_id]->recv_buf_cap) {
                        cons[conn_id]->out_of_order[seq] = vector<char>(payload, payload + data_len);
                        cons[conn_id]->recv_buf_used += data_len;
                    }
                }

                // Daca am primit un pachet de date, atunci trimitem un ACK inapoi la sender,
                // cu numarul de ACK egal cu urmatorul numar de seq asteptat
                send_ack(cons[conn_id]);
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    // 
    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    while (1) {
        // Asteptam sa primim un segment de tip SYN pe socket ul de listen.
        int n = recvfrom(listen_sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&client_addr, &client_addr_len);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr)) {
            continue;
        }

        // Verificam daca segmentul primit e de tip SYN si daca are protocol_id corect
        poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buffer;
        if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != POLI_SYN) {
            continue;
        }

        // Daca am primit un segment de tip SYN, atunci cream o noua conexiune pentru clientul care a trimis segmentul de tip SYN
        int conn_id = next_conn_id++;
        struct connection *con = new struct connection();
        con->conn_id = conn_id;
        con->next_recv_seq_num = 0;
        con->recv_buf_cap = recv_buffer_bytes;
        con->recv_buf_used = 0;
        con->servaddr = client_addr;
        pthread_mutex_init(&con->con_lock, NULL);
        pthread_cond_init(&con->con_cond, NULL);

        /* Receive SYN on the connection socket. Create a new socket and bind it to
         * the chosen port. Send the data port number via SYN-ACK to the client */

        // Cream un socket nou pentru conexiunea cu clientul care a trimis segmentul de tip SYN
        con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

        // Legam socket ul nou creat la un port ales de sistem,
        // pe care il vom folosi pentru a comunica cu clientul care a trimis segmentul de tip SYN
        struct sockaddr_in bind_addr;
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        bind_addr.sin_port = 0;
        if (bind(con->sockfd, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
            exit(1);
        }

        // Dupa ce am legat socket ul la un port, aflam numarul portului la care am legat socket ul
        socklen_t bind_addr_len = sizeof(bind_addr);
        getsockname(con->sockfd, (struct sockaddr *)&bind_addr, &bind_addr_len);
        uint16_t data_port = bind_addr.sin_port;

        // Construim un segment de tip SYN-ACK, care contine numarul portului la care am legat socket ul nou creat,
        // si il trimitem inapoi la clientul care a trimis segmentul de tip SYN
        poli_tcp_ctrl_hdr synack;
        char pkt[sizeof(synack) + sizeof(uint16_t)];
        synack.protocol_id = POLI_PROTOCOL_ID;
        synack.conn_id = conn_id;
        synack.type = POLI_SYN_ACK;
        synack.ack_num = htons(0);
        synack.recv_window = htons(recv_window_available_size(con));

        memcpy(pkt, &synack, sizeof(synack));
        memcpy(pkt + sizeof(synack), &data_port, sizeof(uint16_t));

        sendto(listen_sockfd, pkt, sizeof(pkt), 0, (struct sockaddr *)&client_addr, sizeof(client_addr));

        /* This can be used to set a timer on a socket, useful once we received a
        * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
        * tv_usec = 0)
        struct timeval tv;
        tv.tv_sec = 2;
        tv.tv_usec = 100000;
        if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
            perror("Error");
        } */

        // Setam un timeout pentru socketul de date
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            perror("Error");
        }

        // Dupa ce am trimis segmentul de tip SYN-ACK,
        // asteptam sa primim un segment de tip ACK de la clientul care a trimis segmentul de tip SYN,
        // pentru a finaliza three way handshake ul
        int acked = 0;
        while (!acked) {
            n = recvfrom(con->sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&client_addr, &client_addr_len);
            
            // Daca am primit un segment, verificam daca e de tip ACK, daca are protocol_id corect si daca are dimensiunea corecta
            if (n >= (int)sizeof(poli_tcp_ctrl_hdr)) {
                poli_tcp_ctrl_hdr *ah = (poli_tcp_ctrl_hdr *)buffer;
                if (ah->protocol_id == POLI_PROTOCOL_ID && ah->conn_id == conn_id) {
                    if (ah->type == POLI_ACK || ah->type == POLI_DATA) {
                        acked = 1;
                    }
                }
            }

            // Daca nu am primit un segment de tip ACK, atunci retrimitem segmentul de tip SYN-ACK
            if (!acked) {
                sendto(listen_sockfd, pkt, sizeof(pkt), 0, (struct sockaddr *)&client_addr, sizeof(client_addr));
            }
        }

        // Dezactivam timeout ul pentru socket ul de date,
        // deoarece conexiunea a fost stabilita si nu mai avem nevoie de el pentru three way handshake
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            perror("Error");
        }

        /* Since we can have multiple connection, we want to know if data is available
           on the socket used by a given connection. We use POLL for this */

        // Adaugam socket ul de date al conexiunii noi create in vectorul de poll fds pentru date,
        // si setam evenimentul POLLIN pentru acest socket
        poll_conn_map[fdmax] = conn_id;
        data_fds[fdmax].fd = con->sockfd;
        data_fds[fdmax].events = POLLIN;

        // Adaugam un timer pentru conexiunea nou creata in vectorul de poll fds pentru timere,
        // si setam evenimentul POLLIN pentru acest timer
        timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
        timer_fds[fdmax].events = POLLIN;
        set_timer_interval(timer_fds[fdmax].fd);
        fdmax++;

        // Adaugam conexiunea nou creata in map ul de conexiuni
        // si returnam ID ul conexiunii nou create
        cons.insert({conn_id, con});
        DEBUG_PRINT("Connection established!");
        return conn_id;
    }
}

void init_receiver(int recv_buffer_bytes_arg)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    // Setam capacitatea buffer ului de receive la valoarea data ca parametru
    recv_buffer_bytes = recv_buffer_bytes_arg;

    // Cream socket ul de listen pentru a accepta conexiuni noi
    listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // Configuram adresa de bind pentru socket ul de listen
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(POLI_LISTEN_PORT);

    // Legam socket ul de listen la adresa de ascultare
    if (bind(listen_sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        exit(1);
    }

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
