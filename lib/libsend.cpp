#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "./include/lib.h"
#include "./include/utils.h"
#include "./include/protocol.h"
#include <cassert>
#include <poll.h>
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

// Facem 2 variabile ce retin viteza de trimitere a datelor si delay ul intre trimiterea a 2 pachete consecutive
static int sender_speed = 0;
static int sender_delay = 0;

// Facem un vector ce retine cele mai recente ACK uri primite pentru fiecare conexiune
// si unul pentru a numara cate ACK uri duplicate am primit pentru fiecare conexiune
static uint16_t last_acks_arrived[MAX_CONNECTIONS];
static int duplicate_acks_count[MAX_CONNECTIONS];

// Facem o functie care compara 2 nr de seq (returneaza 1 daca a e mai mic decat b si daca nu 0)
static int seq_less (uint16_t a, uint16_t b) {
    // Verificam daca a e mai mic decat b, tinand cont ca numerele de seq sunt modulo 65536
    if ((uint16_t) (b - a) < 32768) {
        // Daca a e si diefrit de b returnam 1
        if (a != b) {
            return 1;
        }
    }

    // Altfel, a nu e mai mic decat b si returnam 0
    return 0;
}

// Facem o funcie ce returneaza cati bytes sunt pe drum
static int bytes_in_flight (struct connection *c) {
    int bytes = 0;

    // Calculam cati bytes sunt pe drum, adunand dimensiunea datelor din toate pachetele care sunt inca inflight
    // si scazand dimensiunea header ului din fiecare pachet
    for (auto &p : c->inflight) {
        bytes += p.second.size() - sizeof(poli_tcp_data_hdr);
    }

    return bytes;
}

// Facem o functie ce seteaza un timer cu un delay dat pt un fd dat
static void set_timer_interval (int fd, int delay_ms) {
    int timer_ms = TIMEOUT_SEND(delay_ms) * 2;

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

// Facem o functie care seteaza limitele la window ul pt transmitator
static void compute_window (struct connection *c) {
    // Calculam bandwidth delay product ul pentru conexiunea data,
    // folosind viteza de trimitere a datelor si delay ul intre trimiterea a 2 pachete consecutive
    int bdp = (sender_speed * 1000000.0 / 8) * (sender_delay * 2.0 / 1000);

    // Daca bdp e mai mic decat dimensiunea maxima a datelor dintr un pachet
    if (bdp < MAX_DATA_SIZE) {
        bdp = MAX_DATA_SIZE;
    }

    // Setam max_window_seq pentru conexiunea data, folosind bdp calculat,
    // astfel incat sa avem intotdeauna cel putin 4 pachete
    c->max_window_seq = bdp / MAX_DATA_SIZE + 4;

    // Daca max_window_seq e mai mic de 32, atunci il setam la 32
    if (c->max_window_seq < 32) {
        c->max_window_seq = 32;
    }

    // Daca max_window_seq e mai mare de 512, atunci il setam la 512
    if (c->max_window_seq > 512) {
        c->max_window_seq = 512;
    }

    // Alocam capacitatea la 512 KB pentru buffer ul de transmitere
    c->send_buf_cap = 512 * 1024;
    // Setam empty_size_recv_window cu bdp pt a cuprinde tot bdp ul calculat
    c->empty_size_recv_window = bdp;
}

// Facem o functie care incearca sa trimita datele din buffer ul de transmitere, daca e posibil, respectand limitele ferestrei
static void try_send (struct connection *c) {
    // Cat timp avem loc in fereastra de transmitere si avem date de trimis in buffer ul de transmitere
    while (c->inflight_count < c->max_window_seq && !c->app_send_buf.empty()) {
        // Calculam dimensiunea unei bucati pe care putem sa o trimitem
        int size_bucata = MAX_DATA_SIZE;
        if (size_bucata > (int)c->app_send_buf.size()) {
            size_bucata = c->app_send_buf.size();
        }

        // Daca dimeniunea bucatii + ce bytes mai sunt in flight e > decat cat avem liber in window receive
        // at dam break
        if (bytes_in_flight(c) + size_bucata > c->empty_size_recv_window) {
            break;
        }

        // Construim un pachet de tip DATA cu bucata de date de trimis
        char packet[MAX_SEGMENT_SIZE];
        poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)packet;

        // Configuram headerul pachetului de trimis si ii punem datele
        hdr->protocol_id = POLI_PROTOCOL_ID;
        hdr->conn_id = c->conn_id;
        hdr->type = POLI_DATA;
        hdr->seq_num = htons(c->next_send_seq_num);
        hdr->len = htons(size_bucata);
        memcpy(packet + sizeof(poli_tcp_data_hdr), c->app_send_buf.data(), size_bucata);

        // Trimitem pachetul de date construit la destinatia conexiunii
        int packet_len = sizeof(poli_tcp_data_hdr) + size_bucata;
        sendto(c->sockfd, packet, packet_len, 0, (struct sockaddr *)&c->servaddr, sizeof(c->servaddr));

        // Adaugam pachetul trimis in map ul de pachete inflight, folosind numarul de seq ca si cheie
        c->inflight[c->next_send_seq_num] = vector<char>(packet, packet + packet_len);
        c->next_send_seq_num++;
        c->inflight_count++;

        // Scoatem bucata trimisa din buffer ul de transmitere
        c->app_send_buf.erase(c->app_send_buf.begin(), c->app_send_buf.begin() + size_bucata);
    }
}

// Facem o functie care retransmite toate pachetele inflight
static void retransmit (struct connection *c) {
    // Retransmitem toate pachetele inflight, iterand prin map ul de pachete inflight si
    // trimitand fiecare pachet inapoi la destinatie
    for (auto &p : c->inflight) {
        sendto(c->sockfd, p.second.data(), p.second.size(), 0, (struct sockaddr *)&c->servaddr, sizeof(c->servaddr));
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    // Daca adaugarea datelor noi in buffer ul de transmitere depaseste capacitatea buffer ului
    if ((int)cons[conn_id]->app_send_buf.size() + len > cons[conn_id]->send_buf_cap) {
        // Dam unlock la conexiune si returnam -1
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }

    // Adaugam datele noi in buffer ul de transmitere
    cons[conn_id]->app_send_buf.insert(cons[conn_id]->app_send_buf.end(), buffer, buffer + len);
    // Incercam sa trimitem datele noi adaugate, daca e posibil
    try_send(cons[conn_id]);
    size = len;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        // Daca res e -1 inseamn ca e timeout, deci daca conn_id e valid si exista conex in cons, dam continue
        if (conn_id == -1 || cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        // Daca res == -1 ins ca a fost timeout deci retransmitem toate pachetele inflight pentru conexiunea data,
        // si apoi dam unlock la conexiune si continuam
        if (res == -1) {
            retransmit(cons[conn_id]);
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        // Daca res are macar dimensiunea unui header de control, facem un header pt buffer
        if (res >= (int)sizeof(poli_tcp_ctrl_hdr)) {
            poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;

            // Daca protocol_id e corect si e un segment de tip SYN-ACK pentru conexiunea data
            if (hdr->protocol_id == POLI_PROTOCOL_ID) {
                if (hdr->type == POLI_SYN_ACK) {
                    // Facem un ACK de tip control cu numarul de ACK egal cu 0 si configuram campurile
                    poli_tcp_ctrl_hdr ack;
                    ack.protocol_id = POLI_PROTOCOL_ID;
                    ack.conn_id = conn_id;
                    ack.type = POLI_ACK;
                    ack.ack_num = htons(0);
                    ack.recv_window = htons(0);

                    // Trimitem ACK ul construit inapoi la destinatia conexiunii
                    sendto(cons[conn_id]->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&cons[conn_id]->servaddr, sizeof(cons[conn_id]->servaddr));
                    
                    // Daca am primit un segment de tip ACK si conn_id e corect
                } else if (hdr->type == POLI_ACK && hdr->conn_id == conn_id) {
                    uint16_t ack_num = ntohs(hdr->ack_num);
                    cons[conn_id]->empty_size_recv_window = ntohs(hdr->recv_window);

                    // Daca numarul de ACK primit e egal cu ultimul numar de ACK primit pentru conexiunea data
                    if (ack_num == last_acks_arrived[conn_id]) {
                        // Incrementam contorul de ACK uri duplicate pentru conexiunea data
                        duplicate_acks_count[conn_id]++;
                        
                        // Daca am primit 3 ACK uri duplicate pentru conexiunea data si avem pachete inflight
                        // atunci retransmitem primul pachet inflight pentru conexiunea data
                        if (duplicate_acks_count[conn_id] == 3) {
                            if (!cons[conn_id]->inflight.empty()) {
                                auto p = cons[conn_id]->inflight.begin();
                                sendto(cons[conn_id]->sockfd, p->second.data(), p->second.size(), 0, (struct sockaddr *)&cons[conn_id]->servaddr, sizeof(cons[conn_id]->servaddr));
                            }
                        }

                        // Daca numarul de ACK primit e mai mare decat ultimul numar de ACK primit pentru conexiunea data
                    } else if (seq_less(last_acks_arrived[conn_id], ack_num)) {
                        last_acks_arrived[conn_id] = ack_num;
                        duplicate_acks_count[conn_id] = 0;
                        vector<uint16_t> to_remove;

                        // Scoatem din map ul de pachete inflight toate pachetele care au numarul de seq mai mic decat numarul de ACK primit
                        // pentru conexiunea data si decrementam contorul de pachete inflight pentru conexiunea data
                        for (auto &p : cons[conn_id]->inflight) {
                            if (seq_less(p.first, ack_num))
                                to_remove.push_back(p.first);
                        }

                        for (uint16_t s : to_remove) {
                            cons[conn_id]->inflight.erase(s);
                            cons[conn_id]->inflight_count--;
                        }

                        cons[conn_id]->send_base = ack_num;
                    }

                    // Daca am primit un segment de tip ACK,
                    // atunci incercam sa trimitem datele din buffer ul de transmitere, daca e posibil
                    try_send(cons[conn_id]);
                }
            }
        } else {
            // Daca am primit un segment care nu are macar dimensiunea unui header de control, atunci incercam
            // sa trimitem datele din buffer ul de transmitere, daca e posibil
            try_send(cons[conn_id]);
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = new struct connection();
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // Configuram campurile conexiunii nou create, folosind parametrii dati si valorile implicite pentru celelalte campuri
    con->conn_id = conn_id;
    con->next_send_seq_num = 0;
    con->send_base = 0;
    con->inflight_count = 0;
    compute_window(con);

    // Configuram adresa serverului pentru conexiunea nou creata, folosind ip ul dat si portul de listen
    memset(&con->servaddr, 0, sizeof(con->servaddr));
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = htons(POLI_LISTEN_PORT);

    // Facem un mux pentru conexiunea nou creata
    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->con_cond, NULL);

    // Facem un SYN pt conex noua
    poli_tcp_ctrl_hdr syn;
    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.conn_id = 0;
    syn.type = POLI_SYN;
    syn.ack_num = htons(0);
    syn.recv_window = htons(0);

    // Trimitem SYN ul construit la destinatia conexiunii noi create, adica la serverul cu care vrem sa ne conectam, pe portul de listen
    sendto(con->sockfd, &syn, sizeof(syn), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    char buf[MAX_SEGMENT_SIZE];
    struct sockaddr_in from;
    socklen_t flen = sizeof(from);
    int got_synack = 0;
    uint16_t data_port = 0;

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    // Setam un timeout pentru socket ul de date, pentru a putea retransmite SYN ul daca nu primim un SYN-ACK inapoi intr un timp ok
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error");
    }

    // Cat timp nu am primit un SYN-ACK inapoi pentru conexiunea nou creata
    while (!got_synack) {
        // Incercam sa primim un segment de la server
        int n = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&from, &flen);

        // Daca nu am primit niciun segment inapoi, atunci retrimitem SYN ul la server si continuam sa asteptam un SYN-ACK inapoi
        if (n < 0) {
            sendto(con->sockfd, &syn, sizeof(syn), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
            continue;
        }

        // Daca am primit un segment inapoi, dar dimensiunea lui e mai mica decat dimensiunea unui header de control + dimensiunea unui camp de port,
        // atunci continuam sa asteptam un SYN-ACK inapoi
        if (n < (int)(sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t))) {
            continue;
        }

        // Daca cond ed mai sus sunt respectate, si segm nu e un SYN-ACK cu protocol_id corect, atunci continuam sa asteptam un SYN-ACK inapoi
        poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;
        if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != POLI_SYN_ACK) {
            continue;

        }

        // Daca am primit un segment de tip SYN-ACK cu protocol_id corect,
        // extragem din el portul de date pe care serverul vrea sa il folosim pentru conexiunea nou creata
        conn_id = hdr->conn_id;
        con->conn_id = conn_id;
        con->empty_size_recv_window = ntohs(hdr->recv_window);
        memcpy(&data_port, buf + sizeof(poli_tcp_ctrl_hdr), sizeof(uint16_t));
        got_synack = 1;
    }

    // Actualizam portul din adresa serverului pentru conexiunea nou creata cu portul de date primit in SYN-ACK ul de la server
    con->servaddr.sin_port = data_port;

    // Facem un ACK de tip control cu numarul de ACK egal cu 0 si configuram campurile
    poli_tcp_ctrl_hdr ack;
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = conn_id;
    ack.type = POLI_ACK;
    ack.ack_num = htons(0);
    ack.recv_window = htons(0);

    // Trimitem ACK ul construit inapoi la destinatia conexiunii noi create
    sendto(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */

    // Adaugam socket ul de date al conexiunii noi create in vectorul de poll fds pentru date,
    // si setam evenimentul POLLIN pentru acest socket
    poll_conn_map[fdmax] = conn_id;
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */

    // Adaugam un timer pentru conexiunea nou creata in vectorul de poll fds pentru timere,
    // si setam evenimentul POLLIN pentru acest timer,
    // si setam intervalul timer ului folosind delay ul intre trimiterea a 2 pachete consecutive
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    set_timer_interval(timer_fds[fdmax].fd, sender_delay);
    fdmax++;

    // Initializam pentru conexiunea nou creata ultimul numar de ACK primit cu 0 si contorul de ACK uri duplicate cu 0
    last_acks_arrived[conn_id] = 0;
    duplicate_acks_count[conn_id] = 0;

    // Dezactivam timeout ul pentru socket ul de date,
    // deoarece conexiunea a fost stabilita si nu mai avem nevoie de el pentru three way handshake
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    // Returnam ID ul conexiunii nou create
    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    // Setam viteza de trimitere a datelor si delay ul intre trimiterea a 2 pachete consecutive
    // cu valorile date ca parametri
    sender_speed = speed;
    sender_delay = delay;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
