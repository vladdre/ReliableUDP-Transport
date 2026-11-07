#include <sys/socket.h>
#include <sys/types.h>
#include <sys/timerfd.h>
#include <netinet/ip.h> 
#include <stdio.h>
#include "./include/lib.h"
#include <map>
#include <cstring>
#include <assert.h>
#include <unistd.h>
#include <cstdlib>
#include <pthread.h>
#include <sys/poll.h>
#include <fcntl.h>

using namespace std;

/* We use extern to use the variables defined either in
   librecv or libsend */
extern std::map<int, struct connection *> cons;
extern struct pollfd data_fds[MAX_CONNECTIONS];
extern struct pollfd timer_fds[MAX_CONNECTIONS];
extern int fdmax;

// Declaram extern un vector care tine legatura intre indexul din poll_fds si id ul conexiunii reale
extern int poll_conn_map[MAX_CONNECTIONS];

int recv_message_or_timeout(char *buff, size_t len, int *conn_id)
{
    int ret, i;

    /* We check if data is available on a socket or if a timer has expired */

    // Setam timeout ul la 10 ms
    ret = poll(data_fds, fdmax, 10);
    assert(ret >= 0);
    ret = poll(timer_fds, fdmax, 0);
    assert(ret >= 0);

    for (i = 0; i < fdmax; i++) {
        /* Data available on a socket */
        if (data_fds[i].revents & POLLIN) {

            struct sockaddr_in servaddr;
            socklen_t slen = sizeof(struct sockaddr_in);

            int n = recvfrom(data_fds[i].fd, buff, len, MSG_WAITALL, (struct sockaddr *) &servaddr , &slen);

            // Salvam indexul real al conexiunii in conn_id
            *conn_id = poll_conn_map[i];
            return n;
        }

        /* A timer has expired on a connection */
        if (timer_fds[i].revents & POLLIN) {

            char dummybuf[8];
            read(timer_fds[i].fd, dummybuf, 8);

            // Salvam indexul real al conexiunii in conn_id
            *conn_id = poll_conn_map[i];
            return -1;
        }
    }

    /* If nothing happened, return -14. */
    return -14;
}
