Am implementat un protocol de transport simplificat pe UDP, inspirat din TCP, cu handshake in
trei pasi, control de flux si retransmisii. Ideea a fost sa creez un mecanism sigur de trimitere
datelor intre client si server.


LIBSEND.CPP - PARTEA DE TRIMITERE

Am folosit un map cu conexiunile active, vectori pentru poll (data_fds si timer_fds) si
poll_conn_map pentru a lega indewxul din poll cu id ul real. Variabilele globale sender_speed
si sender_delay configureaza fereastra de transmisie si timeout ul.

seq_less compara circular numere de secventa pe 16 biti. bytes_in_flight calculeaza cati bytes
sunt deja trimisi si neconfrmati. set_timer_interval seteaza timerul periodic pentru detectare
de timeout uri.

compute_window calculeaza dimensiunea ferestrei pe baza BDP (bandwidth delay product). Minimum
sunt 32 pachete, maximum 512. Bufferul de transmitere are 512 KB.

try_send construieste pachete DATA si le trimnite daca fereastra permite. Pachetele sunt salvate
in inflight pana cand primesc ACK. retransmit retransmite toate pachetele din inflight la
timeout.

send_data e interfata pentru aplicatie. Adauga date in buffer si apeleaza try_send. Folosesc
mutex pentru sincronizare cu sender_handler.

sender_handler ruleza intr un thread. La timeout retransmite tot. La ACK, detecteza duplicate
(la trei duplicate retransmite primul pachet). Daca ACK avansa, sterge pachete confirmate si
continua sa trimita.

setup_connection face handshake ul. Trimit SYN, astpt SYN-ACK cu portul de date, trimit ACK.
Apoi adaug conexiunea in poll si o salvez in map.

init_sender incepe thread ul sender_handler cu parammetrii speed si delay.


LIBRECV.CPP - PARTEA DE PRIMIRE

recv_window_available_size calculeaza spatiul liber in buffer. send_ack trimite ACK cu seq
asteptat si spatiul disponibil.

deliver_in_order_packets verifica out_of_order si livreaza pachete in ordine cand sunt
complete.

recv_data e interfata pentru aplicatie. Asteapta cu condition variable pana vine date, apoi
le copiaza.

receiver_handler rulaza intr un thread. La timeout trimite ACK. La pachet de data valid,
adauga in buffer daca e in ordine sau in out_of_order daca nu. Dupa orice data trimite ACK.

wait4connect face handshake ul pe server. Astept SYN, creez socket nou pe port ales de sistem,
trimit SYN ACK cu portul, astept ACK sau DATA pentru a finaliza. Adaug conexiunea in poll.

init_receiver creeaza socket de listen pe POLI_LISTEN_PORT si porneste thread ul receiver_handler.


PROTOCOLLUL

Doua tipuri de segmente: CONTROL (SYN, SYN_ACK, ACK) si DATA. Handshake in trei pasi: SYN,
SYN_ACK cu port de data, ACK. Dupa, datele se trimit in pachete cu seq. Receiverul confirma
seq asteptat si spatiul din recv_window. Senderul retransmite la timeout sau triple ACK
duplicate. Receiverul stochaza pachete out of order si le livreaza in ordine.