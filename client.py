from socket import *
from _thread import *
import json
import events
import threading
from heapq import *
import time
import sys
import lamportclock


# Abrindo arquivo
with open('config.json') as configfile:
    configdata = json.load(configfile)

SIZE_MSGS = configdata["requests"]
MODE = configdata["mode"]
NUMBER_SERVERS = 5
threads = []


class Client:
    hostname = ''
    port = 0
    socket = None
    processID = 0
    replyList = []
    time = 0

    # Inicializando o cliente e seus atributos
    def __init__(self, ID):
        print("Rodando cliente: " + ID)
        ip, port = configdata["systems"][ID]
        print("port " + port)
        print("ip " + ip)
        self.port = port
        self.processID = int(self.port) - 3000
        self.hostname = ip
        self.reqQueue = []
        self.clock = lamportclock.LamportClock(
            time, self.processID, self.reqQueue)
        self.replyList = []
        self.lock = threading.RLock()
        self.socket = socket(AF_INET, SOCK_STREAM)
        # print("Evento: \n" + str(events.event) +
        #       " Contador de eventos: " + str(events.numofAccess))
        start_new_thread(self.startListening, ())
        start_new_thread(self.initMessages, ())

        while True:
            pass

    def receiveMessages(self, conn, addr):
        msg = conn.recv(1024).decode()
        time.sleep(delay)
        if "release" in msg:
            removed = self.removefromRequestQ(self.reqQueue)
            print("Release realizado")
            print("Hora atual no relógio de lamport: " +
                  self.clock.getLamportTime())
            self.clock.incrementTime()
            self.printRequestQ(self.reqQueue)
            print("Digite 1 para criar um evento: \n")
        if "Reply" in msg:
            lamtime = msg.split()[3]
            lamtime = lamtime[0]
            # Verificando se há respostas duplicadas, se houver
            seen = set(self.replyList)
            print("Seen " + str(seen))
            if msg.split()[2] not in seen:
                seen.add(msg.split()[2])
                self.replyList.append(msg.split()[2])
            self.clock.compareTime(int(lamtime))
            # print("Todos os replys foram recebidos")
            print("Hora atual no relógio de lamport: " +
                  self.clock.getLamportTime())
            print("A lista de replys é: ")
            self.printReplyList(self.replyList)
        if "Add" in msg:
            port = msg.split()[3]
            ltime = msg.split()[4]
            self.addtoRequestQueue(
                self.reqQueue, float(ltime), "S"+str(port[-1:]))
            ltime = ltime[0]
            self.clock.incrementTime()
            self.clock.compareTime(int(ltime))
            print("Request enviado ...")
            print("Hora atual no relógio de lamport: " +
                  self.clock.getLamportTime())
            configdata["systems"]["S"+str(port[-1:])]
            self.sendReply(
                configdata["systems"]["S"+str(port[-1:])][0],
                configdata["systems"]["S"+str(port[-1:])][1]
            )
            self.printRequestQ(self.reqQueue)
        if "events" in msg:
            access = msg.split()[-1]
            events.numofAccess = int(access)
        print(msg)

    def initMessages(self):
        if MODE == "input":
            while True:
                message = input('Digite 1 para criar um evento: ')
                message = int(message)
                if (message == 1):
                    start_new_thread(self.callEvent, ())
                else:
                    print('Entrada inválida')
        else:
            for i in range(SIZE_MSGS):
                time.sleep(delay)
                start_new_thread(self.callEvent, ())

    def callEvent(self):

        systemName = "S" + str(self.processID)
        lamporttime = float(self.clock.getLamportTime())
        self.addtoRequestQueue(self.reqQueue, lamporttime, systemName)
        # self.printRequestQ(self.reqQueue)
        time.sleep(delay)
        addMessage = "Add na fila: " + \
            str(self.port) + " " + str(lamporttime)
        item = str(lamporttime).split(".")
        print("Add na fila: (" + item[0] + ",S" + item[1] + ")")
        self.sendToAll(addMessage)  # Add to all request Queues
        self.printRequestQ(self.reqQueue)
        time.sleep(delay)
        topofQ = self.reqQueue[0][1]
        # print("O topo da fila é " + str(topofQ))

        while True:
            topofQ = self.reqQueue[0][1]
            if topofQ == ("S" + str(self.processID)) and len(self.replyList) == NUMBER_SERVERS - 1:
                break
            else:
                time.sleep(delay)

        time.sleep(delay)
        self.lock.acquire(events.numofAccess)
        events.numofAccess = events.numofAccess + 1
        tosend = "Contador atual de eventos: " + str(events.numofAccess)
        print(tosend)
        self.sendToAll(tosend)
        self.removefromRequestQ(self.reqQueue)
        self.replyList.clear()
        time.sleep(delay)
        releaseMessage = "Mensagem de release de recurso da porta" + \
            str(self.port)
        self.sendToAll(releaseMessage)
        print("Hora atual no relogio de lamport: " + self.clock.getLamportTime())
        self.lock.release()

    def startListening(self):
        try:
            self.socket.bind(('', int(self.port)))
            self.socket.listen(5)
            # print("Servidor rodando na porta " + str(self.port))
            while True:
                c, addr = self.socket.accept()
                conn = c
                # print('Obteve conexão de ' + str(addr))
                # connection dictionary
                start_new_thread(self.receiveMessages, (conn, addr))
        except(gaierror):
            print('Ocorreu um erro ao conectar ao host')
            self.socket.close()
            sys.exit()

    def sendReply(self, ip, port):
        rSocket = socket(AF_INET, SOCK_STREAM)
        rSocket.connect((ip, int(port)))
        item = self.clock.getLamportTime().split(".")
        print("Reply de " + item[1])
        reply = "Reply de " + str(self.port) + \
            " " + self.clock.getLamportTime()
        rSocket.send(reply.encode())
        print("Enviando reply para porta " + str(port) + '\n')
        rSocket.close()

    # Enviando mensagens para todos
    def sendToAll(self, message):
        for i in configdata["systems"]:
            if (configdata["systems"][i][1] == self.port):
                continue
            else:
                cSocket = socket(AF_INET, SOCK_STREAM)
                ip, port = configdata["systems"][i]
                port = int(port)
                cSocket.connect((ip, port))
                # print('Conectado na porta ' +
                #       configdata["systems"][i][1])
                cSocket.send(message.encode())
                time.sleep(delay)
                # print('Mensagem enviada para o cliente da porta ' + str(port))
                cSocket.close()

    def addtoRequestQueue(self, reqQueue, time, systemName):
        heappush(reqQueue, (time, systemName))

    def removefromRequestQ(self, queue):
        return heappop(queue)

    def printRequestQ(self, queue):
        # print("A fila de requests atual é:  ")
        # nsmallest(n,fila) retorna as n linhas ordenadas por coluna em ordem crescente
        sorted = nsmallest(len(queue), queue)
        for i in sorted:
            item = str(i[0]).split(".")
            print("("+item[0]+","+i[1]+")")

    def printReplyList(self, rlist):
        for i in rlist:
            print(i)

    def closeSocket(self):
        self.socket.close()


time.sleep(10)
events = events.Event(0)
delay = configdata["delay"]
ID = sys.argv[1]
c = Client(ID)
