import socket
import time


class ClientError(Exception):
    pass


class Client:

    def __init__(self,host,port,timeout=None):
        self.host = host
        self.port = port
        self.tineout = timeout
        self.sock = socket.create_connection((self.host, self.port))
        self.sock.settimeout(timeout)

    def put(self, key, value, timestamp=0):
        if timestamp == 0:
            timestamp = int(time.time())
        text = 'put {} {} {}\n'.format(key, value, timestamp)
        try:
            self.sock.send(text.encode("utf8"))
            ans = self.sock.recv(1024).decode("utf-8")
            if ans == 'ok\n\n':
                return
            else:
                raise ClientError()
        except:
            raise ClientError()

    def get(self, text):
        text = "get "+text+"\n"
        self.sock.send(text.encode("utf8"))
        ans = self.sock.recv(1024).decode("utf-8")
        dict = {}
        if ans == 'ok\n\n':
            return dict
        elif ans == 'error\nwrong command\n\n':
            raise ClientError()
        else:
            ans_list = ans.split('\n')
            for line in ans_list:
                if line == 'ok' or line == '' or line == ' ':
                    continue
                part = line.split()
                if len(part)!=3:
                    raise ClientError()
                if part[0] in dict:
                    dict[part[0]].append((int(part[2]), float(part[1])))
                    dict[part[0]].sort()
                else:
                    dict[part[0]] = [(int(part[2]), float(part[1]))]
            return dict

#client = Client("127.0.0.1", 10001, timeout=150)
#print(client.get("*"))
