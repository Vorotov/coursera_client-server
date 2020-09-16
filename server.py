import asyncio


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


class ClientServerProtocol(asyncio.Protocol):
    big_data = {}

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())

    @staticmethod
    def process_data(data):
        if data == "\n":
            return "error\nwrong command\n\n"
        data = data[0:len(data) - 1]
        split_data = data.split()
        if split_data[0] == "get":
            if len(split_data) != 2:
                return "error\nwrong command\n\n"
            if split_data[1] == "*":
                resp = "ok\n"
                for k, el in ClientServerProtocol.big_data.items():
                    for key, value in el.items():
                        resp += "{0} {1} {2}\n".format(k, value, key)
                resp += "\n"
                return resp
            elif split_data[1] in ClientServerProtocol.big_data:
                resp = "ok\n"
                for key, value in ClientServerProtocol.big_data[split_data[1]].items():
                    resp += "{0} {1} {2}\n".format(split_data[1], value, key)
                resp += "\n"
                return resp
            else:
                return "ok\n\n"
        elif split_data[0] == "put":
            if len(split_data) != 4:
                return "error\nwrong command\n\n"
            try:
                value = float(split_data[2])
                timestamp = int(split_data[3])
                if split_data[1] in ClientServerProtocol.big_data:
                    ClientServerProtocol.big_data[split_data[1]][timestamp] = value
                else:
                    ClientServerProtocol.big_data[split_data[1]] = {timestamp: value}
                return "ok\n\n"
            except:
                return "error\nwrong command\n\n"
        else:
            return "error\nwrong command\n\n"

#run_server("127.0.0.1", 10001)
