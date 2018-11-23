# -*- coding: utf-8 -*-

import queue


class _PollableQueue(queue.Queue):
    # 定义一种新的Queue，底层有一对互联的socket
    # Create a pair of connected sockets
    def __init__(self, backlog=None):
        import os,socket

        super().__init__()
        self.continue_flag = True
        if os.name == 'posix':
            self._putsocket, self._getsocket = socket.socketpair()
        else:
            # non-POSIX 系统
            # non-POSIX system
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #Open a localhost, port = 0 means avaliable random port number from 1024 to 65535
            #建立一个localhost的服务，port=0 参数表示从1024到65535中随机选择一个可用端口使用
            server.bind(('127.0.0.1', 0))
            print('pollablequeue listening on port:', server.getsockname()[1])
            #Since Python3.5 backlog changed to optional
            #Python3.5后，listen的参数backlog变为可选
            if backlog is None:
                server.listen()
            else:
                server.listen(backlog)
            # 创建一个服务器socket，之后立刻创建客户端socket并连接到服务器上
            # create a sever socket，then create a client socket and connect to server
            self._putsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._putsocket.connect(server.getsockname())
            self._getsocket, _ = server.accept()
            server.close()

    def fileno(self):
        # 返回套接字的文件描述符
        # return fileno of socket
        return self._getsocket.fileno()

    def put(self, item):
        super().put(item)
        self._putsocket.send(b'x')

    def get(self):
        self._getsocket.recv(1)
        return super().get()

    def empty(self):
        return super().empty()

    def end(self, fin):
        # 使用自定义字符通知消费者终止，在close()前使用
        # use customized string to notify consumer thread to terminate, call before close()
        self.continue_flag = False
        self.put(fin)

    def continuum(self):
        # 获取线程终止标志
        # get consumer thread continue flag
        return self.continue_flag

    def close(self):
        self._putsocket.close()
        self._getsocket.close()





class wechatpyWrapper():
    from wechatpy.session.memorystorage import MemoryStorage

    def __init__(self, session_interface = MemoryStorage()):
        self.session = session_interface
        self.pollablequeue = _PollableQueue()

    def getWeChatClient(self,CorpId,secret):
        from wechatpy.enterprise import WeChatClient
        return WeChatClient(CorpId,secret,session=self.session)




if __name__ == "__main__":
    wechat_wrapper = wechatpyWrapper()


