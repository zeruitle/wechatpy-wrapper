# -*- coding: utf-8 -*-
import queue
import select
import threading


class _PollableQueue(queue.Queue):
    # 定义一种新的Queue，底层有一对互联的socket
    # Create a pair of connected sockets
    def __init__(self, backlog=None):
        import os, socket

        super().__init__()
        self.continue_flag = True
        if os.name == 'posix':
            self._putsocket, self._getsocket = socket.socketpair()
        else:
            # non-POSIX 系统
            # non-POSIX system
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Open a localhost, port = 0 means avaliable random port number from 1024 to 65535
            # 建立一个localhost的服务，port=0 参数表示从1024到65535中随机选择一个可用端口使用
            server.bind(('127.0.0.1', 0))
            print('pollablequeue listening on port:', server.getsockname()[1])
            # Since Python3.5 backlog changed to optional
            # Python3.5后，listen的参数backlog变为可选
            if backlog is None:
                server.listen(1)
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

    def clear(self):
        while not super().empty():
            super().get(False)
            super().task_done()

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


class WechatpyDataPack:
    def __init__(self):
        self.agentid = None
        self.friends = []
        self.msg = []


class WechatpyWrapper(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, name="Sending Thread")
        from wechatpy.session.memorystorage import MemoryStorage
        self.session = MemoryStorage()
        self.pollablequeue = _PollableQueue()
        self.wechatclients = {}

    def set_session(self, redis=None, shove=None, mc=None, prefix="wechatpy"):
        if redis is not None:
            from wechatpy.session.redisstorage import RedisStorage
            self.session = RedisStorage(redis, prefix)
        elif shove is not None:
            from wechatpy.session.shovestorage import ShoveStorage
            self.session = ShoveStorage(shove, prefix)
        elif mc is not None:
            from wechatpy.session.memcachedstorage import MemcachedStorage
            self.session = MemcachedStorage(mc, prefix)

    def set_wechatclient(self, agentid, CorpId, secret):
        from wechatpy.enterprise import WeChatClient
        self.wechatclients[agentid] = WeChatClient(CorpId, secret, session=self.session)

    def getQueue(self):
        return self.pollablequeue

    # override run
    #
    def run(self):
        self._listening_send()

    # listening for new message in queue
    # if incoming, pop new threads for sending
    def _listening_send(self):
        flag = True
        while flag:
            try:
                threads = []
                can_read, _, _ = select.select([self.pollablequeue], [], [])
                for r in can_read:
                    wechatpyDataPack = r.get()
                    if wechatpyDataPack == 'Fin':
                        print("Received fin")
                        return
                    else:
                        # TODO:: can use coroutine here
                        for friend in wechatpyDataPack.friends:
                            thread = self._SendThreads(self.wechatclients[wechatpyDataPack.agentid],
                                                       wechatpyDataPack.msg,
                                                       friend,
                                                       self.pollablequeue)
                            thread.start()
                            threads.append(thread)
                flag = flag and self.pollablequeue.continuum()
                for t in threads:
                    t.join()
                # print("All send finished")
            except Exception as e:
                pass

    class _SendThreads(threading.Thread):
        def __init__(self, client, msg_list, friend, pollablequeue):
            threading.Thread.__init__(self, name="Send_2_{}".format(friend))
            self.clients = client
            self.msg_pack = msg_list
            self.friend = friend
            self.pollablequeue = pollablequeue

        def run(self):
            self._sending()

        def _sending(self):
            try:
                time.sleep(msg_pack['retry'])
                # print("Try send to {} now".format(friend))
                r = client['client'].message.send_text(client['agentid'], friend, msg_pack['msg'])
                # print("Send to {} Debug: {}".format(friend, r))
            except InvalidCorpIdException as e:
                logger.exception("CorpID Issue: {}".format(e))
                raise
            except Exception:
                logger.error("Connection Issue, put message back, retry: {}".format(msg_pack['retry']))
                msg_pack = {'client': msg_pack['client'], 'msg': msg_pack['msg'], 'friends': [friend],
                            'retry': msg_pack['retry'] + 1}
                # will retry send to everyone in this suite
                retry_queue.put(msg_pack)  # put msg back into queue if connection issue, wait for next send
                logger.exception("Sending Failed")
                pass


if __name__ == "__main__":
    wechat_wrapper = WechatpyWrapper()
