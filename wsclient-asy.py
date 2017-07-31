#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import getpass
import socket
import json
import base64
#
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen
from tornado.websocket import websocket_connect

class Client(object):
    received_ids=set()
    
    def __init__(self, url, recipient, timeout):
        self.url = url
        self.timeout = timeout
        self.recipientmsg = '{"sender": "'+recipient+'"}'
        self.ws = None
        self.connect()
        PeriodicCallback(self.keep_alive, 20000).start()

    @gen.coroutine
    def connect(self):
        logging.info("trying to connect")
        try:
            self.ws = yield websocket_connect(self.url)
        except:
            #logging.error("connection error",exc_info=True)
            print ( self.url+ 'inactive' )
        else:
            logging.info("websocket is opened")
            self.ws.write_message(self.recipientmsg) 
            self.run()

    @gen.coroutine
    def run(self):
        while True:
            msg = yield self.ws.read_message()
            if msg is None:
                logging.info("connection closed")
                self.ws = None
                break
            else:
                parsed = json.loads(msg)
                if 'id' in parsed and parsed['id'] not in Client.received_ids:
                    Client.update_ids(parsed['id'])
                    if 'string' in parsed:
                        print('Got string: '+parsed['string'])
                    if 'file' in parsed and 'filename' in parsed:
                        safename = '/tmp/'+parsed['filename'].strip('./').split('/')[-1].lstrip('.') #paranoia
                        with open(safename, 'wb') as aFile:
                            aFile.write(base64.b64decode(parsed['file']))
                        logging.info("the file is written on disk: {}".format(safename))

    def keep_alive(self):
        if self.ws is None:
            self.connect()
        else:
            self.ws.write_message(self.recipientmsg)

    @classmethod
    def update_ids(cls,id_str):
        cls.received_ids.add(id_str)


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Get string and/or a file messages from exchange websocket server.')
    parser.add_argument('-u','--url', default='ws://127.0.0.1:8888/', help='url of websocket server ( default: ws://127.0.0.1:8888 )')
    parser.add_argument('-r','--recipient', help='recipient of the message/file (default: local user name)', default=getpass.getuser()+'@'+socket.getfqdn())
    args = parser.parse_args()
    urls = args.url.split(',')
    client = {} 
    try:
        for u in urls: 
            client[u] = Client(u, args.recipient, 5)
        IOLoop.instance().start()
    except KeyboardInterrupt: 
        exit(0)
