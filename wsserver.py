import logging
import uuid
#
import tornado.web
import tornado.websocket
import tornado.ioloop
import tornado.options
from tornado.options import define, options

define("port", default=8888, help="bind server to the port", type=int)
define("address", default="", help="bind server to the ip address", type=str)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [ (r"/", ExchangeWebSocket), ]
        settings = dict(
            cookie_secret="8qZR9GEKvaQIB1PW",
            xsrf_cookies=True,
        )
        super(Application, self).__init__(handlers, **settings)

class ExchangeWebSocket(tornado.websocket.WebSocketHandler):
    parties = set()
    cache = []
    cache_size = 100
    wsuser = ''
    wsid = ''
    def open(self):
        ExchangeWebSocket.parties.add(self)
        self.wsid = str(uuid.uuid4())
        logging.info("websocket is opened "+self.wsid)

    @classmethod
    def update_cache(cls, parcel):
        cls.cache.append(parcel)
        if len(cls.cache) > cls.cache_size:
            cls.cache = cls.cache[-cls.cache_size:]

    @classmethod
    def newmessage(cls, parcel):
        delivered = False
        for partie in cls.parties:
            if 'recipient' in parcel and parcel['recipient']==partie.wsuser :
                try:
                    partie.write_message(parcel)
                    partie.write_message('{}') #for sync clients: "you are free"
                    delivered = True
                    logging.info("send the message to the connected partie:"+partie.wsuser)
                except:
                    logging.error("sending message error", exc_info=True)
        if not delivered:
            ExchangeWebSocket.update_cache(parcel)

    def on_message(self, message):
        try:
           parsed = tornado.escape.json_decode(message)
        except:
           logging.error("wrong message format", exc_info=True)
        logging.info("got message")

        if 'string' in parsed or 'file' in parsed : 
            ExchangeWebSocket.newmessage(parsed)
        
        if 'sender' in parsed: # send parcels from cache to him 
            self.wsuser = parsed['sender'] #mark the websocket as used by him
            logging.info('user '+self.wsuser+' to the socket '+self.wsid) ## for debugging
            for p in ExchangeWebSocket.cache[:]:
                if 'recipient' in p and p['recipient'] == parsed['sender']:
                    try:
                        self.write_message(tornado.escape.json_encode(p))
                        ExchangeWebSocket.cache.remove(p) # the parcel delivered, remove it 
                    except:
                        logging.error("sending message error", exc_info=True)

            try:
                self.write_message('{}') #for sync clients: "you are free"
            except:
                logging.error("sending message error", exc_info=True)

    def on_close(self):
        ExchangeWebSocket.parties.remove(self)
        logging.info("websocket is closed "+self.wsid)


if __name__ == "__main__":
    try:
        tornado.options.parse_command_line()
        app = Application()
        app.listen(options.port,options.address)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.current().stop()

