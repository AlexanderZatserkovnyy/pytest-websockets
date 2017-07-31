import logging
import argparse
import base64
import json
import getpass
import socket
import uuid
#
import websocket
###############################################



def parse_and_output_message(parsed_dict):
    
    if 'string' in parsed_dict:
        print('Got string: '+parsed_dict['string'])
    if 'file' in parsed_dict and 'filename' in parsed_dict:
        safename = '/tmp/'+parsed_dict['filename'].strip('./').split('/')[-1].lstrip('.') #paranoia 
        with open(safename, 'wb') as aFile:
            aFile.write(base64.b64decode(parsed_dict['file']))
        logging.info("the file is written on disk: {}".format(safename))

def poll_wsserver(url, json_msg, rec_id):
    logging.info("trying to connect")
    try:
        ws = websocket.create_connection(url)
        logging.info("websocket is opened")
    except:
        return 

    try:
        ws.send(json_msg)
    except:
        logging.error("sending message error",exc_info=True)        

    try:
        result=ws.recv()
    except:
        logging.error("recieving message error",exc_info=True)

    while result!='{}':
        try:
            parsed = json.loads(result)

            if 'id' in parsed and parsed['id'] not in rec_id: # ignore the message if it's received earlier
                rec_id.add(parsed['id'])
                parse_and_output_message(parsed)

            result=ws.recv()
        except:
            logging.error("recieving message error",exc_info=True)

    
    ws.close()
    logging.info("websocket is closed")

##############################################

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Send a string and/or a file via exchange websocket server.')
    parser.add_argument('-u','--url', default='ws://127.0.0.1:8888/',
                        help='url of websocket server ( default: ws://127.0.0.1:8888 )')
    parser.add_argument('-s','--string', help='string to be sent via the websocket')
    parser.add_argument('-f','--file', help='file to be sent via the websocket')
    parser.add_argument('-w','--who', help='sender of the message/file (default: local user name)', default=getpass.getuser()+'@'+socket.getfqdn())
    parser.add_argument('-r','--recipient', help='recipient of the message (e.g. bob, default: to all, except sender)')
    #
    args = parser.parse_args()
    received_ids=set()
    urls = args.url.split(',')

    message={ "sender": args.who }

    if args.string is not None and len(args.string)!=0 :
        message["string"]=args.string

    if args.file is not None and len(args.file)!=0 :
        with open(args.file, 'rb') as aFile:
            message["file"] = base64.b64encode(aFile.read()).decode()
        message["filename"] = args.file.strip('./').split('/')[-1].lstrip('.') #paranoia  

    if args.recipient is not None and len(args.who)!=0:
        message["recipient"]=args.recipient

    if 'string' in message or 'file' in message :    
         message['id'] = str(uuid.uuid4())

    json_str = json.dumps(message)

    for u in urls:
        poll_wsserver(u, json_str, received_ids)
##
#websocket.enableTrace(True)
#


