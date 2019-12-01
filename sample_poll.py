# -*- coding: utf-8 -*-
import json
from mns.mns_exception import MNSExceptionBase
import logging
from mns.account import Account
try:
    import configparser as ConfigParser
except ImportError:
    import ConfigParser as ConfigParser


class MNSClient(object):
    def __init__(self):
        
        ## Setup
        cfgFN = "sample.cfg"
        required_ops = [("Base", "AccessKeyId"), ("Base", "AccessKeySecret"), ("Base", "Endpoint")]
        optional_ops = [("Optional", "SecurityToken")]
        queue_ops = [("Queue", "QueueName")]

        parser = ConfigParser.ConfigParser()
        parser.read(cfgFN)
        for sec,op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec,op,cfgFN))
                sys.stderr.write("RTFM to get help inforamtion.\n")
                sys.exit(1)

        accessKeyId = parser.get("Base", "AccessKeyId")
        accessKeySecret = parser.get("Base", "AccessKeySecret")
        endpoint = parser.get("Base", "Endpoint")
        
        self.account =  Account(endpoint, accessKeyId, accessKeySecret)
        self.queue_name = parser.get("Queue", "QueueName")
        self.listeners = dict()

#   Setup Listener
    # def regist_listener(self, listener, eventname=''):
    #     if eventname in self.listeners.keys():
    #         self.listeners.get(eventname).append(listener)
    #     else:
    #         self.listeners[eventname] = [listener]

    def run(self):
        queue = self.account.get_queue(self.queue_name)
        ## Ugly hack as request parameter do not get saved further down the code
        queue.encoding = False
        while True:
            try:
                message = queue.receive_message(wait_seconds=5)
                event = json.loads(message.message_body)
                print(event)
                # if event['name'] in self.listeners:
                #     for listener in self.listeners.get(event['name']):
                #         listener.process(event)
                queue.delete_message(receipt_handle=message.receipt_handle)
            except MNSExceptionBase as e:
                if e.type == 'QueueNotExist':
                    logging.error('Queue %s not exist, please create queue before receive message.', self.queue_name)
                else:
                    logging.error('No Message, continue waiting')


class BasicListener(object):
    def process(self, event):
        pass
        

class ListenerLog(BasicListener):
    def process(self, event):
        pass


mns_client = MNSClient()
#mns_client.regist_listener(ListenerLog())
mns_client.run()