#!/usr/bin/env python
#coding=utf8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import time
from mns.account import Account
from mns.queue import *
from mns.topic import *
from mns.subscription import *
from mns.mns_request import *
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

        parser = ConfigParser.ConfigParser()
        parser.read(cfgFN)
        for sec,op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec,op,cfgFN))
                sys.stderr.write("RTFM to get help inforamtion. (Just kidding, there is no documentation))\n")
                sys.exit(1)

        accessKeyId = parser.get("Base", "AccessKeyId")
        accessKeySecret = parser.get("Base", "AccessKeySecret")
        endpoint = parser.get("Base", "Endpoint")
        securityToken = ""
        if parser.has_option("Optional", "SecurityToken") and parser.get("Optional", "SecurityToken") != "$SecurityToken":
            securityToken = parser.get("Optional", "SecurityToken")
        self.my_account = Account(endpoint, accessKeyId, accessKeySecret, securityToken)

        self.topicName = "test-topic"
        self.queueName = "MyQueue-sample"


    def setupMNS(self):

        ## Create/Get Topic
        self.my_topic = self.my_account.get_topic(self.topicName)
        topic_meta = TopicMeta()
        topic_meta.topic_name = self.topicName
        topic_meta.set_maximum_message_size = -1
        topic_meta.set_logging_enabled = False
        try:
            topic_url = self.my_topic.create(topic_meta)
            sys.stdout.write("Create Queue Succeed!\nQueueURL:%s\n\n" % topic_url)
        except MNSExceptionBase as e:
            sys.stderr.write("Create Queue Fail!\nException:%s\n\n" % e)
            sys.exit(1)


        ## Create/Get Queue
        self.my_queue = self.my_account.get_queue(self.queueName)

        queue_meta = QueueMeta()
        queue_meta.set_visibilitytimeout(100)
        queue_meta.set_maximum_message_size(10240)
        queue_meta.set_message_retention_period(3600)
        queue_meta.set_delay_seconds(10)
        queue_meta.set_polling_wait_seconds(20)
        queue_meta.set_logging_enabled(True)
        try:
            queue_url = self.my_queue.create(queue_meta)
            sys.stdout.write("Create Queue Succeed!\nQueueURL:%s\n\n" % queue_url)
        except MNSExceptionBase as e:
            sys.stderr.write("Create Queue Fail!\nException:%s\n\n" % e)
            sys.exit(1)


        ## Subscribe Queue to Topic
        self.my_subscription = self.my_account.get_subscription(self.topicName, self.queueName)

        subscription_meta = SubscriptionMeta()
        subscription_meta.topic_name = self.topicName
        subscription_meta.subscription_name = self.queueName + "_sub"
        subscription_meta.endpoint = "acs:mns:eu-central-1:5502198119686404:queues/" + self.queueName
        subscription_meta.notify_strategy = "EXPONENTIAL_DECAY_RETRY"
        subscription_meta.notify_content_format = "JSON"
        try:
            subscription_url = self.my_subscription.subscribe(subscription_meta)
            sys.stdout.write("Subscription Succeed!\nQueueURL:%s\n\n" % subscription_url)
        except MNSExceptionBase as e:
            sys.stderr.write("Subscription Fail!\nException:%s\n\n" % e)
            sys.exit(1)
    
    
    def sendMessage(self):

        ## Send message to topic
        msg_body = "I am test message."
        msg_tag = "important"
        message = TopicMessage(msg_body, msg_tag)
        try:
            re_msg = self.my_topic.publish_message(message)
            sys.stdout.write("Publish Message Succeed.\nMessageBody:%s\nMessageTag:%s\nMessageId:%s\nMessageBodyMd5:%s\n\n" % (msg_body, msg_tag, re_msg.message_id, re_msg.message_body_md5))
        except MNSExceptionBase as e:
            sys.stderr.write("Publish Message Fail!\nException:%s\n\n" % e)
            sys.exit(1)

        sys.stdout.write("PASS ALL!!\n\n")


        ## Ugly hack as request parameter do not get saved further down the code
        ## Read message
        # my_queue.encoding = False
        # my_message = my_queue.receive_message()
        # print(my_message.message_body)


    def cleanMNS(self):

        ## Remove queue
        try:
            self.my_queue.delete()
            sys.stdout.write("Delete Queue Succeed!\n")
        except MNSExceptionBase as e:
            sys.stderr.write("Delete Queue Fail!\nException:%s\n\n" % e)
            sys.exit(1)


        ## Unsuscribe from Topic
        try:
            self.my_subscription.unsubscribe()
            sys.stdout.write("Unsuscription Succeed!\n")
        except MNSExceptionBase as e:
            sys.stderr.write("Unsuscription Fail!\nException:%s\n\n" % e)
            sys.exit(1)


        ## Remove Topic
        try:
            self.my_topic.delete()
            sys.stdout.write("Delete Topic Succeed!\n")
        except MNSExceptionBase as e:
            sys.stderr.write("Delete Topic Fail!\nException:%s\n\n" % e)
            sys.exit(1)
            
            
mns_client = MNSClient()
menu = {}
menu['1']="Setup MNS" 
menu['2']="Send Message to Topic"
menu['3']="Destroy MNS"
menu['4']="Exit"
while True: 
    options=menu.keys()
    options.sort()
    for entry in options: 
        print entry, menu[entry]

    selection=raw_input("Please Select:") 
    if selection =='1': 
        mns_client.setupMNS()
    elif selection == '2': 
        mns_client.sendMessage()
    elif selection == '3':
        mns_client.cleanMNS()
    elif selection == '4': 
        break
    else: 
        print "Unknown Option Selected!" 





