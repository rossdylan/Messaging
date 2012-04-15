import zmq
from zmq.devices import ProcessDevice
from threading import Thread
from time import sleep
import json

class MessagingHub(object):
    def __init__(self, hub_name, pub_port, sub_port, max_workers=10, peers=[]):
        """
        Central Messaging hub used to direct traffic and send messages from publishers to all subscribers

        :type hub_name: str
        :param hub_name: name of the hub, used in routing traffic

        :type pub_port: int
        :param pub_port: port to listen for incoming publishers on

        :type sub_port: int
        :param sub_port: port to listen for outgoing subscribers on

        :type max_workers: int
        :param max_workers: max number of threads to use to process incoming messages

        :type peers: list
        :param peers: list of peer hubs to connect to and get data from
        """
        self.hub_name = hub_name
        self.zmq_context = zmq.Context(1)
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.subscriber_sock = self.zmq_context.socket(zmq.PUB)
        self.publisher_sock = self.zmq_context.socket(zmq.DEALER)
        self.worker_sock = self.zmq_context.socket(zmq.DEALER)
        self.max_workers = max_workers
        self.worker_url = "inproc://workers"
        self.peers = peers #peers list is just a list of "tcp://hub_addr:hub_port" we then connect to this and use it to subscribe to their shits

    def worker(self):
        """
        Worker used to forward messaging coming from publishers to the subscribers
        """
        worker_sock = self.zmq_context.socket(zmq.REP)
        worker_sock.connect(self.worker_url)
        while True:
            [meta, content] = worker_sock.recv_multipart()
            split_msg = meta.split("::")
            routing = split_msg[0]
            if not ":" in routing:
                self.subscriber_sock.send_multipart([self.hub_name + "::" + meta, content])
            if not self.hub_name in routing:
                self.subscriber_sock.send_multipart([self.hub_name + ":" + meta, content])
            worker_sock.send("")

    def start(self):
        """"
        Start the messaging hub
        """
        self.worker_sock.bind(self.worker_url)
        self.subscriber_sock.bind("tcp://*:{}".format(self.sub_port))
        self.publisher_sock.bind("tcp://*:{}".format(self.pub_port))
        if len(self.peers) > 0:
            for peer in self.peers:
                processDevice = ProcessDevice(zmq.QUEUE,zmq.SUB,zmq.REQ)
                processDevice.connect_out(peer)
                processDevice.connect_in("tcp://localhost:{}".format(self.pub_port))
                processDevice.start()

        for i in range(self.max_workers):
            t = Thread(target=self.worker)
            t.start()
        zmq.device(zmq.QUEUE,self.publisher_sock,self.worker_sock)


class MessagingSubscriber(object):
    """
    Recive messages being distributed by the hub we connect to
    """
    def __init__(self,hub_addr,hub_port, subscriptions=['',]):
        """
        Create a new subscriber and subscribe it to 1 or more topics being
        broadcasted by the selected hub

        :type hub_addr: str
        :param hub_addr: address of the hub we want to connect to

        :type hub_port: int
        :param hub_port: port of the hub we want to connect to

        :type subscriptions: list
        :param subscriptions: list of topics to subscribe to
        """
        self.zmq_context = zmq.Context(1)
        self.subscription = self.zmq_context.socket(zmq.SUB)
        for sub in subscriptions:
            self.subscription.setsockopt(zmq.SUBSCRIBE, sub)
        self.hub_addr = hub_addr
        self.hub_port = hub_port

    def start(self):
        self.subscription.connect("tcp://{}:{}".format(self.hub_addr,self.hub_port))
        while True:
            [meta, content] = self.subscription.recv_multipart()
            meta = meta.split("::")
            topic = "::".join(meta[1:]) #throw out the routing information we do not need it here
            if topic in self.subscriptions:
                t = Thread(target=self.process, args=(json.loads(content),))
                t.start()

    def process(self,data):
        """
        Easily over-rideable function used to process the recieved data

        :type data: dict
        :param data: a dict containing the json object revieved from the hub
        """
        print data


class MessagingPublisher(object):
    """
    Push messages out to the messaging network
    """
    def __init__(self,hub_addr,hub_port):
        """
        Create a new MessagingPublisher and connect it to a specified hub
        :type hub_addr: str
        :param hub_addr: Address of the messaging hub

        :type hub_port: int
        :param hub_port: port of the messaging hub
        """
        self.zmq_context = zmq.Context(1)
        self.publisher = self.zmq_context.socket(zmq.REQ)
        self.hub_addr = hub_addr
        self.hub_port = hub_port
        self.publisher.connect("tcp://{}:{}".format(str(hub_addr),str(hub_port)))

    def publish(self,topic,**kwargs):
        """
        Publish a mesage to the network

        :type topic: string
        :param topic: the messages topic (helps subscribes decide if they want to recieve this message

        :type kwargs: dict
        :param kwargs: used to create a json blob which is the message to be sent
        """
        print "Sending", kwargs
        self.publisher.send_multipart([topic,json.dumps(kwargs)])
        self.publisher.recv()


if __name__ == "__main__":
    """This is for tests"""
    import sys
    if sys.argv[1] == "hub":
        hub = MessagingHub("DefaultHub", 5667,5668)
        hub.start()
    elif sys.argv[1] == "pub":
        pub = MessagingPublisher("localhost",5667)
        while True:
            pub.publish("derpy-topic",derp=1234,herp="derpy")
            sleep(2)
    elif sys.argv[1] == "sub":
        sub = MessagingSubscriber("localhost",5668, subscriptions = ['derpy-topic',])
        sub.start()
