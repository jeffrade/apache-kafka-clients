import sys
import time
from kafka import KafkaProducer

# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

def main(filename, args):
    print('Starting KafkaProducer...')
    client = KafkaProducerClient(args[0])
    client.start()

class KafkaProducerClient():

    __file_location = None

    def __init__(self, file_location):
        self.__file_location = file_location

    def start(self):
        print('in start')
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        #FIXME Any new lines written to file during for loop aren't in file object.
        with open(self.__file_location) as file:
            for line in file:
                print(line.rstrip('\n'))
                producer.send('test-topic', b"{0}".format(line.rstrip('\n')))
                time.sleep(1)

if __name__ == '__main__':
    main(__file__, sys.argv[1:])
