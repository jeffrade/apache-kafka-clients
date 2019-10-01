import sys
import time
from kafka import KafkaConsumer

# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html

def main(filename, args):
    print('Starting KafkaConsumer...')
    client = KafkaConsumerClient(args[0])
    client.start()

class KafkaConsumerClient():

    __file_location = None

    def __init__(self, file_location):
        self.__file_location = file_location

    def start(self):
        print('in start')
        output_file = open(self.__file_location, 'a')
        consumer = KafkaConsumer("test-topic", bootstrap_servers='localhost:9092')
        for message in consumer:
            print(message)
            output_file.write("%s\n" % message.value)
            output_file.flush() 

if __name__ == '__main__':
    main(__file__, sys.argv[1:])
