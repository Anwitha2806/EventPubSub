import threading
import queue
from collections import defaultdict

class PubSubSystem:
    def __init__(self):
        self.topics = {}  # Dictionary to store topics and their queues

        self.publishers = [] # Dictionary to store publishers and their subscriptions
        self.subscribers = []#Dictionary to store subsribers and their subscriptions
        self.subtopic = defaultdict(list)#Maintains a hash table of topics and the list of subscribers subscribed to these tooics
        self.lock = threading.Lock()

    def create_topic(self, topic):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = queue.Queue()
                print("Topic "+topic+" created.")
                print("List of available topics "+str(self.topics.keys()))


    def add_publisher(self, publisher_name):
        with self.lock:
            if publisher_name not in self.publishers:
                self.publishers.append(publisher_name)
                print("Publisher added to the network:",publisher_name)
            else:
                print("Publisher already existing in the system.")
        print("List of existing publishers:",str(self.publishers))

    def publish(self, topic, message, publisher_name):
        with self.lock:
            if topic in self.topics and publisher_name in self.publishers:
                self.topics[topic].put(f"Published by '{publisher_name}': {message}")
                print(f"Published to '{topic}' by '{publisher_name}': {message}")
            else:
                print(f"Error: Publisher '{publisher_name}' is not authorized to publish to '{topic}'.")

    def add_subscriber(self, subscriber_name):
        with self.lock:
            if subscriber_name not in self.subscribers:
                self.subscribers.append(subscriber_name)
                print("Subscriber added to the network:",subscriber_name)
            else:
                print("Subscriber already existing in the system.")
        print("List of existing subscribers:",str(self.subscribers))

    def subscribe(self, topic, subscriber_name):
        with self.lock:
            if topic in self.topics and subscriber_name in self.subscribers:
                print("Topic and subsriber is existing in the system")
                self.subtopic[topic].append(subscriber_name)
                print(f"Subscriber '{subscriber_name}' subscribed to '{topic}'")
                print("The topic "+topic+" is subscribed by these many subsribers "+str(self.subtopic[topic]))

            else:
                print(f"Error: Topic '{topic}' does not exist.")

    def unsubscribe(self, topic, subscriber_name):
        with self.lock:
            if topic in self.topics:
                self.topics[topic].subscribers.remove(subscriber_name)
                print(f"Subscriber '{subscriber_name}' unsubscribed from '{topic}'")
            else:
                print(f"Error: Topic '{topic}' does not exist.")

    def get_messages(self, topic, subscriber_name):
        with self.lock:
            messages = []
            for topic, subscribers in self.subtopic.items():
                print("My topic: ",topic)
                print("My subscriber: ",subscribers)
                if subscriber_name in subscribers:
                    while not self.topics[topic].empty():
                        print("My messages")
                        print(self.topics[topic])
                        messages.append(self.topics[topic].get())
            return messages


def main():
    pubsub_system = PubSubSystem()
    while True:
        print("Available commands:")
        print("1. create_topic <topic_name>")
        print("2. add_publisher <publisher_name>")
        print("3. publish <topic_name> <message> <publisher_name>")
        print("4. add_subscriber <subscriber_name>")
        print("5. subscribe <topic_name> <subscriber_name>")
        print("6. unsubscribe <topic_name> <subscriber_name>")
        print("7. get_messages <topic> <subscriber_name>")
        print("8. exit")
        user_input = input("Enter command: ")
        command_parts = user_input.split()

        if command_parts[0] == "create_topic":
            pubsub_system.create_topic(command_parts[1])
        elif command_parts[0] == "add_publisher":
            pubsub_system.add_publisher(command_parts[1])
        elif command_parts[0] == "add_subscriber":
            pubsub_system.add_subscriber(command_parts[1])
        elif command_parts[0] == "publish":
            pubsub_system.publish(command_parts[1], command_parts[2], command_parts[3])
        elif command_parts[0] == "subscribe":
            pubsub_system.subscribe(command_parts[1], command_parts[2])
        elif command_parts[0] == "unsubscribe":
            pubsub_system.unsubscribe(command_parts[1], command_parts[2])
        elif command_parts[0] == "get_messages":
            messages = pubsub_system.get_messages(command_parts[1],command_parts[2])
            print(f"Messages for '{command_parts[2]}':")
            for message in messages:
                print(message)
        elif command_parts[0] == "exit":
            break
        else:
            print("Invalid command. Please try again.")

if __name__ == "__main__":
    main()
