from ensurepip import bootstrap
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer


class Server:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers='10.0.10.44:9092'
        )

    def createTopic(self, source, topic_name):
        self.client = KafkaAdminClient(bootstrap_servers='10.0.10.44:9092')
        print(topic_name)
        topic_list = []
        for topic in topic_name:
            topic_list.append(
                NewTopic(name=source.upper() + "." + topic.upper(), num_partitions=1, replication_factor=1))
        try:
            self.client.create_topics(
                new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError as err:
            print("Topic ", topic_name, " is exist")
        finally:
            self.client.close()

    def singleMessage(self, topic_name, key, message):

        self.producer.send(
            topic=topic_name,
            key=key,
            value=message
        )
        self.producer.flush()

    def batchMessage():
        return "Multi message"


if __name__ == "__main__":
    customer = Server()
    customer.createTopic('ASYWDB.SEO.CUSTOMERSSS')
