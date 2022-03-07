# kafka-twitter-project

This project was created following a Udemy course about Kafka for beginners.

Small Java project that has a Kafka Producer and Consumer.
Producer gets tweets from Twitter and inserts them in Kafka.
Consumer reads from Kafka and inserts them into an ElasticSearch.

There are 2 project folders with separate Maven "pom.xml" files.

## Producer
For the producer you will need to create a ".env" file on the root folder "kafka-twitter-producer" with the following ENV variables:

API_KEY="..."
API_SECRET="..."
TOKEN="..."
TOKEN_SECRET="..."

These should be your OAuth1 credentials from the twitter account you will be using.

## Consumer
For the producer you will need to create a ".env" file on the root folder "kafka-twitter-consumer" with the following ENV variables:

ELASTIC_HOSTNAME="..." 
ELASTIC_USERNAME="..."
ELASTIC_PASSWORD="..."

These should be your ElasticSearch server credentials. The hostname should not have "http"/"https".

The consumer also assumes that the Index "twitter" and type "tweet" has already been created manually on your ElasticSearch server.

