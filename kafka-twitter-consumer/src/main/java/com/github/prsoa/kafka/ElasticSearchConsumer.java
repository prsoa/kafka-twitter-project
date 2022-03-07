package com.github.prsoa.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Hello world!
 *
 */
public class ElasticSearchConsumer 
{

    public static RestHighLevelClient createClient() {
        Dotenv dotenv = Dotenv.load();
   

        String hostname = dotenv.get("ELASTIC_HOSTNAME");
        String username = dotenv.get("ELASTIC_USERNAME");
        String password = dotenv.get("ELASTIC_PASSWORD");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder
                .HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
 
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        JSONObject tweet = new JSONObject(tweetJson);
        return tweet.getString("id_str");
    }


    public static void main( String[] args )
    {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        try{
            KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

            // poll for new data
            while(true) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
                Integer recordCount = records.count();
                logger.info("Received " + records.count() + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records) {

                    try {
                        // Extract id from Tweet
                        String id = extractIdFromTweet(record.value());

                        // Insert into ElasticSearch
                        // Improve in the future
                        // This assumes that the Index /twitter
                        // and mapping /tweet
                        // already created manually in the ElasticSearch 
                        IndexRequest indexRequest = new IndexRequest("twitter", "tweet", id)
                            .source(record.value(), XContentType.JSON);

                        bulkRequest.add(indexRequest); // add to bulk request
                    } catch(NullPointerException e) {
                        logger.warn("Skipping bad data: " + record.value());
                    }
                }

                if (recordCount > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                    logger.info("Consuming offsets ...");
                    consumer.commitSync();
                    logger.info("Offsets committed.");
                    try {
                        Thread.sleep(1000); // small delay
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // client.close();
        } catch (Exception e) {
            logger.error("Something went wrong: ", e);
        }
    }

}
