package com.noc.sample;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleKafkaProducer {

	private static Logger logger = LoggerFactory.getLogger(SampleKafkaProducer.class);
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	public static void main(String[] args) {
		
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the producer
		KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);
		
		//create a producer record
		ProducerRecord<String, String>  record = new ProducerRecord<String, String>("mysample_topic", "Hello Kafka!!");
		
		//Sending data to Kafka
		producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if( exception != null)
					logger.error(exception.getMessage());
				else
					logger.info("message published to Topic: " + metadata.topic() + " Partision: " + metadata.partition() +" offset: " + metadata.offset());
					
			}
		});
		
		producer.close();
	}

}
