package com.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class SpringStarterApp {
	
	public static void main(String args[]) {
		
		Properties config = new Properties();
		
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-starter-app");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> wordCountInput= builder.stream("word-count-input");
		
		KTable<String, Long> wordCounts = wordCountInput
			.mapValues(textLine -> textLine.toLowerCase())
			.flatMapValues(lowerCaseTextLine-> Arrays.asList(lowerCaseTextLine.split(" ")))
			.selectKey((ignoreKey, word) -> word)
			.groupByKey()
			.count("counts");
			
		
		wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");
		
		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();
		
		System.out.println(streams.toString());
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
