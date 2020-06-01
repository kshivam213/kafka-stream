package com.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class ColoringProblem {

	public static void main(String args[]) {

		Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> colorInput= builder.stream("favourite-colour-input");
		
		KStream<String, String> userAndColors= colorInput
				.filter((key, value) ->value.contains(","))
				.selectKey((key, value) -> value.split(",")[0].toLowerCase())
				.mapValues(value -> value.split(",")[1].toLowerCase())
				.filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));
		
		userAndColors.to("user-keys-and-colour");
		
		KTable<String, String> userAndColorsTable = builder.table("user-keys-and-colour");
		KTable<String, Long> favColours= userAndColorsTable
				.groupBy((user, colour) -> new KeyValue<>(colour, colour))
				.count("CountsByColours");
		
		
		favColours.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");
		
		KafkaStreams streams= new KafkaStreams(builder, config);
		streams.cleanUp();
		streams.start();
		
		System.out.println(streams.toString());
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
	}
}
