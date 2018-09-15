package com.redhat.rhte.cep.kafka.consumer.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import com.redhat.rhte.cep.kafka.model.CreditCardTransaction;
import com.redhat.rhte.cep.kafka.model.CreditCardTransactionAccumulator;
import com.redhat.rhte.cep.kafka.utils.CreditCardAccumaltorTransformer;
import com.redhat.rhte.cep.kafka.utils.CreditCardTransactionPartitioner;
import com.redhat.rhte.cep.kafka.utils.StreamsSerdes;
import com.redhat.rhte.cep.kafka.utils.TransactionPatterns;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KStreamsConfig {

	private static String ccTransactionsStateStoreName = "ccTransactionsStore";

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic}")
	private String kafkaTopic;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<String, Object>();

		props.put(StreamsConfig.CLIENT_ID_CONFIG, "RHTEConsumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "RHTEGroupId");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RHTEAppId");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StreamsSerdes.CreditCardTransactionSerde().getClass().getName());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				StreamsSerdes.CreditCardTransactionSerde().getClass().getName());

		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

		/*
		 * props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
		 * props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
		 * props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
		 * props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		 * props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
		 * props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 * props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		 * props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
		 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		 * "org.apache.kafka.common.serialization.StringDeserializer");
		 * props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		 * "org.apache.kafka.common.serialization.StringDeserializer");
		 * props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		 * props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
		 * TransactionTimestampExtractor.class)
		 */

		return new StreamsConfig(props);
	}

	@SuppressWarnings({"deprecation","unchecked"})
	@Bean
	public KStream<?, ?> processIncomingStream(KStreamBuilder streamBuilder) {
		int LEGAL_TRANS = 0;
        int ILLEGAL_TRANS = 1;
        
		KStream<String, CreditCardTransaction> stream = streamBuilder.stream(kafkaTopic);
		
		KStream<String, CreditCardTransaction>[] branchCountriesStream =stream.branch(TransactionPatterns.allowedCountries,TransactionPatterns.bannedCountries);
		branchCountriesStream[ILLEGAL_TRANS].to("illegal-trans");
		
		KStream<String, CreditCardTransaction>[] branchTimeOfDayStream =branchCountriesStream[LEGAL_TRANS].branch(TransactionPatterns.ValidHourOfDay,TransactionPatterns.InvalidHourOfDay);
		branchTimeOfDayStream[ILLEGAL_TRANS].to("illegal-trans");
		stream = branchCountriesStream[LEGAL_TRANS];
		

		
		CreditCardTransactionPartitioner streamPartitioner = new CreditCardTransactionPartitioner();

		//stream.through("by-cc-trans", Produced.with(Serdes.String(), StreamsSerdes.CreditCardTransactionSerde(), streamPartitioner)).to("processed");

		stream.to(Serdes.String(), StreamsSerdes.CreditCardTransactionSerde(), streamPartitioner,"by-cc-trans");

		return stream;
	}
	@SuppressWarnings({"deprecation","unchecked"})
	@Bean
	public KafkaStreams processTransactionsByCreditCardIDStream(TopologyBuilder topologyBuilder,StreamsConfig streamingConfig) {
		
		
		StateStoreSupplier<KeyValueStore<String, Integer>>  storeSupplier =Stores.create(ccTransactionsStateStoreName)
			    .withKeys(Serdes.String())
			    .withValues(Serdes.Integer())
			    .persistent()
			    .build();
		topologyBuilder.addSource(AutoOffsetReset.EARLIEST,"by-cc-trans-source", new WallclockTimestampExtractor(),Serdes.String().deserializer(),StreamsSerdes.CreditCardTransactionSerde().deserializer(),"by-cc-trans")//
		
        //
        .addProcessor("ACCUMLATORPROCESSOR", CreditCardTransactionAccumulatorProcessor::new, "by-cc-trans-source")//
        .addProcessor("ACCUMLATORFILTERPROCESSOR",CreditCardTransactionAccumulatorFilterProcessor::new , "ACCUMLATORPROCESSOR")
        // connect the state store "COUNTS" with processor "ACCUMLATORPROCESSOR"  
        .addStateStore(storeSupplier, //
                "ACCUMLATORPROCESSOR")//
        .connectProcessorAndStateStores("ACCUMLATORPROCESSOR", ccTransactionsStateStoreName)
        //
        .addSink("SINK1", "processed",Serdes.String().serializer(),StreamsSerdes.CreditCardTransactionSerde().serializer(), "ACCUMLATORFILTERPROCESSOR")
		.addSink("SINK2", "illegal-trans",Serdes.String().serializer(),StreamsSerdes.CreditCardTransactionSerde().serializer(), "ACCUMLATORFILTERPROCESSOR");
       
		KafkaStreams streaming = new KafkaStreams(topologyBuilder, streamingConfig);
		return streaming;
	}
	
	
}