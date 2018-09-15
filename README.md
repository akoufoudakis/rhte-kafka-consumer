# Kafka Consumer

This is a kafka consumer usning spring boot and spring kafka. 

| Technology          |   Reference         |
| -------------       |:-------------:|
| RHOAR               |  <https://access.redhat.com/documentation/en-us/red_hat_openshift_application_runtimes/1/> |
| Spring Boot on OCP  |   <https://access.redhat.com/documentation/en-us/red_hat_openshift_application_runtimes/1/html/spring_boot_runtime_guide/>      |
| Spring Kafka        |  <https://docs.spring.io/spring-kafka/docs/1.3.x/reference/htmlsingle/> |
| Strimzi             | <http://strimzi.io/> |

The idea of this sample is to show both running kafka locally and on OCP
the logic is spllitted into two parts
- using DSL
```java
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
```
- using Processor API
```java
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
```
# To run it locally (Not on OCP)
1. Create the required topics on your kafka server
```sh
$ cd rhte-kafka-consumer
$ sh create-topics.sh 
```
2. There is also a shell script to delete the topics just in case they are created before
3. Set the application properties to the correct values including the numberofCreditCard you want the producer to generate
4. run the application using the spring boot mvn plugin
```sh
[rhte-kafka-consumer]$ mvn spring-boot:run
```
# To run it on OCP
