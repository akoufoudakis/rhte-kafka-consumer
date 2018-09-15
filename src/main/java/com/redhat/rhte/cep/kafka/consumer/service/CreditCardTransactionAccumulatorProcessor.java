package com.redhat.rhte.cep.kafka.consumer.service;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.redhat.rhte.cep.kafka.model.CreditCardTransaction;

public class CreditCardTransactionAccumulatorProcessor implements Processor<String, CreditCardTransaction> {
	private static String ccTransactionsStateStoreName = "ccTransactionsStore";
    private ProcessorContext context;
    private KeyValueStore<String, Integer> kvStore;
    

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
      /*  long interval = 24 * 60 * 60 * 1000;
        this.context.schedule(interval, PunctuationType.STREAM_TIME, (long timestamp)->{
            this.punctuate(timestamp);
        });*/
        
        this.kvStore =(KeyValueStore<String, Integer>) context.getStateStore(ccTransactionsStateStoreName);

    }

   

    @Override
    public void punctuate(long timestamp) {
       
    }

    @Override
    public void close() {
        this.kvStore.close();
    }

	@Override
	public void process(String key, CreditCardTransaction value) {
				
		Integer accumulatedSoFar = kvStore.get(value.getCreditCardId());
        if (accumulatedSoFar != null) {
        	accumulatedSoFar=accumulatedSoFar+1;
        	
        }
        else
        {
        	accumulatedSoFar = 1;
        }
        value.setNoOfTransactions(accumulatedSoFar);
    	kvStore.put(value.getCreditCardId(), accumulatedSoFar);
    	context.forward(key, value);

	}


}
