package com.redhat.rhte.cep.kafka.consumer.service;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.redhat.rhte.cep.kafka.model.CreditCardTransaction;
import com.redhat.rhte.cep.kafka.utils.TransactionPatterns;

public class CreditCardTransactionAccumulatorFilterProcessor implements Processor<String, CreditCardTransaction> {

	private String illegalTransNode = "SINK2";
	private String byCCTransCountNode = "SINK1";

	private ProcessorContext context;

	@Override
	public void process(String key, CreditCardTransaction value) {
		if (TransactionPatterns.isExceedMaxNumerofTransactions(value.getNoOfTransactions())) {
			this.context.forward(key, value, illegalTransNode);
		} else {
			this.context.forward(key, value, byCCTransCountNode);

		}

	}

	@Override
	public void init(ProcessorContext context) {
		this.context = context;

	}

	@Override
	public void punctuate(long timestamp) {

	}

	@Override
	public void close() {

	}

}
