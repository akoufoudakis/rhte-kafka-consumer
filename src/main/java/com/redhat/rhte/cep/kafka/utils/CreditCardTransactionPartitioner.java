package com.redhat.rhte.cep.kafka.utils;

import org.apache.kafka.streams.processor.StreamPartitioner;

import com.redhat.rhte.cep.kafka.model.CreditCardTransaction;

public class CreditCardTransactionPartitioner implements StreamPartitioner<String, CreditCardTransaction> {

	@Override
	public Integer partition(String key, CreditCardTransaction value, int numPartitions) {
		return Math.abs(value.getCreditCardId().hashCode()) % numPartitions;

	}
}