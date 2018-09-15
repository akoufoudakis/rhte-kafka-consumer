package com.redhat.rhte.cep.kafka.utils;

import java.util.Calendar;
import java.util.Date;

import org.apache.kafka.streams.kstream.Predicate;

import com.redhat.rhte.cep.kafka.model.CreditCardTransaction;

public class TransactionPatterns {
	public static Predicate<String, CreditCardTransaction> bannedCountries = (key,
			ccTrans) -> IsValidCountry(ccTrans.getCountryOfTransaction());
	public static Predicate<String, CreditCardTransaction> allowedCountries = (key,
			ccTrans) -> !IsValidCountry(ccTrans.getCountryOfTransaction());

	public static Predicate<String, CreditCardTransaction> InvalidHourOfDay = (key,
			ccTrans) -> IsValidTimeOfDay(ccTrans.getPurchaseDate());
	public static Predicate<String, CreditCardTransaction> ValidHourOfDay = (key,
			ccTrans) -> !IsValidTimeOfDay(ccTrans.getPurchaseDate());

	public static Predicate<String, CreditCardTransaction> isExceedMaxNumerofTransactions = (key,
			ccTrans) -> isExceedMaxNumerofTransactions(ccTrans.getNoOfTransactions());


	public static boolean IsValidTimeOfDay(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minutes = cal.get(Calendar.MINUTE);
		if (hour > 22 && minutes >= 30) {
			return false;
		}
		return true;
	}

	public static boolean IsValidCountry(String counrty) {
		return counrty.equals("Iran (Islamic Republic of)") || counrty.equals("Afghanistan");
	}

	public static boolean isExceedMaxNumerofTransactions(Integer noOfTrans) {
		System.out.println("isExceedMaxNumerofTransactions noOfTrans==" + noOfTrans);
		return noOfTrans > 15;
	}
}
