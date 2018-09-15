package com.redhat.rhte.cep.kafka.model;

public class CreditCardTransactionAccumulator {

	private String creditCardId;
	private double purchaseTotal;
	private int totalNumberOfTransactions;

	private CreditCardTransactionAccumulator(String customerId, double purchaseTotal) {
		this.creditCardId = customerId;
		this.purchaseTotal = purchaseTotal;
	}

	public void addTransactions() {
		this.totalNumberOfTransactions += 1;
	}

	public String getCreditCardId() {
		return creditCardId;
	}

	public void setCreditCardId(String creditCardId) {
		this.creditCardId = creditCardId;
	}

	public double getPurchaseTotal() {
		return purchaseTotal;
	}

	public void setPurchaseTotal(double purchaseTotal) {
		this.purchaseTotal = purchaseTotal;
	}

	public int getTotalNumberOfTransactions() {
		return totalNumberOfTransactions;
	}

	public void setTotalNumberOfTransactions(int totalNumberOfTransactions) {
		this.totalNumberOfTransactions = totalNumberOfTransactions;
	}

	@Override
	public String toString() {
		return "CreditCardTransactionAccumulator{" + "creditCardId='" + creditCardId + '\'' + ", purchaseTotal="
				+ purchaseTotal + ", totalNumberOfTransactions=" + totalNumberOfTransactions + '}';
	}

	public static CreditCardTransactionAccumulator from(CreditCardTransaction creditCardTransaction) {
		return new CreditCardTransactionAccumulator(creditCardTransaction.getCreditCardId(),
				creditCardTransaction.getAmount());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof CreditCardTransactionAccumulator))
			return false;

		CreditCardTransactionAccumulator that = (CreditCardTransactionAccumulator) o;

		if (Double.compare(that.purchaseTotal, purchaseTotal) != 0)
			return false;
		if (totalNumberOfTransactions != that.totalNumberOfTransactions)
			return false;
		return creditCardId != null ? creditCardId.equals(that.creditCardId) : that.creditCardId == null;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = creditCardId != null ? creditCardId.hashCode() : 0;
		temp = Double.doubleToLongBits(purchaseTotal);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + totalNumberOfTransactions;
		return result;
	}

}
