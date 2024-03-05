package io.flowing.retail.payment;

public class PaymentFailed extends Exception {

    public PaymentFailed(String message) {
        super(message);
    }
}