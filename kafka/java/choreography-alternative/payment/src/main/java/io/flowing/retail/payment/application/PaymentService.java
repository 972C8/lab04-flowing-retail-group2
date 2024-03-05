package io.flowing.retail.payment.application;

import java.util.UUID;

import org.springframework.stereotype.Component;

import io.flowing.retail.payment.PaymentFailed;


@Component
public class PaymentService {

  public String createPayment(String orderId, long amount) throws PaymentFailed {

    // in 50% of the cases the payment will fail
    if (Math.random() < 0.5) {
      System.out.println("Payment failed for " + orderId + " with amount "+amount);
      throw new PaymentFailed("Payment failed for " + orderId + " with amount "+amount);
    }

    System.out.println("Create Payment for " + orderId + " with amount "+amount);    
    return UUID.randomUUID().toString();
  }

}
