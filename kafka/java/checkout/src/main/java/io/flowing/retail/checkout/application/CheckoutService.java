package io.flowing.retail.checkout.application;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.flowing.retail.checkout.domain.Order;
import io.flowing.retail.checkout.messages.Message;
import io.flowing.retail.checkout.messages.MessageSender;

@Component
public class CheckoutService {

  @Autowired
  private MessageSender messageSender;

  public String placeOrder(Order order) {
    Message<Order> message = new Message<Order>("OrderPlacedEvent", order);
    messageSender.send(message);

    return message.getTraceid();
  }

  public void retryPayment(Order order) {
    messageSender.send(
        new Message<Order>(
            "RetryPaymentCommand",
            order
        )
    );
  }

}
