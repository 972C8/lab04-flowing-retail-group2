package io.flowing.retail.checkout.messages;

import java.io.IOException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.flowing.retail.checkout.application.CheckoutService;
import io.flowing.retail.checkout.domain.Order;

@Component
public class MessageListener {
  
  @Autowired
  private MessageSender messageSender;
  
  @Autowired
  private CheckoutService checkoutService;
  
  @Autowired
  private ObjectMapper objectMapper;

  @Transactional
  @KafkaListener(id = "inventory", topics = "flowing-retail")
  public void paymentFailed(String messageJson, @Header("type") String messageType) throws JsonParseException, JsonMappingException, IOException {
    if ("PaymentFailedEvent".equals(messageType)) {
      Message<JsonNode> message = objectMapper.readValue(messageJson, new TypeReference<Message<JsonNode>>(){});

      Order order = objectMapper.treeToValue(message.getData(), Order.class);

      checkoutService.retryPayment(order);
    }
  }

    
}
