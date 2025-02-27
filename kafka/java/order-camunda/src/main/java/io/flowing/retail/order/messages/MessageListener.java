package io.flowing.retail.order.messages;

import java.io.IOException;
import java.util.Collections;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.spin.plugin.variable.SpinValues;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.flowing.retail.order.domain.Order;
import io.flowing.retail.order.persistence.OrderRepository;

@Component
public class MessageListener {
  
  @Autowired
  private OrderRepository repository;
  
  @Autowired
  private ProcessEngine camunda;

  @Autowired
  private ObjectMapper objectMapper;
  
  /**
   * Handles incoming OrderPlacedEvents. 
   *
   */
  @Transactional
  public void orderPlacedReceived(Message<Order> message) throws JsonParseException, JsonMappingException, IOException {
    Order order = message.getData();
    
    System.out.println("New order placed, start flow. " + order);
    
    // persist domain entity
    repository.save(order);    
    
    // and kick of a new flow instance
    camunda.getRuntimeService().createMessageCorrelation(message.getType())
      .processInstanceBusinessKey(message.getTraceid())
      .setVariable("orderId", order.getId())
      .correlateWithResult();
  }
  
  /**
   * Very generic listener for simplicity. It takes all events and checks, if a 
   * flow instance is interested. If yes, they are correlated, 
   * otherwise they are just discarded.
   *  
   * It might make more sense to handle each and every message type individually.
   */
  @Transactional
  @KafkaListener(id = "order", topics = MessageSender.TOPIC_NAME)
  public void messageReceived(String messagePayloadJson, @Header("type") String messageType) throws Exception{
    if ("OrderPlacedEvent".equals(messageType)) {
      orderPlacedReceived(objectMapper.readValue(messagePayloadJson, new TypeReference<Message<Order>>() {}));
    }
    Message<JsonNode> message = objectMapper.readValue( //
        messagePayloadJson, //
        new TypeReference<Message<JsonNode>>() {});
    
    long correlatingInstances = camunda.getRuntimeService().createExecutionQuery() //
      .messageEventSubscriptionName(message.getType()) //
      .processInstanceBusinessKey(message.getTraceid()) //
      .count();
    
    if (correlatingInstances==1) {
      System.out.println("Correlating " + message + " to waiting flow instance");
      
      camunda.getRuntimeService().createMessageCorrelation(message.getType())
        .processInstanceBusinessKey(message.getTraceid())
        .setVariable(//
            "PAYLOAD_" + message.getType(), // 
            SpinValues.jsonValue(message.getData().toString()).create())//
        .correlateWithResult();
    }
  }

}
