package io.flowing.retail.checkout.messages;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowing.retail.checkout.application.InventoryService;
import io.flowing.retail.checkout.domain.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.flowing.retail.checkout.application.CheckoutService;
import io.flowing.retail.checkout.domain.Order;

@Component
public class MessageListener {
  
  @Autowired
  private InventoryService inventoryService;
  
  @Autowired
  private CheckoutService checkoutService;
  
  @Autowired
  private ObjectMapper objectMapper;

  @Transactional
  @KafkaListener(id = "checkout", topics = "flowing-retail")
  public void paymentFailed(String messageJson, @Header("type") String messageType) throws JsonParseException, JsonMappingException, IOException {
    if ("InventoryIncreaseEvent".equals(messageType)) {
      try {
          JsonNode message = objectMapper.readTree(messageJson);
          JsonNode dataNode = message.get("data");
          Item[] items = objectMapper.treeToValue(dataNode, Item[].class);

          inventoryService.increaseInventory(Arrays.asList(items));
      } catch (Exception e) {
          e.printStackTrace();
      }
    }

    if ("InventoryDecreaseEvent".equals(messageType)) {
        try {
            JsonNode message = objectMapper.readTree(messageJson);
            JsonNode dataNode = message.get("data");
            Item[] items = objectMapper.treeToValue(dataNode, Item[].class);

            inventoryService.decreaseInventory(Arrays.asList(items));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    if ("PaymentFailedEvent".equals(messageType)) {
      Message<JsonNode> message = objectMapper.readValue(messageJson, new TypeReference<Message<JsonNode>>(){});

      Order order = objectMapper.treeToValue(message.getData(), Order.class);

      checkoutService.cancelOrder(order, message.getTraceid());
    }
    if ("OrderCompletedEvent".equals(messageType)) {
      Message<JsonNode> message = objectMapper.readValue(messageJson, new TypeReference<Message<JsonNode>>(){});

      Order order = objectMapper.treeToValue(message.getData(), Order.class);

      checkoutService.completeOrder(order);
    }
  }
}
