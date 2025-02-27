package io.flowing.retail.inventory.messages;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

import io.flowing.retail.inventory.application.InventoryService;
import io.flowing.retail.inventory.domain.Item;

@Component
public class MessageListener {
  
  @Autowired
  private MessageSender messageSender;
  
  @Autowired
  private InventoryService inventoryService;
  
  @Autowired
  private ObjectMapper objectMapper;

  @Transactional
  @KafkaListener(id = "inventory", topics = MessageSender.TOPIC_NAME)
  public void paymentReceived(String messageJson, @Header("type") String messageType) throws JsonParseException, JsonMappingException, IOException {
    if ("PaymentReceivedEvent".equals(messageType)) {
      try {
        Message<JsonNode> message = objectMapper.readValue(messageJson, new TypeReference<Message<JsonNode>>(){});

        ObjectNode payload = (ObjectNode) message.getData();
        Item[] items = objectMapper.treeToValue(payload.get("items"), Item[].class);

        String pickId = inventoryService.pickItems( //
                Arrays.asList(items), "order", payload.get("orderId").asText());

        // as in payment - we have to keep the whole order in the payload
        // as the data flows through this service

        payload.put("pickId", pickId);

        messageSender.send( //
                new Message<JsonNode>( //
                        "GoodsFetchedEvent", //
                        message.getTraceid(), //
                        payload));

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if ("OrderPlacedEvent".equals(messageType)) {
      try {
        JsonNode message = objectMapper.readTree(messageJson);
        JsonNode payload = message.get("data");
        Item[] items = objectMapper.treeToValue(payload.get("items"), Item[].class);
        String orderId = objectMapper.treeToValue(payload.get("orderId"), String.class);

        inventoryService.reserveGoods(Arrays.asList(items), "order placed", orderId, LocalDateTime.now().plusMinutes(2));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if ("PaymentFailedEvent".equals(messageType)) {
      try {
        JsonNode message = objectMapper.readTree(messageJson);
        JsonNode payload = message.get("data");

        String orderId = objectMapper.treeToValue(payload.get("orderId"), String.class);

        inventoryService.revertReservation(orderId);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

    
}
