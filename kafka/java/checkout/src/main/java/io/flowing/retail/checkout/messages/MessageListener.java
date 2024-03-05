package io.flowing.retail.checkout.messages;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.flowing.retail.checkout.application.InventoryService;
import io.flowing.retail.checkout.domain.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;

@Component
public class MessageListener {

    @Autowired
    private MessageSender messageSender;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ObjectMapper objectMapper;

    @Transactional
    @KafkaListener(id = "checkout", topics = "flowing-retail")
    public void inventoryChange(String messageJson, @Header("type") String messageType) throws Exception {
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
    }
}
