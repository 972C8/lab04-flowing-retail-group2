package io.flowing.retail.email.messages;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.flowing.retail.email.application.EmailService;
import io.flowing.retail.email.domain.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MessageListener {

    @Autowired
    private MessageSender messageSender;

    @Autowired
    private EmailService emailService;

    @Autowired
    private ObjectMapper objectMapper;

    @Transactional
    @KafkaListener(id = "email", topics = MessageSender.TOPIC_NAME)
    public void messageReceived(String messagePayloadJson, @Header("type") String messageType) throws Exception {
        if ("OrderPlacedEvent".equals(messageType)) {
            try {
                Message<JsonNode> message = objectMapper.readValue(messagePayloadJson, new TypeReference<Message<JsonNode>>(){});
                ObjectNode payload = (ObjectNode) message.getData();
                Customer customer = objectMapper.treeToValue(payload.get("customer"), Customer.class);
                //TODO: use message mapper if email should include personalized content
                //Message<EmailCommandPayload> message = objectMapper.readValue(messagePayloadJson, new TypeReference<Message<EmailCommandPayload>>() {});

                //Send email to recipient of order
                emailService.createEmail(customer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
