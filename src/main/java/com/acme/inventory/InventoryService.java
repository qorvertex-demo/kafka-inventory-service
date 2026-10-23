package com.acme.inventory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {
  private final KafkaTemplate<String, String> kafkaTemplate;
  public InventoryService(KafkaTemplate<String, String> t) { this.kafkaTemplate = t; }

  // Consumes the SAME "orders" topic the TS kafka-order-producer publishes to
  // (cross-language + cross-repo coupling), plus payments-completed.
  @KafkaListener(topics = {"orders", "payments-completed"}, groupId = "inventory")
  public void onEvent(String msg) {
    kafkaTemplate.send("inventory-reserved", msg);
  }
}
