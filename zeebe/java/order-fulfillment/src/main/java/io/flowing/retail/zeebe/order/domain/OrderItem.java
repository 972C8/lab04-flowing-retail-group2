package io.flowing.retail.zeebe.order.domain;

import java.util.UUID;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.hibernate.annotations.GenericGenerator;

@Entity
public class OrderItem {

  @Id
  @GeneratedValue(generator = "uuid2")
  @GenericGenerator(name = "uuid2", strategy = "uuid2")
  private String id;
  
  private String articleId;
  private int amount;
  
  public String getArticleId() {
    return articleId;
  }
  public OrderItem setArticleId(String articleId) {
    this.articleId = articleId;
    return this;
  }
  public int getAmount() {
    return amount;
  }
  public OrderItem setAmount(int amount) {
    this.amount = amount;
    return this;
  }
  @Override
  public String toString() {
    return "OrderItem [articleId=" + articleId + ", amount=" + amount + "]";
  }
}
