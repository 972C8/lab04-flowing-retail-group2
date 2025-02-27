package io.flowing.retail.zeebe.order.domain;

import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.hibernate.annotations.GenericGenerator;

@Entity
public class Customer {
  
  @Id
  @GeneratedValue(generator = "uuid2")
  @GenericGenerator(name = "uuid2", strategy = "uuid2")
  private String id;
  
  private String name;
  private String address;
  
  public String getName() {
    return name;
  }
  public Customer setName(String name) {
    this.name = name;
    return this;
  }
  public String getAddress() {
    return address;
  }
  public Customer setAddress(String address) {
    this.address = address;
    return this;
  }

}
