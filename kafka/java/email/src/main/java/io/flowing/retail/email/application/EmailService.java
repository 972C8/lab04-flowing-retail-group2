package io.flowing.retail.email.application;

import io.flowing.retail.email.domain.Customer;
import org.springframework.stereotype.Component;


@Component
public class EmailService {

    public void createEmail(Customer customer) {
        System.out.println("Sending out email: Dear " + customer.getName() + ", your goods will be soon shipped to " + customer.getAddress());
    }

}
