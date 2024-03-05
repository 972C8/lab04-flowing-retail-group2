package io.flowing.retail.inventory.rest;

import io.flowing.retail.inventory.application.InventoryService;
import io.flowing.retail.inventory.domain.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class InventoryController {

    @Autowired
    private InventoryService inventoryService;

    @RequestMapping(path = "/api/inventory/", method = RequestMethod.POST)
    public String changeInventory() {

        List<Item> newItems = new ArrayList<>();
        Item item1 = new Item();
        item1.setArticleId("article1");
        item1.setAmount(5);
        newItems.add(item1);
        Item item2 = new Item();
        item2.setArticleId("article2");
        item2.setAmount(20);
        newItems.add(item2);

        inventoryService.topUpInventory(newItems);

        return "Inventory filled up";
    }
}
