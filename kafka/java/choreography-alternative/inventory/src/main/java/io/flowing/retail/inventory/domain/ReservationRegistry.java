package io.flowing.retail.inventory.domain;

import java.util.ArrayList;
import java.util.List;

public class ReservationRegistry {

    private final List<Reservation> registry;

    private static final ReservationRegistry instance = new ReservationRegistry();

    private ReservationRegistry() { this.registry = new ArrayList<>(); }

    public static ReservationRegistry getInstance() { return instance; }

    public void addReservation(Reservation reservation) {
        registry.add(reservation);
    }

    public List<Item> removeReservation(String orderId) throws Exception{
        for (Reservation r: registry) {
            if (r.refId.equals(orderId)) {
                return r.items;
            }
        }
        throw new Exception("No Reservation found");
    }
}
