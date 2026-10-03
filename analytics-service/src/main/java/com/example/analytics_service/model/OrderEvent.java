package com.example.analytics_service.model;

import lombok.Data;

@Data
public class OrderEvent {
    private String id;
    private String product;
    private double price;
}
