package io.github.durableflow.sample.model;

import java.math.BigDecimal;

/**
 * Represents the JSON payload consumed from the Artemis order topic.
 *
 * <p>Example JSON:
 * <pre>{@code
 * {
 *   "orderId":     "ORD-001",
 *   "customerId":  "CUST-42",
 *   "amount":      149.99,
 *   "description": "Two widgets and a sprocket"
 * }
 * }</pre>
 */
public class OrderMessage {

    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private String description;

    /** No-arg constructor required by Jackson. */
    public OrderMessage() {}

    public OrderMessage(String orderId, String customerId, BigDecimal amount, String description) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.description = description;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
