package com.reebelo.springbatch.entity;

import javax.persistence.*;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.io.Serializable;
import java.util.Date;

@Entity
public class Product {

    @Id
    @NotNull
    @Column (name = "product_id")
    private Long product_id;

    @NotNull
    @Column (name = "price")
    private Double price;

    @NotNull
    @Column (name = "stock")
    private Integer stock;

    @Column (name = "updated_at")
    private Date updated_at;

    public Long getProduct_id() {
        return product_id;
    }

    public void setProduct_id(Long product_id) {
        this.product_id = product_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public int getStock() {
        return stock;
    }

    public void setStock(Integer stock) {
        this.stock = stock;
    }

    @PrePersist
    protected void onCreate() {
        updated_at = new Date();
    }

    @PreUpdate
    protected void onUpdate() {
        updated_at = new Date();
    }

    @Override
    public String toString() {
        return product_id +
                "," + price +
                "," + stock;
    }
}
