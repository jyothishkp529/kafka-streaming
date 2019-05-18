package com.jkp.tech.poc.kafka.message;

import java.util.Date;

/**
 * Supplier Message record
 */
public class SupplierDo {
    private int id;
    private String name;
    private String startDate;

    public SupplierDo(int id, String name, String startDate) {
        this.id = id;
        this.name = name;
        this.startDate = startDate;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getStartDate() {
        return startDate;
    }

    @Override
    public String toString() {
        return "SupplierDo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", startDate=" + startDate +
                '}';
    }
}
