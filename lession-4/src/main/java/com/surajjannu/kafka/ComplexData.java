package com.surajjannu.kafka;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
public class ComplexData implements Serializable { // 1. implementing serializable is very important for custom objects
    private String stringValue;
    private boolean boolValue;
    private int intValue;
    private double doubleValue;
}
