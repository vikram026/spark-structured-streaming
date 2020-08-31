package com.nisum.spark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.Objects;
@Document
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Apple implements Serializable {

    private String color;
    private Integer weight;


}
