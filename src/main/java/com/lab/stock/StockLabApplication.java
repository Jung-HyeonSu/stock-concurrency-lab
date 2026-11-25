package com.lab.stock;

import com.lab.stock.config.StockProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(StockProperties.class)
public class StockLabApplication {
    public static void main(String[] args) {
        SpringApplication.run(StockLabApplication.class, args);
    }
}
