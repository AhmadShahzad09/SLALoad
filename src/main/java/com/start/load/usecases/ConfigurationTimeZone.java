package com.start.load.usecases;

import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.TimeZone;

@Configuration
public class ConfigurationTimeZone {

        @PostConstruct
        public void init() {

            TimeZone.setDefault(TimeZone.getTimeZone("GST"));

            System.out.println("Date in UTC: " + new Date().toString());
        }
}
