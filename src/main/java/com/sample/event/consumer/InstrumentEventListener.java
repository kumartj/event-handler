package com.sample.event.consumer;

import com.sample.event.annotation.EventListener;
import com.sample.event.annotation.EventType;

import java.util.Map;

@EventListener( topic = "${spring.kafka.templates.default-topic}")
public class InstrumentEventListener implements IEventListener {


    //can configure dependencies or downstream services

    @EventType("INSTRUMENT_CREATE")
    public  void instrumentCreate(String message, Map<String, String> header) {
        System.out.println("instrument Create  = " + message);
    }

    @EventType("INSTRUMENT_UPDATE")
    public  void instrumentUpdate(String message, Map<String, String> header) {
        System.out.println("instrument Update  = " + message);
    }
}
