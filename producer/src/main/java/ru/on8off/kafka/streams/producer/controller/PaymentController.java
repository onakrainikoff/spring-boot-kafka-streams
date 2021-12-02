package ru.on8off.kafka.streams.producer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.on8off.kafka.streams.producer.service.ProducerService;

@RestController
public class PaymentController {
    @Autowired
    private ProducerService producerService;

    @GetMapping("/generate")
    public String generateNewPayment(@RequestParam(required = false) Integer count, @RequestParam Integer pause) throws InterruptedException {
        producerService.produce(count, pause);
        return "OK";
    }
}
