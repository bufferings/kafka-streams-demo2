package com.example.demo;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class SampleProducer {

  private static final Logger logger = LoggerFactory.getLogger(SampleProducer.class);

  private Schema sampleSchema;

  private Schema recoverySchema;

  private KafkaTemplate<String, GenericRecord> template;

  @Autowired
  public SampleProducer(KafkaTemplate<String, GenericRecord> template,
      @Value(value = "classpath:avro/sample.avsc") Resource sampleSchemaResource,
      @Value(value = "classpath:avro/recovery.avsc") Resource recoverySchemaResource) {
    this.template = template;
    try {
      try (InputStream is = sampleSchemaResource.getInputStream()) {
        this.sampleSchema = new Schema.Parser().parse(is);
      }
      try (InputStream is = recoverySchemaResource.getInputStream()) {
        this.recoverySchema = new Schema.Parser().parse(is);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Scheduled(fixedRate = 1000)
  public void request() {
    sendRequest("A", 3);
    sendRequest("B", 5);
    sendRequest("A", 4);
    sendRequest("A", 5);
  }

  private void sendRequest(String id, int count) {
    GenericRecord request = new GenericData.Record(sampleSchema);
    request.put("id", id);
    request.put("count", count);
    request.put("shiiba", "mitsuyuki");
    template.send("Sample", id, request);
  }

  @Scheduled(fixedRate = 5000)
  public void reportCurrentTime() {
    sendRecoveryRequest("A");
    sendRecoveryRequest("B");
  }

  private void sendRecoveryRequest(String id) {
    GenericRecord request = new GenericData.Record(recoverySchema);
    request.put("id", id);
    request.put("count", 0);
    request.put("shiiba", "mitsuyuki");
    template.send("Sample", id, request);
  }

}
