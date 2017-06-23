package com.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InventoryStoreViewer {

  @Autowired
  private KStreamBuilderFactoryBean kStreamBuilder;

  @GetMapping
  public List<KeyValue<String, String>> query() {
    final ReadOnlyKeyValueStore<String, GenericRecord> store = kStreamBuilder.getKafkaStreams().store("InventoryStore",
        QueryableStoreTypes.keyValueStore());

    final List<KeyValue<String, String>> results = new ArrayList<>();
    final KeyValueIterator<String, GenericRecord> range = store.all();
    while (range.hasNext()) {
      KeyValue<String, GenericRecord> kv = range.next();
      results.add(new KeyValue<String, String>(kv.key, kv.value.toString()));
    }
    return results;
  }

}
