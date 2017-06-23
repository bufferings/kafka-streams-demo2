package com.example.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

@Component
public class InventoryCounter {

  private Schema succeededSchema;

  private Schema failedSchema;

  private Schema inventorySchema;

  private Schema recoverySchema;

  @Autowired
  public InventoryCounter(@Value(value = "classpath:avro/succeeded.avsc") Resource succeededSchemaResource,
      @Value(value = "classpath:avro/failed.avsc") Resource failedSchemaResource,
      @Value(value = "classpath:avro/inventory.avsc") Resource inventorySchemaResource,
      @Value(value = "classpath:avro/recovery.avsc") Resource recoverySchemaResource) {
    try {
      try (InputStream is = succeededSchemaResource.getInputStream()) {
        this.succeededSchema = new Schema.Parser().parse(is);
      }
      try (InputStream is = failedSchemaResource.getInputStream()) {
        this.failedSchema = new Schema.Parser().parse(is);
      }
      try (InputStream is = inventorySchemaResource.getInputStream()) {
        this.inventorySchema = new Schema.Parser().parse(is);
      }
      try (InputStream is = recoverySchemaResource.getInputStream()) {
        this.recoverySchema = new Schema.Parser().parse(is);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Bean
  public KStream<String, GenericRecord> kStream(KStreamBuilder kStreamsBuilder) {
    final GenericAvroSerde serde = new GenericAvroSerde();
    serde.configure(
        Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
        false);

    StateStoreSupplier inventoryStoreSupplier = Stores.create("InventoryStore").withKeys(Serdes.String())
        .withValues(serde).persistent().build();
    kStreamsBuilder.addStateStore(inventoryStoreSupplier);

    KStream<String, GenericRecord> stream = kStreamsBuilder.stream("Sample");
    KStream<String, GenericRecord> transed = stream
        .transformValues(() -> new ValueTransformer<GenericRecord, GenericRecord>() {
          private ProcessorContext context;
          private KeyValueStore<String, GenericRecord> inventoryStore;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;
            // this.context.schedule(1000);
            inventoryStore = (KeyValueStore) context.getStateStore("InventoryStore");
          }

          @Override
          public GenericRecord transform(GenericRecord value) {
            if (value.getSchema().getName().equals("Recovery")) {
              inventoryStore.put(((Utf8) value.get("id")).toString(), create(((Utf8) value.get("id")).toString(), 10));
              GenericRecord record = new GenericData.Record(succeededSchema);
              record.put("id", "recovered");
              record.put("count", 0);
              record.put("shiiba", "mitsuyuki");
              return record;
            }

            GenericRecord inventory = inventoryStore.get(((Utf8) value.get("id")).toString());
            int inventoryCount = (Integer) inventory.get("count");
            int demand = (Integer) value.get("count");
            if (inventoryCount - demand > 0) {
              inventoryStore.put(((Utf8) value.get("id")).toString(),
                  create(((Utf8) value.get("id")).toString(), inventoryCount - demand));
              GenericRecord record = new GenericData.Record(succeededSchema);
              record.put("id", "success");
              record.put("count", demand);
              record.put("shiiba", "mitsuyuki");
              return record;
            } else {
              GenericRecord record = new GenericData.Record(failedSchema);
              record.put("id", "fail");
              record.put("count", demand);
              record.put("shiiba", "mitsuyuki");
              return record;
            }
          }

          private GenericRecord create(String id, int count) {
            GenericRecord request = new GenericData.Record(inventorySchema);
            request.put("id", id);
            request.put("count", count);
            return request;
          }

          @Override
          public GenericRecord punctuate(long timestamp) {
            return null;
          }

          @Override
          public void close() {
            inventoryStore.close();
          }
        }, "InventoryStore");
    transed.to("result-inventory");
    transed.print();

    return null;
  }

}
