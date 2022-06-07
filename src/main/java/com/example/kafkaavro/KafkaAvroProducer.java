package com.example.kafkaavro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

@Log
public class KafkaAvroProducer {

    public static Schema getAvroSchemaFromClass(Class<?> clazz) throws ClassNotFoundException, JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(clazz, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        return schemaWrapper.getAvroSchema();
    }

    public static Schema getAvroSchemaFromClassName(String className) throws ClassNotFoundException, JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        Class classTemp = Class.forName(className);
        mapper.acceptJsonFormatVisitor(classTemp, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        return schemaWrapper.getAvroSchema();
    }

    public static GenericData.Record convertObjectToAvro(Object payload) throws ClassNotFoundException, JsonProcessingException {
        byte[] json = new ObjectMapper().writeValueAsBytes(payload);
        JsonAvroConverter converter = new JsonAvroConverter();
        return converter.convertToGenericDataRecord(json, getAvroSchemaFromClass(payload.getClass()));
    }

    public static GenericData.Record convertJsonToAvro(String jsonPayload, String className) throws ClassNotFoundException, JsonProcessingException {
        byte[] json = jsonPayload.getBytes();
        JsonAvroConverter converter = new JsonAvroConverter();
        return converter.convertToGenericDataRecord(json, getAvroSchemaFromClassName(className));
    }

    public static String convertToJson(GenericRecord genericRecord) {
        JsonAvroConverter converter = new JsonAvroConverter();
        return new String(converter.convertToJson(genericRecord));
    }

    @SneakyThrows
    public static void main(String[] args) {
        Schema schemaFromClass = getAvroSchemaFromClass(Account.class);
        log.info("Avro Schema from class: " + schemaFromClass.toString(true));

        Schema schemaFromClassName = getAvroSchemaFromClassName("com.example.kafkaavro.Account");
        log.info("Avro Schema from class name: " + schemaFromClassName.toString(true));
        
        Account account = new Account("id1", "type1");
        Record objectToAvroPayload = convertObjectToAvro(account);
        log.info("Object to Avro payload: " + objectToAvroPayload);
        log.info("Schema from Avro payload: " + objectToAvroPayload.getSchema());

        String jsonObject = "{\"id\":\"id1\", \"type\":\"type1\"}";
        Record jsonToAvroPayload = convertJsonToAvro(jsonObject,"com.example.kafkaavro.Account");
        log.info("Json to Avro payload: " + jsonToAvroPayload);
        log.info("Schema from Avro payload: " + jsonToAvroPayload.getSchema());
    }
}
