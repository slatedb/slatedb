package com.slatedb.internal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.time.Duration;

/**
 * Custom Jackson module for SlateDB JSON serialization.
 * 
 * This module handles the specific serialization requirements for SlateDB Go bindings:
 * - Duration objects are serialized as nanoseconds (long integers)
 * - Field names are in snake_case format
 */
public class SlateDBJacksonModule extends SimpleModule {
    
    public SlateDBJacksonModule() {
        super("SlateDBModule");
        
        // Custom serializer for Duration -> nanoseconds
        addSerializer(Duration.class, new DurationSerializer());
        addDeserializer(Duration.class, new DurationDeserializer());
    }
    
    /**
     * Serializes Duration objects as nanoseconds (long integers)
     */
    private static class DurationSerializer extends JsonSerializer<Duration> {
        @Override
        public void serialize(Duration duration, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (duration == null) {
                gen.writeNull();
            } else {
                gen.writeNumber(duration.toNanos());
            }
        }
    }
    
    /**
     * Deserializes nanoseconds (long integers) back to Duration objects
     */
    private static class DurationDeserializer extends JsonDeserializer<Duration> {
        @Override
        public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.currentToken().isNumeric()) {
                long nanos = p.getLongValue();
                return Duration.ofNanos(nanos);
            } else {
                return null;
            }
        }
    }
}