package com.ca1ci0.wikimedia.stream.processor.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

@Component
public class WebsiteCountStreamProcessor {

    private static final String WEBSITE_COUNT_STORE = "website-count-store";
    private static final String WEBSITE_COUNT_TOPIC = "wikimedia.stats.website";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    public void setup(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputStream = streamsBuilder.stream("wikimedia.recentchange");
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));
        inputStream
                .selectKey((k, changeJson) -> {
                    try {
                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                        return jsonNode.get("server_name")
                                .asText();
                    } catch (IOException e) {
                        return "parse-error";
                    }
                })
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as(WEBSITE_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {
                    final Map<String, Object> kvMap = Map.of(
                            "website", key.key(),
                            "count", value
                    );
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(WEBSITE_COUNT_TOPIC, Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String()
                ));
    }
}
