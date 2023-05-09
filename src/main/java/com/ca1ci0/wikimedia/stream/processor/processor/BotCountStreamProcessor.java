package com.ca1ci0.wikimedia.stream.processor.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class BotCountStreamProcessor {

    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    public void setup(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputStream = streamsBuilder.stream("wikimedia.recentchange");

        inputStream
                .mapValues(changeJson -> {
                    try {
                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                        if (jsonNode.get("bot")
                                .asBoolean()) {
                            return "bot";
                        }
                        return "non-bot";
                    } catch (IOException e) {
                        return "parse-error";
                    }
                })
                .groupBy((key, botOrNot) -> botOrNot)
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {
                    final Map<String, Long> kvMap = Map.of(String.valueOf(key), value);
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(BOT_COUNT_TOPIC);
    }
}
