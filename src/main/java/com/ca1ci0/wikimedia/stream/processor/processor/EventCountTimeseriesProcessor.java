package com.ca1ci0.wikimedia.stream.processor.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class EventCountTimeseriesProcessor {

    private static final String TIMESERIES_TOPIC = "wikimedia.stats.timeseries";
    private static final String TIMESERIES_STORE = "event-count-store";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    public void setup(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputStream = streamsBuilder.stream("wikimedia.recentchange");
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));

        inputStream
                .selectKey((key, value) -> "key-to-group")
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as(TIMESERIES_STORE))
                .toStream()
                .mapValues((readOnlyKey, value) -> {
                    final Map<String, Object> kvMap = Map.of(
                            "start_time", readOnlyKey.window()
                                    .startTime()
                                    .toString(),
                            "end_time", readOnlyKey.window()
                                    .endTime()
                                    .toString(),
                            "window_size", timeWindows.size(),
                            "event_count", value
                    );
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(TIMESERIES_TOPIC, Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String()
                ));
    }
}
