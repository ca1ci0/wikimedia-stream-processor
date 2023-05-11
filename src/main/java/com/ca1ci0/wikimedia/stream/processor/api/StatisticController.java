package com.ca1ci0.wikimedia.stream.processor.api;

import com.ca1ci0.wikimedia.stream.processor.api.dto.UserTypeEventCount;
import com.ca1ci0.wikimedia.stream.processor.api.dto.WebsiteEventCount;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

import static com.ca1ci0.wikimedia.stream.processor.processor.BotCountStreamProcessor.BOT_COUNT_STORE;
import static com.ca1ci0.wikimedia.stream.processor.processor.WebsiteCountStreamProcessor.WEBSITE_COUNT_STORE;

@RequiredArgsConstructor
@RequestMapping("/stats")
@RestController
public class StatisticController {

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/user-type")
    public List<UserTypeEventCount> getCountByUserType() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyWindowStore<String, ValueAndTimestamp<Long>> counts = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(BOT_COUNT_STORE, QueryableStoreTypes.timestampedWindowStore())
        );
        List<UserTypeEventCount> countByUserType = new ArrayList<>();
        counts.all().forEachRemaining(entry -> countByUserType.add(new UserTypeEventCount(entry.key.key(), entry.value.value())));
        return countByUserType;
    }

    @GetMapping("/website")
    public List<WebsiteEventCount> getCountByWebsite() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyWindowStore<String, ValueAndTimestamp<Long>> counts = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(WEBSITE_COUNT_STORE, QueryableStoreTypes.timestampedWindowStore())
        );
        List<WebsiteEventCount> countByWebsite = new ArrayList<>();
        counts.all().forEachRemaining(entry -> countByWebsite.add(new WebsiteEventCount(entry.key.key(), entry.value.value())));
        return countByWebsite;
    }
}
