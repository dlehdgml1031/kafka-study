package com.dvwy.day2.steams.dsl.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

// address KTable
// order KStream
// order -> Key 기준으로 ADDRESS와 join
public class KStreamJoinKTable {

    private static String APPLICATION_NAME = "order-join-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address";
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order-join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // topology 생성
//        KTable<String, String> address = builder.table(ADDRESS_TABLE);
        GlobalKTable<String, String> address = builder.globalTable(ADDRESS_TABLE); // 파티션 개수가 맞지 않는 경우
        KStream<String, String> order = builder.stream(ORDER_STREAM);
        
        // 파티션 개수가 맞지 않는 경우
        order.join(address,
                (orderKey, orderValue) -> orderKey,
                (orderValue, addressValue) -> orderValue + " send to " + addressValue).to(ORDER_JOIN_STREAM);

//            order.join(address, (orderValue, addressValue) -> {
//            return orderValue + " send to " + addressValue;
//        }).to(ORDER_JOIN_STREAM); // address와 join

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}