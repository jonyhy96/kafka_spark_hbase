package cn.jony.spark;

import cn.jony.spark.model.Location;
import cn.jony.spark.service.HbaseInsert;
import cn.jony.spark.service.HbasePutConvertFunction;
import cn.jony.spark.service.HbaseSaveFunction;
import cn.jony.spark.service.HbaseSearch;
import cn.jony.spark.service.Impl.HbaseInsertImpl;
import cn.jony.spark.service.Impl.HbaseSearchImpl;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * @author haoyun
 */
public final class KafkaSparkHbase {
    private final  static HbaseSearch hbaseSearch = new HbaseSearchImpl();
    private final  static HbaseInsert hbaseInsert = new HbaseInsertImpl();

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }
        String brokers = args[0];
        String topics = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "test");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
        JavaDStream<Location> locationJavaDStream = messages.map((Function<ConsumerRecord<String, String>, Location>) stringStringConsumerRecord -> {
            Gson gson = new Gson();
            return gson.fromJson(stringStringConsumerRecord.value(),Location.class);
        });
        JavaPairDStream<ImmutableBytesWritable, Put> locationPairStream =  locationJavaDStream.mapToPair(new HbasePutConvertFunction());
        locationPairStream.foreachRDD(new HbaseSaveFunction());
        jssc.start();
        jssc.awaitTermination();
    }
}
