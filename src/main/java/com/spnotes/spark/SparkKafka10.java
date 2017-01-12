package com.spnotes.spark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Created by sunilpatil on 1/11/17.
 */
public class SparkKafka10 {
    public static void main(String[] argv) throws Exception{

        // Configure Spark to connect to Kafka running on local machine
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        //Configure Spark to listen messages in topic test
        Collection<String> topics = Arrays.asList("test");

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafka10WordCount");

        //Read messages in batch of 30 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });

        // Break every message into words and return list of words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // Take every word and return Tuple with (word,1)
        JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word,1);
            }
        });

        // Count occurance of each word
        JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer first, Integer second) throws Exception {
                return first+second;
            }
        });

        //Print the word count
        wordCount.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
