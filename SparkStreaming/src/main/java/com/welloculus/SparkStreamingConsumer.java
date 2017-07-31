package com.welloculus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkStreamingConsumer {

	public static void main(String[] args) throws InterruptedException {

		String topics = "test"; //Topic name of Kafka

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "127.0.0.1");
		conf.set("es.port", "9200");
		conf.set("es.net.http.auth.user","elastic");
		conf.set("es.net.http.auth.pass","elasticpassword");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

		Set<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap();
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		messages.print();

		JavaDStream<String> word = messages.map(new Function<Tuple2<String, String>, String>() {

			public String call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2;
			}
		});

		word.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> arg0) throws Exception {
				try {
					JavaEsSpark.saveJsonToEs(arg0, "welloculus/data");
				} catch (Exception es) {
					es.printStackTrace();
				}

			}
		});
		ssc.start();
		ssc.awaitTermination();
	}
}
