package com.welloculus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONArray;
import org.json.JSONObject;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


/**
 * 
 * 
 * This class implements a pipeline between Kafka and Elasticsearch using the spark-streaming.
 * It also performs some trasnformation over the data to make it compatible to elasticsearch and finally kibana which we are using for visualization.
 * 
 * 
 * @author anujkumar.garg
 * @author vinit.tiwari
 *
 */

public class SparkStreamingConsumer {

	static String topics;
	static String collection;

	public static void main(String[] args) throws InterruptedException {

		if (args.length < 2) {
			System.out.println("Please provide two arguements - kafka-topic, es-collection");
			return;
		}
		topics = args[0];
		collection = args[1];

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "127.0.0.1");
		conf.set("es.port", "9200");
		conf.set("es.net.http.auth.user", "elastic");
		conf.set("es.net.http.auth.pass", "elasticpassword");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		messages.print();

		JavaDStream<String> word = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2;
			}
		});

		/*
		 * This is the part where we are parsing the Json array and processing the objects one by one.
		 * So one Kafka message is broken into multiple elastic search messages.
		 */
		JavaDStream<String> wordParts = word.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				System.out.println("String received in flatMapFunction:  " + t);
				if (t == null) {
					return null;
				} else {
					JSONObject receivedJson = new JSONObject(t);
					List<String> records = convertToESFormat(receivedJson);
					return records.iterator();

				}
			}

			private List<String> convertToESFormat(JSONObject receivedJson) {
				List<String> records = new ArrayList<String>();
				JSONArray dataArray = receivedJson.getJSONArray("data");
				receivedJson.remove("data");
				Set<String> keyset = receivedJson.keySet();
				for (int i = 0; i < dataArray.length(); i++) {
					JSONObject record = dataArray.getJSONObject(i);
					for (String key : keyset) {
						record.put(key, receivedJson.get(key));
					}
					records.add(record.toString());
				}
				return records;
			}

		});

		wordParts.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> arg0) throws Exception {
				try {
					JavaEsSpark.saveJsonToEs(arg0, collection + "/data");
				} catch (Exception es) {
					es.printStackTrace();
				}

			}
		});
		ssc.start();
		ssc.awaitTermination();
	}
}
