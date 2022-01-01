package cn.itcast.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

//https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/client/producer/KafkaProducer.html
public class KafkaProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "server102:9092, server103:9092, server104:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i=0; i<100; ++i){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", i+"", i + "message");
            Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            future.get();
            System.out.println("第 "+i+" 条消息写入成功！");
        }
        kafkaProducer.close();
    }
}



















