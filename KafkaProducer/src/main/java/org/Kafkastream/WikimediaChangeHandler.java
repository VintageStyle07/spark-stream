package org.Kafkastream;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.requests.FetchMetadata.log;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String>kafkaProducer,String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());
    }
    @Override
    public void onOpen()  {
        //nothing here

    }

    @Override
    public void onClosed() {
        kafkaProducer.close();

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String s)  {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading",t);

    }



}
