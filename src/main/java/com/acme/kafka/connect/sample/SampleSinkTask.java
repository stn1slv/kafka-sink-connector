package com.acme.kafka.connect.sample;

import static com.acme.kafka.connect.sample.SampleSinkConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(SampleSinkTask.class);

    private SampleSinkConnectorConfig config;
    private int monitorThreadTimeout;
    private List<String> sources;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new SampleSinkConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(MONITOR_THREAD_TIMEOUT_CONFIG);
        String sourcesStr = properties.get("sources");
        sources = Arrays.asList(sourcesStr.split(","));
    }

    @Override
    public void stop() {
    }

    @Override
    public void put(Collection<SinkRecord> records){
        // log.info("Received "+records.size());
        Boolean isPrint=false;
        int max=0;
        int errorOn=3;
        if (records.size()!=1){
            log.info("Received collection with not a single element. Count is "+records.size());
            isPrint=true;

        } 
        for (SinkRecord sinkRecord : records) {
            if (max>=errorOn) {
                processRecord(sinkRecord, true, true);
            } else {
                processRecord(sinkRecord, false, isPrint);
            }
            max++;
        }
    }

    private void processRecord(SinkRecord record, Boolean generateError, Boolean isPrint){
        //Emulates communication with external system
        if (generateError){
            log.info("Error on sending to external system the message ["+record.value()+"]");
            // throw new RetriableException("Some error on external system side");
            throw new RuntimeException("Error on external system side");
        } else if (isPrint) {
            log.info("Successful send to external system the message ["+record.value()+"]");
        }
    }

}
