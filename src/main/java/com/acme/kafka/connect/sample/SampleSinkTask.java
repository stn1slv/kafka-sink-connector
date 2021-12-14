package com.acme.kafka.connect.sample;

import static com.acme.kafka.connect.sample.SampleSinkConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(SampleSinkTask.class);

    private SampleSinkConnectorConfig config;
    private ErrantRecordReporter reporter;
    private int monitorThreadTimeout;
    private List<String> sources;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new SampleSinkConnectorConfig(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        for (SinkRecord sinkRecord : records) {
            log.info("New message: " + sinkRecord.value().toString());
        }
    }

}
