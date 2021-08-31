package com.acme.kafka.connect.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.time.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import static com.acme.kafka.connect.sample.SampleSinkConnectorConfig.*;

public class SampleSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(SampleSinkTask.class);

    private SampleSinkConnectorConfig config;
    private int monitorThreadTimeout;
    private List<String> sources;
    private Map<Integer, LongAdder> stat;

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
        stat = new ConcurrentHashMap<>();
    }

    @Override
    public void stop() {
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        stat.computeIfAbsent(records.size(), k -> new LongAdder()).increment();
        log.info("Received " + records.size());
        log.info(stat.toString());
    }

}
