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
        monitorThreadTimeout = config.getInt(MONITOR_THREAD_TIMEOUT_CONFIG);
        String sourcesStr = properties.get("sources");
        sources = Arrays.asList(sourcesStr.split(","));

        this.reporter = null;
        try {
            if (context.errantRecordReporter() == null) {
                log.info("Errant record reporter not configured.");
            }
            // may be null if DLQ not enabled
            reporter = context.errantRecordReporter();
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("AK versions prior to 2.6 do not support the errant record reporter.");
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        int max = 0;
        int errorOn = 3;
        if (records.size() != 1) {
            log.info("Received collection with multiple elements. Count is " + records.size());
        }
        for (SinkRecord sinkRecord : records) {
            if (max >= errorOn) {
                // Emulate an error from external system
                processRecord(sinkRecord, true, true);
            } else {
                processRecord(sinkRecord, false, false);
            }
            max++;
        }
    }

    private void processRecord(SinkRecord record, Boolean generateError, Boolean isPrint) {
        // Emulates communication with external system
        if (generateError) {
            log.info("Error on sending to external system the message [" + record.toString() + "]");
            reportBadRecord(record, new ConnectException("Some error on external system side"));
        } else if (isPrint) {
            log.info("Successful send to external system the message [" + record.toString() + "]");
        }
    }

    private void reportBadRecord(SinkRecord record, Throwable error) {
        if (reporter != null) {
            reporter.report(record, error);
        }
    }

}
