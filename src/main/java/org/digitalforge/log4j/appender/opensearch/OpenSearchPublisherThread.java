package org.digitalforge.log4j.appender.opensearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.digitalforge.sneakythrow.SneakyThrow;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;

class OpenSearchPublisherThread implements Runnable {

    private final OpenSearchClient client;
    private final BlockingQueue<JsonNode> queue;
    private final String index;
    private final MeterRegistry meterRegistry;

    private final Counter counterFlush;
    private final Counter counterSuccess;
    private final Timer timerLatency;

    private volatile boolean running;

    OpenSearchPublisherThread(OpenSearchClient client, BlockingQueue<JsonNode> queue, String index, MeterRegistry meterRegistry) {

        this.client = client;
        this.queue = queue;
        this.index = index;
        this.meterRegistry = meterRegistry;

        this.counterFlush = meterRegistry.counter("log4j.opensearch.flush");
        this.counterSuccess = meterRegistry.counter("log4j.opensearch.success");
        this.timerLatency = meterRegistry.timer("log4j.opensearch.latency");

    }

    public void stop() {
        this.running = false;
        processQueue();
    }

    @Override
    public void run() {

        this.running = true;

        while(running) {

            long start = System.currentTimeMillis();

            try {

                processQueue();

                if(counterFlush != null) {
                    counterFlush.increment();
                }

            } catch (Throwable t) {

                Counter failureCounter = meterRegistry.counter("log4j.opensearch.failure", "exception", t.getClass().getSimpleName());
                failureCounter.increment();

                t.printStackTrace();

            } finally {

                long duration = System.currentTimeMillis() - start;

                timerLatency.record(duration, TimeUnit.MILLISECONDS);

            }

        }

    }

    private void processQueue() {

        List<JsonNode> entries = new ArrayList<>(5120);

        synchronized(queue) {

            if (queue.size() < 200) {
                try {
                    queue.wait(2000);
                } catch (InterruptedException e) {
                    running = false;
                }
            }

            queue.drainTo(entries, 5120);

            if(entries.isEmpty()) {
                return;
            }

        }

        publish(entries);

    }

    private void publish(List<JsonNode> entries) {

        List<BulkOperation> operations = new ArrayList<>(entries.size());

        for(JsonNode entry : entries) {
            operations.add(new BulkOperation.Builder().create(
                new CreateOperation.Builder<>().document(entry).build()
            ).build());
        }

        BulkRequest request = new BulkRequest.Builder()
            .index(index)
            .operations(operations)
            .build();

        try {

            BulkResponse bulk = client.bulk(request);

            if(bulk.errors()) {
                String errors = "";
                for(BulkResponseItem item : bulk.items()) {
                    ErrorCause error = item.error();
                    if(error != null) {
                        errors += "reason=" + error.reason()+" type=" + error.type() + " meta=" + error.metadata() + "\n";
                    }
                }
                throw new RuntimeException("Errors list on logging=" + errors.trim());
            }

            counterSuccess.increment(entries.size());

        } catch(IOException ex) {
            throw SneakyThrow.sneak(ex);
        }
    }

}