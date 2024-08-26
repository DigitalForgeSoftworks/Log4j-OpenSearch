package org.digitalforge.log4j.appender.opensearch;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.status.StatusLogger;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

class OpenSearchManager {

    private final BlockingQueue<JsonNode> queue;
    private final MeterRegistry meterRegistry;
    private final OpenSearchClient client;
    private final String index;

    private OpenSearchPublisherThread publisherThread;

    OpenSearchManager(String host, String username, String password, String index, int queueSize, ObjectMapper mapper, MeterRegistry meterRegistry) {

        this.client = createClient(host, username, password, mapper);
        this.index = index;
        this.queue = meterRegistry.gaugeCollectionSize("log4j.opensearch.queue.size", List.of(), new DisruptorBlockingQueue<>(queueSize));
        this.meterRegistry = meterRegistry;

    }

    public void start() {

        if(client == null) {
            StatusLogger.getLogger().warn("OpenSearch client not configured, ignoring log events");
            return;
        }

        publisherThread = new OpenSearchPublisherThread(client, queue, index, meterRegistry);

        new Thread(publisherThread).start();

    }

    public void stop() {

        if(publisherThread != null) {
            publisherThread.stop();
        }

    }

    public void append(JsonNode node) {

        if(client == null) {
            return;
        }

        queue.offer(node);

    }

    private OpenSearchClient createClient(String host, String username, String password, ObjectMapper mapper) {

        if(host == null || host.isBlank()) {
            return null;
        }

        if(username == null || username.isBlank()) {
            return null;
        }

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClient restClient = RestClient.builder(new HttpHost(host, 443, "https"))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
            .build();
        JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(mapper);
        OpenSearchTransport transport = new RestClientTransport(restClient, jsonpMapper);

        return new OpenSearchClient(transport);

    }

}
