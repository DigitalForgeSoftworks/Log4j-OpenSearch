package org.digitalforge.log4j.appender.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Metrics;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.digitalforge.sneakythrow.SneakyThrow;

@Plugin(name = "OpenSearch", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class OpenSearchAppender extends AbstractAppender {

    private final OpenSearchManager manager;
    private final ObjectMapper mapper;

    public OpenSearchAppender(final String name, final Filter filter, final Layout layout, boolean ignoreExceptions, ObjectMapper mapper, OpenSearchManager manager) {
        super(name, filter, layout, ignoreExceptions);
        this.mapper = mapper;
        this.manager = manager;
    }

    @Override
    public void start() {
        super.start();
        manager.start();
    }

    @Override
    public void stop() {
        manager.stop();
        super.stop();
    }

    @Override
    public void append(LogEvent event) {

        byte[] raw = getLayout().toByteArray(event);
        JsonNode node;

        try {
            node = mapper.readTree(raw);
        } catch(Exception ex) {
            throw SneakyThrow.sneak(ex);
        }

        manager.append(node);

    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

    public static class Builder<B extends OpenSearchAppender.Builder<B>> extends AbstractAppender.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<OpenSearchAppender> {

        @PluginBuilderAttribute
        private String host;

        @PluginBuilderAttribute
        private String username;

        @PluginBuilderAttribute(sensitive = true)
        private String password;

        @PluginBuilderAttribute
        private String index;

        @PluginBuilderAttribute
        private int queueSize = 8192;

        @Override
        public OpenSearchAppender build() {

            Layout layout = getLayout();

            if(layout == null) {
                throw new IllegalArgumentException("Layout is required");
            }

            ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
            OpenSearchManager manager = new OpenSearchManager(host, username, password, index, queueSize, mapper, Metrics.globalRegistry);

            return new OpenSearchAppender(getName(), getFilter(), layout, isIgnoreExceptions(), mapper, manager);

        }

        public String getHost() {
            return host;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getIndex() {
            return index;
        }

        public int getQueueSize() {
            return queueSize;
        }

    }

}
