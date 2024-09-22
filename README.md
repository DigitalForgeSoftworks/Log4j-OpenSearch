## Log4j-OpenSearch
![GitHub License](https://img.shields.io/github/license/DigitalForgeSoftworks/Log4j-OpenSearch?label=License)
[![Maven Central](https://img.shields.io/maven-central/v/org.digitalforge.log4j/log4j-opensearch.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/org.digitalforge.log4j/log4j-opensearch)
![Java Version](https://img.shields.io/badge/Java-11%2B-green)

OpenSearch Appender for Log4j2.

## Example Configuration
#### log4j2.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Appenders>
        <OpenSearch name="OpenSearch" host="${env:OPENSEARCH_HOSTNAME}" username="${env:OPENSEARCH_USERNAME}" password="${env:OPENSEARCH_PASSWORD}" index="${env:OPENSEARCH_INDEX}">
            <JsonTemplateLayout eventTemplateUri="classpath:OpenSearchLayout.json"/>
        </OpenSearch>
    </Appenders>
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="OpenSearch" />
        </Root>
    </Loggers>
</Configuration>
```
#### OpenSearchLayout.json
```json
{
  "@timestamp": {
    "$resolver": "timestamp",
    "pattern": {
      "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
      "timeZone": "UTC",
      "locale": "en_US"
    }
  },
  "level": {
    "$resolver": "level",
    "field": "name"
  },
  "thread": {
    "$resolver": "thread",
    "field": "name"
  },
  "logger": {
    "$resolver": "logger",
    "field": "name"
  },
  "message": {
    "$resolver": "message",
    "stringified": true,
    "stackTraceEnabled": true
  },
  "context": {
    "$resolver": "mdc"
  }
}
```
> [!NOTE]
> This template assumes the `@timestamp` mapping is defined as:
> ```json
> {
>     "format" : "strict_date_optional_time||epoch_millis",
>     "type" : "date_nanos"
> }
> ```
