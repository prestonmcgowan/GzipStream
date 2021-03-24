package io.confluent.ps.convert;

import static com.gist.github.yfnick.Gzip.compress;
import static com.gist.github.yfnick.Gzip.decompress;
import static com.gist.github.yfnick.Gzip.isGZipped;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import org.apache.kafka.common.serialization.Serdes;
import static org.apache.kafka.common.serialization.Serdes.ByteArray;
import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.String;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GzipStream {
  private final static Logger log = LoggerFactory.getLogger(GzipStream.class);

  protected Properties buildStreamsProperties(Properties envProps) {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
    props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
    props.put(SaslConfigs.SASL_MECHANISM, envProps.getProperty("sasl.mechanism"));
    props.put(SaslConfigs.SASL_JAAS_CONFIG, envProps.getProperty("sasl.jaas.config"));

    log.debug("SASL Config------");
    log.debug("bootstrap.servers="+envProps.getProperty("bootstrap.servers"));
    log.debug("security.protocol="+envProps.getProperty("security.protocol"));
    log.debug("sasl.mechanism="+envProps.getProperty("sasl.mechanism"));
    log.debug("sasl.jaas.config="+envProps.getProperty("sasl.jaas.config"));
    log.debug("-----------------");

    props.put("error.topic.name", envProps.getProperty("error.topic.name"));
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDeadLetterQueueDeserialicationExceptionHandler.class.getName());
    props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDeadLetterQueueProductionExceptionHandler.class.getName());

    // Broken negative timestamp
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

    return props;
  }

  protected Topology buildTopology(Properties envProps) {
    log.debug("Starting buildTopology");
    final String gzipMode = envProps.getProperty("gzip.mode");
    final String inputTopicName = envProps.getProperty("input.topic.name");
    final String outputTopicName = envProps.getProperty("output.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    if ("gzip".equals(gzipMode.toLowerCase())) {
        log.debug("Running in gzip mode");
        // topic contains text data
        final KStream<Integer, String> textStringStream =
            builder.stream(inputTopicName, Consumed.with(Serdes.Integer(), Serdes.String()));

        // compress gzip data into a string
        textStringStream.mapValues( data -> {
            log.debug("Set to encode");
            Bytes output = null;
            log.debug("Encoding: " + data);
            try {
                output = new Bytes(compress(data));
            } catch (IOException e) {
                // TODO: Poison Pill Time
                log.info("Encode error!");
                e.printStackTrace();
            };

            return output;
        } ).to(outputTopicName, Produced.valueSerde(Serdes.Bytes()));

    } else if ("gunzip".equals(gzipMode.toLowerCase())) {
        log.debug("Running in gunzip mode");
        // topic contains byte data
        final KStream<Integer, Bytes> gzippedStringStream =
            builder.stream(inputTopicName, Consumed.with(Serdes.Integer(), Serdes.Bytes()));

        // decompress gzip data into a string
        gzippedStringStream.mapValues( gzipData -> {
            String output = "";
            log.debug("Decoded data!");
            if (isGZipped(gzipData.get())) {
              try {
                  output = decompress(gzipData.get());
              } catch (IOException e) {
                  // TODO: Poison Pill Time
                  log.info("Decode error!");
                  e.printStackTrace();
              };
              log.debug("Decoded: " + output);
            } else {
              // Data is already decoded
              output = gzipData.toString();
              log.debug("Data is already decoded: " + output);
            }
            return output;
        }).to(outputTopicName, Produced.valueSerde(Serdes.String()));
    } else {
        log.warn("Unknown gzip mode: [" + gzipMode.toLowerCase() + "]");
    }

    return builder.build();
  }

  protected Properties loadEnvProperties(String fileName) throws IOException {
    Properties envProps = new Properties();
    FileInputStream input = new FileInputStream(fileName);
    envProps.load(input);
    input.close();
    return envProps;
  }

  private void run(String configPath) throws IOException {

    Properties envProps = this.loadEnvProperties(configPath);
    Properties streamProps = this.buildStreamsProperties(envProps);

    Topology topology = this.buildTopology(envProps);
    // this.createTopics(envProps);

    final KafkaStreams streams = new KafkaStreams(topology, streamProps);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close(Duration.ofSeconds(5));
        latch.countDown();
      }
    });

    try {
      streams.cleanUp();
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
    }

    new GzipStream().run(args[0]);
  }
}