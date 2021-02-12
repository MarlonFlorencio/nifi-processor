package com.github.marlonflorencio.nifi.processor;


import com.github.marlonflorencio.nifi.model.Entrega;
import com.github.marlonflorencio.nifi.model.EntregaDto;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.github.marlonflorencio.nifi.processor.MyKafkaProcessor.REL_SUCCESS;

/**
 * This class represents a lease to access a Kafka Consumer object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable, ConsumerRebalanceListener {

    private final long maxWaitMillis;
    private final Consumer<String, Entrega> kafkaConsumer;
    private final ComponentLog logger;
    private final byte[] demarcatorBytes;
    private final String keyEncoding;
    private final String securityProtocol;
    private final String bootstrapServers;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private final Charset headerCharacterSet;
    private final Pattern headerNamePattern;
    private final boolean separateByKey;
    private boolean poisoned = false;
    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<BundleInformation, BundleTracker> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetsMap = new HashMap<>();
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int totalMessages = 0;

    ConsumerLease(
            final long maxWaitMillis,
            final Consumer<String, Entrega> kafkaConsumer,
            final byte[] demarcatorBytes,
            final String keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final boolean separateByKey) {
        this.maxWaitMillis = maxWaitMillis;
        this.kafkaConsumer = kafkaConsumer;
        this.demarcatorBytes = demarcatorBytes;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
    }

    /**
     * clears out internal state elements excluding session and consumer as
     * those are managed by the pool itself
     */
    private void resetInternalState() {
        bundleMap.clear();
        uncommittedOffsetsMap.clear();
        leaseStartNanos = -1;
        lastPollEmpty = false;
        totalMessages = 0;
    }

    /**
     * Kafka will call this method whenever it is about to rebalance the
     * consumers for the given partitions. We'll simply take this to mean that
     * we need to quickly commit what we've got and will return the consumer to
     * the pool. This method will be called during the poll() method call of
     * this class and will be called by the same thread calling poll according
     * to the Kafka API docs. After this method executes the session and kafka
     * offsets are committed and this lease is closed.
     *
     * @param partitions partitions being reassigned
     */
    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' revoked for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
        //force a commit here.  Can reuse the session and consumer after this but must commit now to avoid duplicates if kafka reassigns partition
        commit();
    }

    /**
     * This will be called by Kafka when the rebalance has completed. We don't
     * need to do anything with this information other than optionally log it as
     * by this point we've committed what we've got and moved on.
     *
     * @param partitions topic partition set being reassigned
     */
    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' assigned for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
    }

    /**
     * Executes a poll on the underlying Kafka Consumer and creates any new
     * flowfiles necessary or appends to existing ones if in demarcation mode.
     */
    void poll() {
        /**
         * Implementation note:
         * Even if ConsumeKafka is not scheduled to poll due to downstream connection back-pressure is engaged,
         * for longer than session.timeout.ms (defaults to 10 sec), Kafka consumer sends heartbeat from background thread.
         * If this situation lasts longer than max.poll.interval.ms (defaults to 5 min), Kafka consumer sends
         * Leave Group request to Group Coordinator. When ConsumeKafka processor is scheduled again, Kafka client checks
         * if this client instance is still a part of consumer group. If not, it rejoins before polling messages.
         * This behavior has been fixed via Kafka KIP-62 and available from Kafka client 0.10.1.0.
         */
        try {
            final ConsumerRecords<String, Entrega> records = kafkaConsumer.poll(Duration.ofMillis(10));
            lastPollEmpty = records.count() == 0;
            processRecords(records);
        } catch (final ProcessException pe) {
            throw pe;
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     *
     * if false then we didn't do anything and should probably yield if true
     * then we committed new data
     *
     */
    boolean commit() {
        if (uncommittedOffsetsMap.isEmpty()) {
            resetInternalState();
            return false;
        }
        try {
            /**
             * Committing the nifi session then the offsets means we have an at
             * least once guarantee here. If we reversed the order we'd have at
             * most once.
             */
            final Collection<FlowFile> bundledFlowFiles = getBundles();
            if (!bundledFlowFiles.isEmpty()) {
                getProcessSession().transfer(bundledFlowFiles, REL_SUCCESS);
            }
            getProcessSession().commit();

            final Map<TopicPartition, OffsetAndMetadata> offsetsMap = uncommittedOffsetsMap;
            kafkaConsumer.commitSync(offsetsMap);
            resetInternalState();
            return true;
        } catch (final IOException ioe) {
            poison();
            logger.error("Failed to finish writing out FlowFile bundle", ioe);
            throw new ProcessException(ioe);
        } catch (final KafkaException kex) {
            poison();
            logger.warn("Duplicates are likely as we were able to commit the process"
                    + " session but received an exception from Kafka while committing"
                    + " offsets.");
            throw kex;
        } catch (final Throwable t) {
            poison();
            throw t;
        }
    }

    /**
     * Indicates whether we should continue polling for data. If we are not
     * writing data with a demarcator then we're writing individual flow files
     * per kafka message therefore we must be very mindful of memory usage for
     * the flow file objects (not their content) being held in memory. The
     * content of kafka messages will be written to the content repository
     * immediately upon each poll call but we must still be mindful of how much
     * memory can be used in each poll call. We will indicate that we should
     * stop polling our last poll call produced no new results or if we've
     * polling and processing data longer than the specified maximum polling
     * time or if we have reached out specified max flow file limit or if a
     * rebalance has been initiated for one of the partitions we're watching;
     * otherwise true.
     *
     * @return true if should keep polling; false otherwise
     */
    boolean continuePolling() {
        //stop if the last poll produced new no data
        if (lastPollEmpty) {
            return false;
        }

        //stop if we've gone past our desired max uncommitted wait time
        if (leaseStartNanos < 0) {
            leaseStartNanos = System.nanoTime();
        }
        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        if (durationMillis > maxWaitMillis) {
            return false;
        }

        //stop if we've generated enough flowfiles that we need to be concerned about memory usage for the objects
        if (bundleMap.size() > 200) { //a magic number - the number of simultaneous bundles to track
            return false;
        } else {
            return totalMessages < 1000;//admittedlly a magic number - good candidate for processor property
        }
    }

    /**
     * Indicates that the underlying session and consumer should be immediately
     * considered invalid. Once closed the session will be rolled back and the
     * pool should destroy the underlying consumer. This is useful if due to
     * external reasons, such as the processor no longer being scheduled, this
     * lease should be terminated immediately.
     */
    private void poison() {
        poisoned = true;
    }

    /**
     * @return true if this lease has been poisoned; false otherwise
     */
    boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Trigger the consumer's {@link KafkaConsumer#wakeup() wakeup()} method.
     */
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    /**
     * Abstract method that is intended to be extended by the pool that created
     * this ConsumerLease object. It should ensure that the session given to
     * create this session is rolled back and that the underlying kafka consumer
     * is either returned to the pool for continued use or destroyed if this
     * lease has been poisoned. It can only be called once. Calling it more than
     * once can result in undefined and non threadsafe behavior.
     */
    @Override
    public void close() {
        resetInternalState();
    }

    public abstract ProcessSession getProcessSession();

    public abstract void yield();

    private void processRecords(final ConsumerRecords<String, Entrega> records) {
        records.partitions().stream().forEach(partition -> {
            List<ConsumerRecord<String, Entrega>> messages = records.records(partition);
            if (!messages.isEmpty()) {
                //update maximum offset map for this topic partition
                long maxOffset = messages.stream()
                        .mapToLong(record -> record.offset())
                        .max()
                        .getAsLong();

                messages.forEach(message -> {
                    writeData(getProcessSession(), message, partition);
                });

                totalMessages += messages.size();
                uncommittedOffsetsMap.put(partition, new OffsetAndMetadata(maxOffset + 1L));
            }
        });
    }

    private Collection<FlowFile> getBundles() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final BundleTracker tracker : bundleMap.values()) {
            final boolean includeBundle = processBundle(tracker);
            if (includeBundle) {
                flowFiles.add(tracker.flowFile);
            }
        }
        return flowFiles;
    }

    private boolean processBundle(final BundleTracker bundle) throws IOException {
        final RecordSetWriter writer = bundle.recordWriter;
        if (writer != null) {
            final WriteResult writeResult;

            try {
                writeResult = writer.finishRecordSet();
            } finally {
                writer.close();
            }

            if (writeResult.getRecordCount() == 0) {
                getProcessSession().remove(bundle.flowFile);
                return false;
            }

            final Map<String, String> attributes = new HashMap<>();
            attributes.putAll(writeResult.getAttributes());
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

            bundle.flowFile = getProcessSession().putAllAttributes(bundle.flowFile, attributes);
        }

        populateAttributes(bundle);
        return true;
    }

    private void writeData(final ProcessSession session, ConsumerRecord<String, Entrega> record, final TopicPartition topicPartition) {
        FlowFile flowFile = session.create();
        final BundleTracker tracker = new BundleTracker(record, topicPartition, keyEncoding);
        tracker.incrementRecordCount(1);

        final Entrega value = record.value();

        if (value != null) {
            flowFile = session.write(flowFile, out -> {

                //AQUI - CONVERSAO

                EntregaDto entregaDto = new EntregaDto(
                        value.getEndereco(),
                        value.getNumero(),
                        value.getCidade(),
                        value.getStatus()
                );

                String json = new Gson().toJson(entregaDto);

                out.write(json.getBytes(StandardCharsets.UTF_8));
            });
        }

        flowFile = session.putAllAttributes(flowFile, getAttributes(record));
        tracker.updateFlowFile(flowFile);
        populateAttributes(tracker);
        session.transfer(tracker.flowFile, REL_SUCCESS);
    }

    private Map<String, String> getAttributes(final ConsumerRecord<?, ?> consumerRecord) {
        final Map<String, String> attributes = new HashMap<>();
        if (headerNamePattern == null) {
            return attributes;
        }

        for (final Header header : consumerRecord.headers()) {
            final String attributeName = header.key();
            final byte[] attributeValue = header.value();
            if (headerNamePattern.matcher(attributeName).matches() && attributeValue != null) {
                attributes.put(attributeName, new String(attributeValue, headerCharacterSet));
            }
        }

        return attributes;
    }

    private void populateAttributes(final BundleTracker tracker) {
        final Map<String, String> kafkaAttrs = new HashMap<>();
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_OFFSET, String.valueOf(tracker.initialOffset));
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TIMESTAMP, String.valueOf(tracker.initialTimestamp));

        // If we have a kafka key, we will add it as an attribute only if
        // the FlowFile contains a single Record, or if the Records have been separated by Key,
        // because we then know that even though there are multiple Records, they all have the same key.
        if (tracker.key != null && (tracker.totalRecords == 1 || separateByKey)) {
            if (!keyEncoding.equalsIgnoreCase(KafkaProcessorUtils.DO_NOT_ADD_KEY_AS_ATTRIBUTE.getValue())) {
                kafkaAttrs.put(KafkaProcessorUtils.KAFKA_KEY, tracker.key);
            }
        }

        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_PARTITION, String.valueOf(tracker.partition));
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TOPIC, tracker.topic);
        if (tracker.totalRecords > 1) {
            // Add a record.count attribute to remain consistent with other record-oriented processors. If not
            // reading/writing records, then use "kafka.count" attribute.
            if (tracker.recordWriter == null) {
                kafkaAttrs.put(KafkaProcessorUtils.KAFKA_COUNT, String.valueOf(tracker.totalRecords));
            } else {
                kafkaAttrs.put("record.count", String.valueOf(tracker.totalRecords));
            }
        }
        final FlowFile newFlowFile = getProcessSession().putAllAttributes(tracker.flowFile, kafkaAttrs);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, tracker.topic);
        getProcessSession().getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }


    private static class BundleTracker {
        final long initialOffset;
        final long initialTimestamp;
        final int partition;
        final String topic;
        final String key;
        final RecordSetWriter recordWriter;
        FlowFile flowFile;
        long totalRecords = 0;

        private BundleTracker(final ConsumerRecord<String, Entrega> initialRecord, final TopicPartition topicPartition, final String keyEncoding) {
            this(initialRecord, topicPartition, keyEncoding, null);
        }

        private BundleTracker(final ConsumerRecord<String, Entrega> initialRecord, final TopicPartition topicPartition, final String keyEncoding, final RecordSetWriter recordWriter) {
            this.initialOffset = initialRecord.offset();
            this.initialTimestamp = initialRecord.timestamp();
            this.partition = topicPartition.partition();
            this.topic = topicPartition.topic();
            this.recordWriter = recordWriter;
            //this.key = encodeKafkaKey(initialRecord.key(), keyEncoding);
            this.key = initialRecord.key();
        }

        private void incrementRecordCount(final long count) {
            totalRecords += count;
        }

        private void updateFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
        }
    }

    private static class BundleInformation {
        private final TopicPartition topicPartition;
        private final RecordSchema schema;
        private final Map<String, String> attributes;
        private final byte[] messageKey;

        public BundleInformation(final TopicPartition topicPartition, final RecordSchema schema, final Map<String, String> attributes, final byte[] messageKey) {
            this.topicPartition = topicPartition;
            this.schema = schema;
            this.attributes = attributes;
            this.messageKey = messageKey;
        }

        @Override
        public int hashCode() {
            return 41 + Objects.hash(topicPartition, schema, attributes) + 37 * Arrays.hashCode(messageKey);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof BundleInformation)) {
                return false;
            }

            final BundleInformation other = (BundleInformation) obj;
            return Objects.equals(topicPartition, other.topicPartition) && Objects.equals(schema, other.schema) && Objects.equals(attributes, other.attributes)
                    && Arrays.equals(this.messageKey, other.messageKey);
        }
    }
}