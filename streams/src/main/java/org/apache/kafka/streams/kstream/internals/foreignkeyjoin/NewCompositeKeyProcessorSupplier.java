/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class NewCompositeKeyProcessorSupplier<K, V, KO, VO, VR> implements
    ProcessorSupplier<K, V, K, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(NewCompositeKeyProcessorSupplier.class);
    private final StoreBuilder<TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>>> storeBuilder;
    private final ValueJoiner<V, VO, VR> joiner;

    private final NewCombinedKeySchema<K, KO> keySchema;

    public NewCompositeKeyProcessorSupplier(
        final StoreBuilder<TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>>> storeBuilder,
        final NewCombinedKeySchema<K, KO> keySchema,
        ValueJoiner<V, VO, VR> joiner) {
        this.storeBuilder = storeBuilder;
        this.joiner = joiner;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<K, V, K, VR> get() {
        return new KStreamKTableJoinProcessor();
    }


    private final class KStreamKTableJoinProcessor extends ContextualProcessor<K, V, K, VR> {
        private Sensor droppedRecordsSensor;
        private TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>> store;

        @Override
        public void init(final ProcessorContext<K, VR> context) {
            super.init(context);
            final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                Thread.currentThread().getName(),
                internalProcessorContext.taskId().toString(),
                internalProcessorContext.metrics()
            );
            store = internalProcessorContext.getStateStore(storeBuilder);
        }

        @Override
        public void process(final Record<K, V> record) {
            // if the key is null, we do not need to proceed aggregating
            // the record with the table
            if (record.key() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            final Bytes prefixBytes = keySchema.prefixBytes(record.key());

            //Perform the prefixScan and propagate the results
            try (final KeyValueIterator<Bytes, ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>> prefixScanResults =
                     store.range(prefixBytes, Bytes.increment(prefixBytes))) {

                while (prefixScanResults.hasNext()) {
                    final KeyValue<Bytes, ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>> next = prefixScanResults.next();
                    // have to check the prefix because the range end is inclusive :(
                    next.value.value().getMessageValue();
                    if (prefixEquals(next.key.get(), prefixBytes.get())) {
                        final CombinedKey<KO, K> combinedKey = keySchema.fromBytes(next.key);

                        final VR result = joiner.apply(record.value(), next.value.value().getMessageValue());
                        Record<K,VR> sendRecord = record.withValue(result);

                        context().forward(sendRecord);

                    }
                }
            }
        }

        private boolean prefixEquals(final byte[] x, final byte[] y) {
            final int min = Math.min(x.length, y.length);
            final ByteBuffer xSlice = ByteBuffer.wrap(x, 0, min);
            final ByteBuffer ySlice = ByteBuffer.wrap(y, 0, min);
            return xSlice.equals(ySlice);
        }
    }
}
