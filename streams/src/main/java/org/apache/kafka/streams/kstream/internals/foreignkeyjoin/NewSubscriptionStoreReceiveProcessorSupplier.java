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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewSubscriptionStoreReceiveProcessorSupplier<K, V, KO, VO>
    implements ProcessorSupplier<K, NewSubscriptionWrapper<KO, VO>, CombinedKey<K, KO>, Change<ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>>> {
    private static final Logger LOG = LoggerFactory.getLogger(NewSubscriptionStoreReceiveProcessorSupplier.class);

    private final StoreBuilder<TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>>> storeBuilder;
    private final NewCombinedKeySchema<K, KO> keySchema;

    public NewSubscriptionStoreReceiveProcessorSupplier(
        final StoreBuilder<TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>>> storeBuilder,
        final NewCombinedKeySchema<K, KO> keySchema) {

        this.storeBuilder = storeBuilder;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<K, NewSubscriptionWrapper<KO, VO>, CombinedKey<K, KO>, Change<ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>>> get() {

        return new ContextualProcessor<K, NewSubscriptionWrapper<KO, VO>, CombinedKey<K, KO>, Change<ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>>>() {

            private TimestampedKeyValueStore<Bytes, NewSubscriptionWrapper<KO, VO>> store;
            private Sensor droppedRecordsSensor;

            @Override
            public void init(final ProcessorContext<CombinedKey<K, KO>, Change<ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>>>> context) {
                super.init(context);
                final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;

                droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                    Thread.currentThread().getName(),
                    internalProcessorContext.taskId().toString(),
                    internalProcessorContext.metrics()
                );
                store = internalProcessorContext.getStateStore(storeBuilder);

                keySchema.init(context);
            }

            @Override
            public void process(final Record<K, NewSubscriptionWrapper<KO, VO>> record) {
                if (record.key() == null) {
                    if (context().recordMetadata().isPresent()) {
                        final RecordMetadata recordMetadata = context().recordMetadata().get();
                        LOG.warn(
                            "Skipping record due to null foreign key. "
                                + "topic=[{}] partition=[{}] offset=[{}]",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                        );
                    } else {
                        LOG.warn(
                            "Skipping record due to null foreign key. Topic, partition, and offset not known."
                        );
                    }
                    droppedRecordsSensor.record();
                    return;
                }
                if (record.value().getVersion() > NewSubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to NewSubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older NewSubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("NewSubscriptionWrapper is of an incompatible version.");
                }

                final Bytes subscriptionKey = keySchema.toBytes(record.key(), record.value().getPrimaryKey());

                final ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>> newValue = ValueAndTimestamp.make(record.value(), record.timestamp());
                final ValueAndTimestamp<NewSubscriptionWrapper<KO, VO>> oldValue = store.get(subscriptionKey);

                //This store is used by the prefix scanner in ForeignJoinSubscriptionProcessorSupplier
                if (record.value().getInstruction().equals(NewSubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE) ||
                    record.value().getInstruction().equals(NewSubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE)) {
                    store.delete(subscriptionKey);
                } else {
                    store.put(subscriptionKey, newValue);
                }
            }
        };
    }
}