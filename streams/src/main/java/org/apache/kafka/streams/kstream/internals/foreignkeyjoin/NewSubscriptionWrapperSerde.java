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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerde;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

public class NewSubscriptionWrapperSerde<K, V> extends WrappingNullableSerde<NewSubscriptionWrapper<K, V>, K, V> {

    public NewSubscriptionWrapperSerde(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                       final Serde<K> primaryKeySerde,
                                       final Supplier<String> mesageValueSerializationPseudoTopicSupplier,
                                       final Serde<V> messageValueSerde) {
        super(
            new NewSubscriptionWrapperSerializer<>(primaryKeySerializationPseudoTopicSupplier,
                                                primaryKeySerde == null ? null : primaryKeySerde.serializer(),
                    mesageValueSerializationPseudoTopicSupplier,
                    messageValueSerde == null ? null : messageValueSerde.serializer()),
            new NewSubscriptionWrapperDeserializer<>(primaryKeySerializationPseudoTopicSupplier,
                                                  primaryKeySerde == null ? null : primaryKeySerde.deserializer(),
                    mesageValueSerializationPseudoTopicSupplier,
                    messageValueSerde == null ? null : messageValueSerde.deserializer())
        );
    }

    private static class NewSubscriptionWrapperSerializer<K, V>
        implements Serializer<NewSubscriptionWrapper<K, V>>, WrappingNullableSerializer<NewSubscriptionWrapper<K, V>, K, V> {

        private final Supplier<String> primaryKeySerializationPseudoTopicSupplier;
        private final Serializer<V> messageValueSerializer;
        private final Supplier<String> messageValueSerializationPseudoTopicSupplier;
        private String primaryKeySerializationPseudoTopic = null;
        private String messageValueSerializationPseudoTopic = null;
        private Serializer<K> primaryKeySerializer;
        private boolean upgradeFromV0 = false;

        NewSubscriptionWrapperSerializer(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                      final Serializer<K> primaryKeySerializer,
                                         final Supplier<String> messageValueSerializationPseudoTopicSupplier,
                                         final Serializer<V> messageValueSerializer) {
            this.primaryKeySerializationPseudoTopicSupplier = primaryKeySerializationPseudoTopicSupplier;
            this.primaryKeySerializer = primaryKeySerializer;
            this.messageValueSerializationPseudoTopicSupplier = messageValueSerializationPseudoTopicSupplier;
            this.messageValueSerializer = messageValueSerializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (primaryKeySerializer == null) {
                primaryKeySerializer = (Serializer<K>) getter.keySerde().serializer();
            }
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.upgradeFromV0 = upgradeFromV0(configs);
        }

        private static boolean upgradeFromV0(final Map<String, ?> configs) {
            final Object upgradeFrom = configs.get(StreamsConfig.UPGRADE_FROM_CONFIG);
            if (upgradeFrom == null) {
                return false;
            }

            switch ((String) upgradeFrom) {
                case StreamsConfig.UPGRADE_FROM_0100:
                case StreamsConfig.UPGRADE_FROM_0101:
                case StreamsConfig.UPGRADE_FROM_0102:
                case StreamsConfig.UPGRADE_FROM_0110:
                case StreamsConfig.UPGRADE_FROM_10:
                case StreamsConfig.UPGRADE_FROM_11:
                case StreamsConfig.UPGRADE_FROM_20:
                case StreamsConfig.UPGRADE_FROM_21:
                case StreamsConfig.UPGRADE_FROM_22:
                case StreamsConfig.UPGRADE_FROM_23:
                case StreamsConfig.UPGRADE_FROM_24:
                case StreamsConfig.UPGRADE_FROM_25:
                case StreamsConfig.UPGRADE_FROM_26:
                case StreamsConfig.UPGRADE_FROM_27:
                case StreamsConfig.UPGRADE_FROM_28:
                case StreamsConfig.UPGRADE_FROM_30:
                case StreamsConfig.UPGRADE_FROM_31:
                case StreamsConfig.UPGRADE_FROM_32:
                case StreamsConfig.UPGRADE_FROM_33:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public byte[] serialize(final String ignored, final NewSubscriptionWrapper<K, V> data) {
            //{1-bit-isHashNull}{7-bits-version}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}{4-bytes-primaryPartition}

            //7-bit (0x7F) maximum for data version.
            if (Byte.compare((byte) 0x7F, data.getVersion()) < 0) {
                throw new UnsupportedVersionException("SubscriptionWrapper version is larger than maximum supported 0x7F");
            }

            final int version = data.getVersion();
            if (upgradeFromV0 || version == 0) {
                return serializeV0(data);
            } else if (version == 1) {
                return serializeV1(data);
            } else {
                throw new UnsupportedVersionException("Unsupported SubscriptionWrapper version " + data.getVersion());
            }
        }

        private byte[] serializePrimaryKey(final NewSubscriptionWrapper<K, V> data) {
            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            return  primaryKeySerializer.serialize(
                primaryKeySerializationPseudoTopic,
                data.getPrimaryKey()
            );
        }

        private byte[] serializeMessageValue(final NewSubscriptionWrapper<K, V> data) {
            if (messageValueSerializationPseudoTopic == null) {
                messageValueSerializationPseudoTopic = messageValueSerializationPseudoTopicSupplier.get();
            }

            return  messageValueSerializer.serialize(
                    messageValueSerializationPseudoTopic,
                    data.getMessageValue()
            );
        }

        private ByteBuffer serializeCommon(final NewSubscriptionWrapper<K, V> data, final byte version, final int extraLength) {
            final byte[] primaryKeySerializedData = serializePrimaryKey(data);
            final byte[] messageValueSerializedData = serializeMessageValue(data);
            final ByteBuffer buf;
            int dataLength = 2 + primaryKeySerializedData.length + messageValueSerializedData.length + extraLength;
            if (data.getHash() != null) {
                dataLength += 2 * Long.BYTES;
                buf = ByteBuffer.allocate(dataLength);
                buf.put(version);
            } else {
                //Don't store hash as it's null.
                buf = ByteBuffer.allocate(dataLength);
                buf.put((byte) (version | (byte) 0x80));
            }
            buf.put(data.getInstruction().getValue());
            final long[] elem = data.getHash();
            if (data.getHash() != null) {
                buf.putLong(elem[0]);
                buf.putLong(elem[1]);
            }
            buf.put(primaryKeySerializedData);
            buf.put(messageValueSerializedData);
            return buf;
        }

        private byte[] serializeV0(final NewSubscriptionWrapper<K, V> data) {
            return serializeCommon(data, (byte) 0, 0).array();
        }

        private byte[] serializeV1(final NewSubscriptionWrapper<K, V> data) {
            final ByteBuffer buf = serializeCommon(data, data.getVersion(), Integer.BYTES);
            buf.putInt(data.getPrimaryPartition());
            return buf.array();
        }
    }

    private static class NewSubscriptionWrapperDeserializer<K, V>
        implements Deserializer<NewSubscriptionWrapper<K, V>>, WrappingNullableDeserializer<NewSubscriptionWrapper<K, V>, K, V> {

        private final Supplier<String> primaryKeySerializationPseudoTopicSupplier;
        private final Supplier<String> mesageValueSerializationPseudoTopicSupplier;
        private String primaryKeySerializationPseudoTopic = null;

        private String messageValueSerializationPseudoTopic = null;
        private Deserializer<K> primaryKeyDeserializer;
        private Deserializer<V> messageValueDeserializer;

        NewSubscriptionWrapperDeserializer(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                        final Deserializer<K> primaryKeyDeserializer,
                                           final Supplier<String> messageValueSerializationPseudoTopicSupplier,
                                           final Deserializer<V> messageValueDeserializer) {
            this.primaryKeySerializationPseudoTopicSupplier = primaryKeySerializationPseudoTopicSupplier;
            this.primaryKeyDeserializer = primaryKeyDeserializer;
            this.mesageValueSerializationPseudoTopicSupplier = messageValueSerializationPseudoTopicSupplier;
            this.messageValueDeserializer = messageValueDeserializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (primaryKeyDeserializer == null) {
                primaryKeyDeserializer = (Deserializer<K>) getter.keySerde().deserializer();
            }
        }

        @Override
        public NewSubscriptionWrapper<K, V> deserialize(final String ignored, final byte[] data) {
            //{7-bits-version}{1-bit-isHashNull}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}{4-bytes-primaryPartition}
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte) (0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;
            final NewSubscriptionWrapper.Instruction inst = NewSubscriptionWrapper.Instruction.fromValue(buf.get());

            int lengthSum = 2; //The first 2 bytes
            final long[] hash;
            if (isHashNull) {
                hash = null;
            } else {
                hash = new long[2];
                hash[0] = buf.getLong();
                hash[1] = buf.getLong();
                lengthSum += 2 * Long.BYTES;
            }

            final int primaryKeyLength;
            if (version > 0) {
                //primaryKeyLength = data.length - lengthSum - Integer.BYTES;
            } else {
                //primaryKeyLength = data.length - lengthSum;
            }
            // This is temp fix
            primaryKeyLength = 7;
            final byte[] primaryKeyRaw = new byte[primaryKeyLength];
            buf.get(primaryKeyRaw, 0, primaryKeyLength);

            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            final K primaryKey = primaryKeyDeserializer.deserialize(
                primaryKeySerializationPseudoTopic,
                primaryKeyRaw
            );

            /********************************Test*********************************/
            final int messageValueLength;
            if (version > 0) {
                messageValueLength = data.length - lengthSum - Integer.BYTES - primaryKeyLength;
            } else {
                messageValueLength = data.length - lengthSum - primaryKeyLength;
            }
            final byte[] messageValueRaw = new byte[messageValueLength];
            buf.get(messageValueRaw, 0, messageValueLength);


            if (messageValueSerializationPseudoTopic == null) {
                messageValueSerializationPseudoTopic = mesageValueSerializationPseudoTopicSupplier.get();
            }

            final V messageValue = messageValueDeserializer.deserialize(
                    messageValueSerializationPseudoTopic,
                    messageValueRaw
            );

            /********************************Test*********************************/

            final Integer primaryPartition;
            if (version > 0) {
                primaryPartition = buf.getInt();
            } else {
                primaryPartition = null;
            }

            return new NewSubscriptionWrapper<>(hash, inst, primaryKey, version, messageValue, primaryPartition);
        }

    }

}
