/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.encode;

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public final class ValueEncoder {

    private static final ValueEncoder PLAIN_ENCODER = new ValueEncoder(KvValueLayout.PLAIN, null);

    private final KvValueLayout kvValueLayout;

    /**
     * Generates tags for {@link #encodeValue(BinaryValue)}. {@code null} for plain values or when
     * callers supply each tag to {@link #encodeValue(BinaryValue, long)}.
     */
    @Nullable private final ToLongFunction<BinaryRow> valueTagProvider;

    private ValueEncoder(
            KvValueLayout kvValueLayout, @Nullable ToLongFunction<BinaryRow> valueTagProvider) {
        this.kvValueLayout = kvValueLayout;
        this.valueTagProvider = valueTagProvider;
    }

    /**
     * Returns an encoder for the given layout. Tagged values must supply their tag to {@link
     * #encodeValue(BinaryValue, long)}.
     */
    public static ValueEncoder forLayout(KvValueLayout kvValueLayout) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        return kvValueLayout == KvValueLayout.PLAIN
                ? PLAIN_ENCODER
                : new ValueEncoder(kvValueLayout, null);
    }

    /** Returns an encoder for a layout with an internal value tag. */
    public static ValueEncoder forLayout(
            KvValueLayout kvValueLayout, ToLongFunction<BinaryRow> valueTagProvider) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        checkNotNull(valueTagProvider, "valueTagProvider must not be null.");
        if (!kvValueLayout.hasValueTag()) {
            throw new IllegalArgumentException(
                    "A value tag provider is not supported for this KV value layout.");
        }
        return new ValueEncoder(kvValueLayout, valueTagProvider);
    }

    /** Returns whether this encoder writes an internal value tag before the row bytes. */
    public boolean hasValueTag() {
        return kvValueLayout.hasValueTag();
    }

    /**
     * Encodes a binary value using the layout bound to this encoder.
     *
     * <p>For tagged layouts, this method requires a value tag provider supplied when creating the
     * encoder. Callers supplying tags explicitly must use {@link #encodeValue(BinaryValue, long)}.
     *
     * @throws IllegalStateException if the layout is tagged and no value tag provider was supplied
     */
    public byte[] encodeValue(BinaryValue value) {
        if (kvValueLayout.hasValueTag()) {
            checkState(
                    valueTagProvider != null,
                    "An explicit value tag is required for this KV value encoder.");
            return encodeValue(value, valueTagProvider.applyAsLong(value.row));
        }
        return encodeValueBody(value);
    }

    /** Encodes a binary value with the supplied opaque value tag. */
    public byte[] encodeValue(BinaryValue value, long valueTag) {
        checkState(
                kvValueLayout.hasValueTag(),
                "An explicit value tag is not supported for this KV value layout.");
        byte[] values = encodeValueBody(value);
        kvValueLayout.writeValueTag(values, valueTag);
        return values;
    }

    private byte[] encodeValueBody(BinaryValue value) {
        int rowPayloadOffset = kvValueLayout.rowPayloadOffset();
        byte[] values = new byte[rowPayloadOffset + value.row.getSizeInBytes()];
        kvValueLayout.writeSchemaId(values, value.schemaId);
        value.row.copyTo(values, rowPayloadOffset);
        return values;
    }

    /** Encodes a plain binary value for callers that do not select a layout explicitly. */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        return PLAIN_ENCODER.encodeValue(new BinaryValue(schemaId, row));
    }

    /**
     * Encode the {@code row} with a {@code schemaId} and a {@code rowId} to a byte array value.
     * Used for DV-enabled tables. The format is: {@code [RowId (varint)][SchemaId (2B)][BinaryRow
     * (variable)]}, i.e. a varint RowId followed by a {@link KvValueLayout#PLAIN} value.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     * @param rowId the RowId (log offset of the +I/+U record) to embed
     */
    public static byte[] encodeValueWithRowId(short schemaId, BinaryRow row, long rowId) {
        // The RowId prefix is only defined on top of the plain layout: a tagged layout requires
        // its value tag at offset 0, which FRocksDB reads natively.
        KvValueLayout layout = KvValueLayout.PLAIN;
        int varintLen = unsignedVarLongSize(rowId);
        byte[] values = new byte[varintLen + layout.rowPayloadOffset() + row.getSizeInBytes()];
        int offset = putUnsignedVarLong(values, 0, rowId);
        layout.writeSchemaId(values, offset, schemaId);
        row.copyTo(values, offset + layout.rowPayloadOffset());
        return values;
    }

    /**
     * Extract the RowId from the beginning of a DV-enabled value byte array. The RowId is stored as
     * an unsigned varint prefix.
     */
    public static long extractRowId(byte[] valueBytes) {
        return getUnsignedVarLong(valueBytes, 0);
    }

    /**
     * Return the number of bytes occupied by the varint-encoded RowId at the start of the given
     * value byte array.
     */
    public static int rowIdVarIntSize(byte[] valueBytes) {
        int pos = 0;
        while ((valueBytes[pos] & 0x80) != 0) {
            pos++;
        }
        return pos + 1;
    }

    // ---- Unsigned varint (LEB128) encoding/decoding ----

    /** Compute the number of bytes needed to encode the given long as an unsigned varint. */
    public static int unsignedVarLongSize(long value) {
        int size = 1;
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    /**
     * Write the given long as an unsigned varint into the byte array at the given offset. Returns
     * the new offset after writing.
     */
    static int putUnsignedVarLong(byte[] buf, int offset, long value) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buf[offset++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[offset++] = (byte) (value & 0x7F);
        return offset;
    }

    /** Read an unsigned varint-encoded long from the byte array starting at the given offset. */
    static long getUnsignedVarLong(byte[] buf, int offset) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf[offset++];
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
