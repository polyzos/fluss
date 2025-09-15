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
package org.apache.fluss.rpc.messages;

import org.apache.fluss.record.send.WritableOutput;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.utils.ProtoCodecUtils;

/**
 * A minimal hand-written message for FULL_SCAN RPC.
 * Fields:
 *  - required int64 table_id = 1;
 *  - optional int64 partition_id = 2;
 */
public final class FullScanRequest implements ApiMessage {
    private long tableId;
    private static final int _TABLE_ID_FIELD_NUMBER = 1;
    private static final int _TABLE_ID_TAG = (_TABLE_ID_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_VARINT;
    private static final int _TABLE_ID_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_TABLE_ID_TAG);
    private static final int _TABLE_ID_MASK = 1 << (0 % 32);

    public boolean hasTableId() {
        return (_bitField0 & _TABLE_ID_MASK) != 0;
    }

    public long getTableId() {
        if (!hasTableId()) {
            throw new IllegalStateException("Field 'table_id' is not set");
        }
        return tableId;
    }

    public FullScanRequest setTableId(long tableId) {
        this.tableId = tableId;
        _bitField0 |= _TABLE_ID_MASK;
        _cachedSize = -1;
        return this;
    }

    public FullScanRequest clearTableId() {
        _bitField0 &= ~_TABLE_ID_MASK;
        return this;
    }

    private long partitionId;
    private static final int _PARTITION_ID_FIELD_NUMBER = 2;
    private static final int _PARTITION_ID_TAG = (_PARTITION_ID_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_VARINT;
    private static final int _PARTITION_ID_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_PARTITION_ID_TAG);
    private static final int _PARTITION_ID_MASK = 1 << (1 % 32);

    public boolean hasPartitionId() {
        return (_bitField0 & _PARTITION_ID_MASK) != 0;
    }

    public long getPartitionId() {
        if (!hasPartitionId()) {
            throw new IllegalStateException("Field 'partition_id' is not set");
        }
        return partitionId;
    }

    public FullScanRequest setPartitionId(long partitionId) {
        this.partitionId = partitionId;
        _bitField0 |= _PARTITION_ID_MASK;
        _cachedSize = -1;
        return this;
    }

    public FullScanRequest clearPartitionId() {
        _bitField0 &= ~_PARTITION_ID_MASK;
        return this;
    }

    private int _bitField0;
    private static final int _REQUIRED_FIELDS_MASK0 = 0 | _TABLE_ID_MASK;

    @Override
    public int writeTo(ByteBuf _b) {
        int _writeIdx = _b.writerIndex();
        org.apache.fluss.record.send.ByteBufWritableOutput _w = new org.apache.fluss.record.send.ByteBufWritableOutput(_b);
        this.writeTo(_w);
        return (_b.writerIndex() - _writeIdx);
    }

    @Override
    public void writeTo(WritableOutput _w) {
        checkRequiredFields();
        _w.writeVarInt(_TABLE_ID_TAG);
        _w.writeVarInt64(tableId);
        if (hasPartitionId()) {
            _w.writeVarInt(_PARTITION_ID_TAG);
            _w.writeVarInt64(partitionId);
        }
    }

    @Override
    public int totalSize() {
        checkRequiredFields();
        if (_cachedSize > -1) {
            return _cachedSize;
        }
        int _size = 0;
        _size += _TABLE_ID_TAG_SIZE;
        _size += ProtoCodecUtils.computeVarInt64Size(tableId);
        if (hasPartitionId()) {
            _size += _PARTITION_ID_TAG_SIZE;
            _size += ProtoCodecUtils.computeVarInt64Size(partitionId);
        }
        _cachedSize = _size;
        return _size;
    }

    @Override
    public int zeroCopySize() {
        return 0;
    }

    @Override
    public void parseFrom(ByteBuf _buffer, int _size) {
        clear();
        int _endIdx = _buffer.readerIndex() + _size;
        while (_buffer.readerIndex() < _endIdx) {
            int _tag = ProtoCodecUtils.readVarInt(_buffer);
            switch (_tag) {
                case _TABLE_ID_TAG:
                    _bitField0 |= _TABLE_ID_MASK;
                    tableId = ProtoCodecUtils.readVarInt64(_buffer);
                    break;
                case _PARTITION_ID_TAG:
                    _bitField0 |= _PARTITION_ID_MASK;
                    partitionId = ProtoCodecUtils.readVarInt64(_buffer);
                    break;
                default:
                    ProtoCodecUtils.skipUnknownField(_tag, _buffer);
            }
        }
        checkRequiredFields();
        _parsedBuffer = _buffer;
    }

    @Override
    public boolean isLazilyParsed() {
        return false;
    }

    @Override
    public ByteBuf getParsedByteBuf() {
        return _parsedBuffer;
    }

    private void checkRequiredFields() {
        if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
            throw new IllegalStateException("Some required fields are missing");
        }
    }

    public FullScanRequest clear() {
        _parsedBuffer = null;
        _cachedSize = -1;
        _bitField0 = 0;
        return this;
    }

    public FullScanRequest copyFrom(FullScanRequest _other) {
        _cachedSize = -1;
        if (_other.hasTableId()) {
            setTableId(_other.tableId);
        }
        if (_other.hasPartitionId()) {
            setPartitionId(_other.partitionId);
        }
        return this;
    }

    @Override
    public byte[] toByteArray() {
        byte[] a = new byte[this.totalSize()];
        ByteBuf b = Unpooled.wrappedBuffer(a).writerIndex(0);
        this.writeTo(b);
        return a;
    }

    @Override
    public void parseFrom(byte[] a) {
        ByteBuf b = Unpooled.wrappedBuffer(a);
        this.parseFrom(b, b.readableBytes());
    }

    private int _cachedSize = -1;
    private ByteBuf _parsedBuffer;
}
