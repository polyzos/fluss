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

import org.apache.fluss.record.bytesview.ByteBufBytesView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.FileRegionBytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.record.send.WritableOutput;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.utils.ProtoCodecUtils;

/**
 * Response message for FULL_SCAN RPC.
 * Fields:
 *  optional int32 error_code = 1;
 *  optional string error_message = 2;
 *  optional bytes records = 3; // DefaultValueRecordBatch
 *  optional int64 estimated_key_count = 4;
 *  optional int64 elapsed_ms = 5;
 */
public final class FullScanResponse implements ApiMessage, ErrorMessage {
    private int errorCode;
    private static final int _ERROR_CODE_FIELD_NUMBER = 1;
    private static final int _ERROR_CODE_TAG = (_ERROR_CODE_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_VARINT;
    private static final int _ERROR_CODE_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_ERROR_CODE_TAG);
    private static final int _ERROR_CODE_MASK = 1 << (0 % 32);

    @Override
    public boolean hasErrorCode() {
        return (_bitField0 & _ERROR_CODE_MASK) != 0;
    }

    @Override
    public int getErrorCode() {
        if (!hasErrorCode()) {
            throw new IllegalStateException("Field 'error_code' is not set");
        }
        return errorCode;
    }

    @Override
    public FullScanResponse setErrorCode(int errorCode) {
        this.errorCode = errorCode;
        _bitField0 |= _ERROR_CODE_MASK;
        _cachedSize = -1;
        return this;
    }

    @Override
    public FullScanResponse clearErrorCode() {
        _bitField0 &= ~_ERROR_CODE_MASK;
        return this;
    }

    private String errorMessage;
    private int _errorMessageLen = -1;
    private static final int _ERROR_MESSAGE_FIELD_NUMBER = 2;
    private static final int _ERROR_MESSAGE_TAG = (_ERROR_MESSAGE_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED;
    private static final int _ERROR_MESSAGE_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_ERROR_MESSAGE_TAG);
    private static final int _ERROR_MESSAGE_MASK = 1 << (1 % 32);

    @Override
    public boolean hasErrorMessage() {
        return (_bitField0 & _ERROR_MESSAGE_MASK) != 0;
    }

    @Override
    public String getErrorMessage() {
        if (!hasErrorMessage()) {
            throw new IllegalStateException("Field 'error_message' is not set");
        }
        return errorMessage;
    }

    @Override
    public FullScanResponse setErrorMessage(String errorMessage) {
        if (errorMessage == null) {
            throw new NullPointerException("Field 'error_message' cannot be null");
        }
        this.errorMessage = errorMessage;
        _bitField0 |= _ERROR_MESSAGE_MASK;
        _errorMessageLen = ProtoCodecUtils.computeStringUTF8Size(errorMessage);
        _cachedSize = -1;
        return this;
    }

    @Override
    public FullScanResponse clearErrorMessage() {
        _bitField0 &= ~_ERROR_MESSAGE_MASK;
        errorMessage = null;
        _errorMessageLen = -1;
        return this;
    }

    private BytesView records = null;
    private int _recordsIdx = -1;
    private int _recordsLen = -1;
    private static final int _RECORDS_FIELD_NUMBER = 3;
    private static final int _RECORDS_TAG = (_RECORDS_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED;
    private static final int _RECORDS_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_RECORDS_TAG);
    private static final int _RECORDS_MASK = 1 << (2 % 32);

    public boolean hasRecords() { return (_bitField0 & _RECORDS_MASK) != 0; }
    public int getRecordsSize() {
        if (!hasRecords()) { throw new IllegalStateException("Field 'records' is not set"); }
        return _recordsLen;
    }
    public byte[] getRecords() {
        ByteBuf _b = getRecordsSlice();
        byte[] res = new byte[_b.readableBytes()];
        _b.getBytes(0, res);
        return res;
    }
    public ByteBuf getRecordsSlice() {
        if (!hasRecords()) { throw new IllegalStateException("Field 'records' is not set"); }
        if (records == null) {
            return _parsedBuffer.slice(_recordsIdx, _recordsLen);
        } else {
            ByteBuf _b = records.getByteBuf();
            return _b.slice(0, _recordsLen);
        }
    }
    public FullScanResponse setRecords(byte[] records) { setRecordsBytesView(new ByteBufBytesView(records)); return this; }
    public FullScanResponse setRecords(ByteBuf records) { setRecordsBytesView(new ByteBufBytesView(records)); return this; }
    public FullScanResponse setRecords(org.apache.fluss.memory.MemorySegment records, int position, int size) {
        setRecordsBytesView(new MemorySegmentBytesView(records, position, size)); return this; }
    public FullScanResponse setRecords(java.nio.channels.FileChannel records, long position, int size) {
        setRecordsBytesView(new FileRegionBytesView(records, position, size)); return this; }
    public FullScanResponse setRecordsBytesView(BytesView records) {
        this.records = records; _bitField0 |= _RECORDS_MASK; _recordsIdx = -1; _recordsLen = records.getBytesLength(); _cachedSize = -1; return this; }
    public FullScanResponse clearRecords() { _bitField0 &= ~_RECORDS_MASK; records = null; _recordsIdx = -1; _recordsLen = -1; return this; }

    private long estimatedKeyCount;
    private static final int _EST_COUNT_FIELD_NUMBER = 4;
    private static final int _EST_COUNT_TAG = (_EST_COUNT_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_VARINT;
    private static final int _EST_COUNT_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_EST_COUNT_TAG);
    private static final int _EST_COUNT_MASK = 1 << (3 % 32);
    public boolean hasEstimatedKeyCount() { return (_bitField0 & _EST_COUNT_MASK) != 0; }
    public long getEstimatedKeyCount() { if (!hasEstimatedKeyCount()) { throw new IllegalStateException("Field 'estimated_key_count' is not set"); } return estimatedKeyCount; }
    public FullScanResponse setEstimatedKeyCount(long v) { this.estimatedKeyCount = v; _bitField0 |= _EST_COUNT_MASK; _cachedSize = -1; return this; }
    public FullScanResponse clearEstimatedKeyCount() { _bitField0 &= ~_EST_COUNT_MASK; return this; }

    private long elapsedMs;
    private static final int _ELAPSED_FIELD_NUMBER = 5;
    private static final int _ELAPSED_TAG = (_ELAPSED_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
            | ProtoCodecUtils.WIRETYPE_VARINT;
    private static final int _ELAPSED_TAG_SIZE = ProtoCodecUtils.computeVarIntSize(_ELAPSED_TAG);
    private static final int _ELAPSED_MASK = 1 << (4 % 32);
    public boolean hasElapsedMs() { return (_bitField0 & _ELAPSED_MASK) != 0; }
    public long getElapsedMs() { if (!hasElapsedMs()) { throw new IllegalStateException("Field 'elapsed_ms' is not set"); } return elapsedMs; }
    public FullScanResponse setElapsedMs(long v) { this.elapsedMs = v; _bitField0 |= _ELAPSED_MASK; _cachedSize = -1; return this; }
    public FullScanResponse clearElapsedMs() { _bitField0 &= ~_ELAPSED_MASK; return this; }

    private int _bitField0;

    @Override
    public int writeTo(ByteBuf _b) {
        int _writeIdx = _b.writerIndex();
        org.apache.fluss.record.send.ByteBufWritableOutput _w = new org.apache.fluss.record.send.ByteBufWritableOutput(_b);
        this.writeTo(_w);
        return (_b.writerIndex() - _writeIdx);
    }

    @Override
    public void writeTo(WritableOutput _w) {
        if (hasErrorCode()) { _w.writeVarInt(_ERROR_CODE_TAG); _w.writeVarInt(errorCode); }
        if (hasErrorMessage()) { _w.writeVarInt(_ERROR_MESSAGE_TAG); _w.writeVarInt(_errorMessageLen); _w.writeString(errorMessage, _errorMessageLen); }
        if (hasRecords()) { _w.writeVarInt(_RECORDS_TAG); _w.writeVarInt(_recordsLen); if (_recordsIdx == -1) { _w.writeBytes(records); } else { _w.writeByteBuf(_parsedBuffer, _recordsIdx, _recordsLen); } }
        if (hasEstimatedKeyCount()) { _w.writeVarInt(_EST_COUNT_TAG); _w.writeVarInt64(estimatedKeyCount); }
        if (hasElapsedMs()) { _w.writeVarInt(_ELAPSED_TAG); _w.writeVarInt64(elapsedMs); }
    }

    @Override
    public int totalSize() {
        if (_cachedSize > -1) { return _cachedSize; }
        int _size = 0;
        if (hasErrorCode()) { _size += _ERROR_CODE_TAG_SIZE; _size += ProtoCodecUtils.computeVarIntSize(errorCode); }
        if (hasErrorMessage()) { _size += _ERROR_MESSAGE_TAG_SIZE; _size += ProtoCodecUtils.computeVarIntSize(_errorMessageLen); _size += _errorMessageLen; }
        if (hasRecords()) { _size += _RECORDS_TAG_SIZE; _size += ProtoCodecUtils.computeVarIntSize(_recordsLen) + _recordsLen; }
        if (hasEstimatedKeyCount()) { _size += _EST_COUNT_TAG_SIZE; _size += ProtoCodecUtils.computeVarInt64Size(estimatedKeyCount); }
        if (hasElapsedMs()) { _size += _ELAPSED_TAG_SIZE; _size += ProtoCodecUtils.computeVarInt64Size(elapsedMs); }
        _cachedSize = _size; return _size;
    }

    @Override
    public int zeroCopySize() { int _size = 0; _size += (hasRecords() && _recordsIdx == -1) ? this.records.getZeroCopyLength() : 0; return _size; }

    @Override
    public void parseFrom(ByteBuf _buffer, int _size) {
        clear();
        int _endIdx = _buffer.readerIndex() + _size;
        while (_buffer.readerIndex() < _endIdx) {
            int _tag = ProtoCodecUtils.readVarInt(_buffer);
            switch (_tag) {
                case _ERROR_CODE_TAG: _bitField0 |= _ERROR_CODE_MASK; errorCode = ProtoCodecUtils.readVarInt(_buffer); break;
                case _ERROR_MESSAGE_TAG: _bitField0 |= _ERROR_MESSAGE_MASK; _errorMessageLen = ProtoCodecUtils.readVarInt(_buffer); int _errorMessageBufferIdx = _buffer.readerIndex(); errorMessage = ProtoCodecUtils.readString(_buffer, _buffer.readerIndex(), _errorMessageLen); _buffer.skipBytes(_errorMessageLen); break;
                case _RECORDS_TAG: _bitField0 |= _RECORDS_MASK; _recordsLen = ProtoCodecUtils.readVarInt(_buffer); _recordsIdx = _buffer.readerIndex(); _buffer.skipBytes(_recordsLen); break;
                case _EST_COUNT_TAG: _bitField0 |= _EST_COUNT_MASK; estimatedKeyCount = ProtoCodecUtils.readVarInt64(_buffer); break;
                case _ELAPSED_TAG: _bitField0 |= _ELAPSED_MASK; elapsedMs = ProtoCodecUtils.readVarInt64(_buffer); break;
                default: ProtoCodecUtils.skipUnknownField(_tag, _buffer);
            }
        }
        _parsedBuffer = _buffer;
    }

    @Override
    public boolean isLazilyParsed() { return true; }

    @Override
    public ByteBuf getParsedByteBuf() { return _parsedBuffer; }

    public FullScanResponse clear() {
        errorMessage = null; _errorMessageLen = -1; records = null; _recordsIdx = -1; _recordsLen = -1; _parsedBuffer = null; _cachedSize = -1; _bitField0 = 0; estimatedKeyCount = 0; elapsedMs = 0; return this;
    }

    public FullScanResponse copyFrom(FullScanResponse _other) {
        _cachedSize = -1;
        if (_other.hasErrorCode()) { setErrorCode(_other.errorCode); }
        if (_other.hasErrorMessage()) { setErrorMessage(_other.getErrorMessage()); }
        if (_other.hasRecords()) { setRecords(_other.getRecords()); }
        if (_other.hasEstimatedKeyCount()) { setEstimatedKeyCount(_other.estimatedKeyCount); }
        if (_other.hasElapsedMs()) { setElapsedMs(_other.elapsedMs); }
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
