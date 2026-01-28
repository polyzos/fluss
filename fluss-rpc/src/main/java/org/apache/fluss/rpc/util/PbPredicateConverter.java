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

package org.apache.fluss.rpc.util;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Contains;
import org.apache.fluss.predicate.EndsWith;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.IsNotNull;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafFunction;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessOrEqual;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.NotEqual;
import org.apache.fluss.predicate.NotIn;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.predicate.StartsWith;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbCompoundFunction;
import org.apache.fluss.rpc.messages.PbCompoundPredicate;
import org.apache.fluss.rpc.messages.PbDataType;
import org.apache.fluss.rpc.messages.PbDataTypeRoot;
import org.apache.fluss.rpc.messages.PbFieldRef;
import org.apache.fluss.rpc.messages.PbLeafFunction;
import org.apache.fluss.rpc.messages.PbLeafPredicate;
import org.apache.fluss.rpc.messages.PbLiteralValue;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.rpc.messages.PbPredicateType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PbPredicateConverter implements PredicateVisitor<PbPredicate> {
    public static final PbPredicateConverter INSTANCE = new PbPredicateConverter();

    @Override
    public PbPredicate visit(LeafPredicate predicate) {
        PbLeafPredicate pbLeafPredicate = new PbLeafPredicate();
        PbFieldRef fieldRef = new PbFieldRef();
        fieldRef.setDataType(toPbDataType(predicate.type()));
        fieldRef.setFieldIndex(predicate.index());
        fieldRef.setFieldName(predicate.fieldName());
        pbLeafPredicate.setFunction(toPbLeafFunction(predicate.function()));
        pbLeafPredicate.setFieldRef(fieldRef);

        List<PbLiteralValue> literals = new ArrayList<>();
        for (Object literal : predicate.literals()) {
            literals.add(toPbLiteralValue(predicate.type(), literal));
        }
        pbLeafPredicate.addAllLiterals(literals);

        PbPredicate pbPredicate = new PbPredicate();
        pbPredicate.setLeaf(pbLeafPredicate);
        pbPredicate.setType(PbPredicateType.LEAF);
        return pbPredicate;
    }

    @Override
    public PbPredicate visit(CompoundPredicate predicate) {
        PbCompoundPredicate pbCompoundPredicate = new PbCompoundPredicate();
        pbCompoundPredicate.setFunction(toPbCompoundFunction(predicate.function()));
        pbCompoundPredicate.addAllChildrens(
                predicate.children().stream()
                        .map(PredicateMessageUtils::toPbPredicate)
                        .collect(Collectors.toList()));
        PbPredicate pbPredicate = new PbPredicate();
        pbPredicate.setCompound(pbCompoundPredicate);
        pbPredicate.setType(PbPredicateType.COMPOUND);
        return pbPredicate;
    }

    private static PbDataType toPbDataType(DataType dataType) {
        PbDataType pbDataType = new PbDataType();
        pbDataType.setNullable(dataType.isNullable());
        if (dataType instanceof BooleanType) {
            pbDataType.setRoot(PbDataTypeRoot.BOOLEAN);
        } else if (dataType instanceof IntType) {
            pbDataType.setRoot(PbDataTypeRoot.INT);
        } else if (dataType instanceof TinyIntType) {
            pbDataType.setRoot(PbDataTypeRoot.TINYINT);
        } else if (dataType instanceof SmallIntType) {
            pbDataType.setRoot(PbDataTypeRoot.SMALLINT);
        } else if (dataType instanceof BigIntType) {
            pbDataType.setRoot(PbDataTypeRoot.BIGINT);
        } else if (dataType instanceof FloatType) {
            pbDataType.setRoot(PbDataTypeRoot.FLOAT);
        } else if (dataType instanceof DoubleType) {
            pbDataType.setRoot(PbDataTypeRoot.DOUBLE);
        } else if (dataType instanceof CharType) {
            pbDataType.setRoot(PbDataTypeRoot.CHAR);
            pbDataType.setLength(((CharType) dataType).getLength());
        } else if (dataType instanceof StringType) {
            pbDataType.setRoot(PbDataTypeRoot.VARCHAR);
        } else if (dataType instanceof DecimalType) {
            pbDataType.setRoot(PbDataTypeRoot.DECIMAL);
            pbDataType.setPrecision(((DecimalType) dataType).getPrecision());
            pbDataType.setScale(((DecimalType) dataType).getScale());
        } else if (dataType instanceof DateType) {
            pbDataType.setRoot(PbDataTypeRoot.DATE);
        } else if (dataType instanceof TimeType) {
            pbDataType.setRoot(PbDataTypeRoot.TIME_WITHOUT_TIME_ZONE);
            pbDataType.setPrecision(((TimeType) dataType).getPrecision());
        } else if (dataType instanceof TimestampType) {
            pbDataType.setRoot(PbDataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
            pbDataType.setPrecision(((TimestampType) dataType).getPrecision());
        } else if (dataType instanceof LocalZonedTimestampType) {
            pbDataType.setRoot(PbDataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            pbDataType.setPrecision(((LocalZonedTimestampType) dataType).getPrecision());
        } else if (dataType instanceof BinaryType) {
            pbDataType.setRoot(PbDataTypeRoot.BINARY);
            pbDataType.setLength(((BinaryType) dataType).getLength());
        } else if (dataType instanceof BytesType) {
            pbDataType.setRoot(PbDataTypeRoot.BYTES);
        } else {
            throw new IllegalArgumentException("Unknown data type: " + dataType.getClass());
        }
        return pbDataType;
    }

    private static PbLiteralValue toPbLiteralValue(DataType type, Object literal) {
        PbLiteralValue pbLiteralValue = new PbLiteralValue();
        pbLiteralValue.setType(toPbDataType(type));
        if (null == literal) {
            pbLiteralValue.setIsNull(true);
            return pbLiteralValue;
        }
        pbLiteralValue.setIsNull(false);
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                pbLiteralValue.setStringValue(literal.toString());
                break;
            case BOOLEAN:
                pbLiteralValue.setBooleanValue((Boolean) literal);
                break;
            case BINARY:
            case BYTES:
                pbLiteralValue.setBinaryValue((byte[]) literal);
                break;
            case DECIMAL:
                Decimal decimal = (Decimal) literal;
                if (decimal.isCompact()) {
                    pbLiteralValue.setDecimalValue(decimal.toUnscaledLong());
                } else {
                    pbLiteralValue.setDecimalBytes(decimal.toUnscaledBytes());
                }
                break;
            case TINYINT:
                pbLiteralValue.setIntValue((Byte) literal);
                break;
            case SMALLINT:
                pbLiteralValue.setIntValue((Short) literal);
                break;
            case INTEGER:
                pbLiteralValue.setIntValue((Integer) literal);
                break;
            case DATE:
                pbLiteralValue.setBigintValue(((LocalDate) literal).toEpochDay());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                pbLiteralValue.setIntValue(
                        (int) (((LocalTime) literal).toNanoOfDay() / 1_000_000L));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                pbLiteralValue.setTimestampMillisValue(((TimestampNtz) literal).getMillisecond());
                pbLiteralValue.setTimestampNanoOfMillisValue(
                        ((TimestampNtz) literal).getNanoOfMillisecond());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                pbLiteralValue.setTimestampMillisValue(
                        ((TimestampLtz) literal).getEpochMillisecond());
                pbLiteralValue.setTimestampNanoOfMillisValue(
                        ((TimestampLtz) literal).getNanoOfMillisecond());
                break;
            case BIGINT:
                pbLiteralValue.setBigintValue((Long) literal);
                break;
            case FLOAT:
                pbLiteralValue.setFloatValue((Float) literal);
                break;
            case DOUBLE:
                pbLiteralValue.setDoubleValue((Double) literal);
                break;
            default:
                throw new IllegalArgumentException("Unknown data type: " + type.getTypeRoot());
        }
        return pbLiteralValue;
    }

    private static PbLeafFunction toPbLeafFunction(LeafFunction function) {
        if (function.equals(Equal.INSTANCE)) {
            return PbLeafFunction.EQUAL;
        }
        if (function.equals(NotEqual.INSTANCE)) {
            return PbLeafFunction.NOT_EQUAL;
        }
        if (function.equals(LessThan.INSTANCE)) {
            return PbLeafFunction.LESS_THAN;
        }
        if (function.equals(LessOrEqual.INSTANCE)) {
            return PbLeafFunction.LESS_OR_EQUAL;
        }
        if (function.equals(GreaterThan.INSTANCE)) {
            return PbLeafFunction.GREATER_THAN;
        }
        if (function.equals(GreaterOrEqual.INSTANCE)) {
            return PbLeafFunction.GREATER_OR_EQUAL;
        }
        if (function.equals(IsNull.INSTANCE)) {
            return PbLeafFunction.IS_NULL;
        }
        if (function.equals(IsNotNull.INSTANCE)) {
            return PbLeafFunction.IS_NOT_NULL;
        }
        if (function.equals(StartsWith.INSTANCE)) {
            return PbLeafFunction.STARTS_WITH;
        }
        if (function.equals(Contains.INSTANCE)) {
            return PbLeafFunction.CONTAINS;
        }
        if (function.equals(EndsWith.INSTANCE)) {
            return PbLeafFunction.END_WITH;
        }
        if (function.equals(In.INSTANCE)) {
            return PbLeafFunction.IN;
        }
        if (function.equals(NotIn.INSTANCE)) {
            return PbLeafFunction.NOT_IN;
        }
        throw new IllegalArgumentException("Unknown leaf function: " + function);
    }

    private static PbCompoundFunction toPbCompoundFunction(
            CompoundPredicate.Function compoundFunction) {
        if (compoundFunction.equals(And.INSTANCE)) {
            return PbCompoundFunction.AND;
        }
        if (compoundFunction.equals(Or.INSTANCE)) {
            return PbCompoundFunction.OR;
        }
        throw new IllegalArgumentException("Unknown compound function: " + compoundFunction);
    }
}
