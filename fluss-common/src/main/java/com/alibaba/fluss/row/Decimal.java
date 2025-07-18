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

package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.PublicStable;
import com.alibaba.fluss.types.DecimalType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An internal data structure representing data of {@link DecimalType}.
 *
 * <p>This data structure is immutable and might store decimal values in a compact representation
 * (as a long value) if values are small enough.
 *
 * @since 0.1
 */
@PublicStable
public final class Decimal implements Comparable<Decimal>, Serializable {
    private static final long serialVersionUID = 1L;

    // member fields and static fields are package-visible,
    // in order to be accessible for DecimalUtils

    static final int MAX_COMPACT_PRECISION = 18;

    /** Maximum number of decimal digits a Long can represent. (1e18 < Long.MaxValue < 1e19) */
    static final int MAX_LONG_DIGITS = 18;

    static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

    static {
        POW10[0] = 1;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = 10 * POW10[i - 1];
        }
    }

    // The semantics of the fields are as follows:
    //  - `precision` and `scale` represent the precision and scale of SQL decimal type
    //  - If `decimalVal` is set, it represents the whole decimal value
    //  - Otherwise, the decimal value is longVal/(10^scale).
    //
    // Note that the (precision, scale) must be correct.
    // if precision > MAX_COMPACT_PRECISION,
    //   `decimalVal` represents the value. `longVal` is undefined
    // otherwise, (longVal, scale) represents the value
    //   `decimalVal` may be set and cached

    final int precision;
    final int scale;

    final long longVal;
    BigDecimal decimalVal;

    // this constructor does not perform any sanity check.
    Decimal(int precision, int scale, long longVal, BigDecimal decimalVal) {
        this.precision = precision;
        this.scale = scale;
        this.longVal = longVal;
        this.decimalVal = decimalVal;
    }

    // ------------------------------------------------------------------------------------------
    // Public Interfaces
    // ------------------------------------------------------------------------------------------

    /**
     * Returns the <i>precision</i> of this {@link Decimal}.
     *
     * <p>The precision is the number of digits in the unscaled value.
     */
    public int precision() {
        return precision;
    }

    /** Returns the <i>scale</i> of this {@link Decimal}. */
    public int scale() {
        return scale;
    }

    /** Converts this {@link Decimal} into an instance of {@link BigDecimal}. */
    public BigDecimal toBigDecimal() {
        BigDecimal bd = decimalVal;
        if (bd == null) {
            decimalVal = bd = BigDecimal.valueOf(longVal, scale);
        }
        return bd;
    }

    /**
     * Returns a long describing the <i>unscaled value</i> of this {@link Decimal}.
     *
     * @throws ArithmeticException if this {@link Decimal} does not exactly fit in a long.
     */
    public long toUnscaledLong() {
        if (isCompact()) {
            return longVal;
        } else {
            return toBigDecimal().unscaledValue().longValueExact();
        }
    }

    /**
     * Returns a byte array describing the <i>unscaled value</i> of this {@link Decimal}.
     *
     * @return the unscaled byte array of this {@link Decimal}.
     */
    public byte[] toUnscaledBytes() {
        return toBigDecimal().unscaledValue().toByteArray();
    }

    /** Returns whether the decimal value is small enough to be stored in a long. */
    public boolean isCompact() {
        return precision <= MAX_COMPACT_PRECISION;
    }

    /** Returns a copy of this {@link Decimal} object. */
    public Decimal copy() {
        return new Decimal(precision, scale, longVal, decimalVal);
    }

    @Override
    public int hashCode() {
        return toBigDecimal().hashCode();
    }

    @Override
    public int compareTo(@Nonnull Decimal that) {
        if (this.isCompact() && that.isCompact() && this.scale == that.scale) {
            return Long.compare(this.longVal, that.longVal);
        }
        return this.toBigDecimal().compareTo(that.toBigDecimal());
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Decimal)) {
            return false;
        }
        Decimal that = (Decimal) o;
        return this.compareTo(that) == 0;
    }

    @Override
    public String toString() {
        return toBigDecimal().toPlainString();
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link Decimal} from a {@link BigDecimal} and the given precision and
     * scale.
     *
     * <p>The returned decimal value may be rounded to have the desired scale. The precision will be
     * checked. If the precision overflows, null will be returned.
     */
    public static @Nullable Decimal fromBigDecimal(BigDecimal bd, int precision, int scale) {
        bd = bd.setScale(scale, RoundingMode.HALF_UP);
        if (bd.precision() > precision) {
            return null;
        }

        long longVal = -1;
        if (precision <= MAX_COMPACT_PRECISION) {
            longVal = bd.movePointRight(scale).longValueExact();
        }
        return new Decimal(precision, scale, longVal, bd);
    }

    /**
     * Creates an instance of {@link Decimal} from an unscaled long value and the given precision
     * and scale.
     */
    public static Decimal fromUnscaledLong(long unscaledLong, int precision, int scale) {
        checkArgument(precision > 0 && precision <= MAX_LONG_DIGITS);
        return new Decimal(precision, scale, unscaledLong, null);
    }

    /**
     * Creates an instance of {@link Decimal} from an unscaled byte array value and the given
     * precision and scale.
     */
    public static Decimal fromUnscaledBytes(byte[] unscaledBytes, int precision, int scale) {
        BigDecimal bd = new BigDecimal(new BigInteger(unscaledBytes), scale);
        return fromBigDecimal(bd, precision, scale);
    }

    /**
     * Creates an instance of {@link Decimal} for a zero value with the given precision and scale.
     *
     * <p>The precision will be checked. If the precision overflows, null will be returned.
     */
    public static @Nullable Decimal zero(int precision, int scale) {
        if (precision <= MAX_COMPACT_PRECISION) {
            return new Decimal(precision, scale, 0, null);
        } else {
            return fromBigDecimal(BigDecimal.ZERO, precision, scale);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    /** Returns whether the decimal value is small enough to be stored in a long. */
    public static boolean isCompact(int precision) {
        return precision <= MAX_COMPACT_PRECISION;
    }
}
