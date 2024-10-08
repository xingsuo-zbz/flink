/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.utils.RawValueDataAsserter.equivalent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Test for {@link RowData}s. */
class RowDataTest {

    private StringData str;
    private RawValueData<String> generic;
    private DecimalData decimal1;
    private DecimalData decimal2;
    private BinaryArrayData array;
    private BinaryMapData map;
    private BinaryRowData underRow;
    private byte[] bytes;
    private RawValueDataSerializer<String> genericSerializer;
    private TimestampData timestamp1;
    private TimestampData timestamp2;

    @BeforeEach
    void before() {
        str = StringData.fromString("haha");
        generic = RawValueData.fromObject("haha");
        genericSerializer = new RawValueDataSerializer<>(StringSerializer.INSTANCE);
        decimal1 = DecimalData.fromUnscaledLong(10, 5, 0);
        decimal2 = DecimalData.fromBigDecimal(new BigDecimal(11), 20, 0);
        array = new BinaryArrayData();
        {
            BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
            arrayWriter.writeInt(0, 15);
            arrayWriter.writeInt(1, 16);
            arrayWriter.complete();
        }
        map = BinaryMapData.valueOf(array, array);
        underRow = new BinaryRowData(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(underRow);
            writer.writeInt(0, 15);
            writer.writeInt(1, 16);
            writer.complete();
        }
        bytes = new byte[] {1, 5, 6};
        timestamp1 = TimestampData.fromEpochMillis(123L);
        timestamp2 =
                TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
    }

    @Test
    void testBinaryRow() {
        BinaryRowData binaryRow = getBinaryRow();
        testGetters(binaryRow);
        testSetters(binaryRow);
    }

    @Test
    void testNestedRow() {
        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeRow(0, getBinaryRow(), null);
        writer.complete();

        RowData nestedRow = row.getRow(0, 18);
        testGetters(nestedRow);
        testSetters(nestedRow);
    }

    private BinaryRowData getBinaryRow() {
        BinaryRowData row = new BinaryRowData(18);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeBoolean(0, true);
        writer.writeByte(1, (byte) 1);
        writer.writeShort(2, (short) 2);
        writer.writeInt(3, 3);
        writer.writeLong(4, 4);
        writer.writeFloat(5, 5);
        writer.writeDouble(6, 6);
        writer.writeString(8, str);
        writer.writeRawValue(9, generic, genericSerializer);
        writer.writeDecimal(10, decimal1, 5);
        writer.writeDecimal(11, decimal2, 20);
        writer.writeArray(12, array, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
        writer.writeMap(
                13,
                map,
                new MapDataSerializer(
                        DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType()));
        writer.writeRow(
                14, underRow, new RowDataSerializer(RowType.of(new IntType(), new IntType())));
        writer.writeBinary(15, bytes);
        writer.writeTimestamp(16, timestamp1, 3);
        writer.writeTimestamp(17, timestamp2, 9);
        return row;
    }

    @Test
    void testGenericRow() {
        GenericRowData row = new GenericRowData(18);
        row.setField(0, true);
        row.setField(1, (byte) 1);
        row.setField(2, (short) 2);
        row.setField(3, 3);
        row.setField(4, (long) 4);
        row.setField(5, (float) 5);
        row.setField(6, (double) 6);
        row.setField(7, (char) 7);
        row.setField(8, str);
        row.setField(9, generic);
        row.setField(10, decimal1);
        row.setField(11, decimal2);
        row.setField(12, array);
        row.setField(13, map);
        row.setField(14, underRow);
        row.setField(15, bytes);
        row.setField(16, timestamp1);
        row.setField(17, timestamp2);
        testGetters(row);
    }

    @Test
    void testBoxedWrapperRow() {
        BoxedWrapperRowData row = new BoxedWrapperRowData(18);
        row.setBoolean(0, true);
        row.setByte(1, (byte) 1);
        row.setShort(2, (short) 2);
        row.setInt(3, 3);
        row.setLong(4, (long) 4);
        row.setFloat(5, (float) 5);
        row.setDouble(6, (double) 6);
        row.setNonPrimitiveValue(8, str);
        row.setNonPrimitiveValue(9, generic);
        row.setNonPrimitiveValue(10, decimal1);
        row.setNonPrimitiveValue(11, decimal2);
        row.setNonPrimitiveValue(12, array);
        row.setNonPrimitiveValue(13, map);
        row.setNonPrimitiveValue(14, underRow);
        row.setNonPrimitiveValue(15, bytes);
        row.setNonPrimitiveValue(16, timestamp1);
        row.setNonPrimitiveValue(17, timestamp2);
        testGetters(row);
        testSetters(row);
    }

    @Test
    void testJoinedRow() {
        GenericRowData row1 = new GenericRowData(5);
        row1.setField(0, true);
        row1.setField(1, (byte) 1);
        row1.setField(2, (short) 2);
        row1.setField(3, 3);
        row1.setField(4, (long) 4);

        GenericRowData row2 = new GenericRowData(13);
        row2.setField(0, (float) 5);
        row2.setField(1, (double) 6);
        row2.setField(2, (char) 7);
        row2.setField(3, str);
        row2.setField(4, generic);
        row2.setField(5, decimal1);
        row2.setField(6, decimal2);
        row2.setField(7, array);
        row2.setField(8, map);
        row2.setField(9, underRow);
        row2.setField(10, bytes);
        row2.setField(11, timestamp1);
        row2.setField(12, timestamp2);
        testGetters(new JoinedRowData(row1, row2));
    }

    private void testGetters(RowData row) {
        assertThat(row.getArity()).isEqualTo(18);

        // test header
        assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
        row.setRowKind(RowKind.DELETE);
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);

        // test get
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
        assertThat(row.getShort(2)).isEqualTo((short) 2);
        assertThat(row.getInt(3)).isEqualTo(3);
        assertThat(row.getLong(4)).isEqualTo(4L);
        assertThat(row.getFloat(5)).isEqualTo(5f);
        assertThat(row.getDouble(6)).isEqualTo(6d);
        assertThat(row.getString(8)).isEqualTo(str);
        assertThat(row.getRawValue(9)).satisfies(matching(equivalent(generic, genericSerializer)));
        assertThat(row.getDecimal(10, 5, 0)).isEqualTo(decimal1);
        assertThat(row.getDecimal(11, 20, 0)).isEqualTo(decimal2);
        assertThat(row.getArray(12)).isEqualTo(array);
        assertThat(row.getMap(13)).isEqualTo(map);
        assertThat(row.getRow(14, 2).getInt(0)).isEqualTo(15);
        assertThat(row.getRow(14, 2).getInt(1)).isEqualTo(16);
        assertThat(row.getBinary(15)).isEqualTo(bytes);
        assertThat(row.getTimestamp(16, 3)).isEqualTo(timestamp1);
        assertThat(row.getTimestamp(17, 9)).isEqualTo(timestamp2);
    }

    private void testSetters(RowData row) {
        if (!(row instanceof TypedSetters)) {
            throw new RuntimeException(
                    "testSetters only tests RowData which implements TypedSetters. "
                            + "However "
                            + row.getClass().getSimpleName()
                            + " doesn't implements TypedSetters interface.");
        }
        TypedSetters setter = (TypedSetters) row;
        // test set
        setter.setBoolean(0, false);
        assertThat(row.getBoolean(0)).isFalse();
        setter.setByte(1, (byte) 2);
        assertThat(row.getByte(1)).isEqualTo((byte) 2);
        setter.setShort(2, (short) 3);
        assertThat(row.getShort(2)).isEqualTo((short) 3);
        setter.setInt(3, 4);
        assertThat(row.getInt(3)).isEqualTo(4);
        setter.setLong(4, 5);
        assertThat(row.getLong(4)).isEqualTo(5L);
        setter.setFloat(5, 6);
        assertThat(row.getFloat(5)).isEqualTo(6f);
        setter.setDouble(6, 7);
        assertThat(row.getDouble(6)).isEqualTo(7d);
        setter.setDecimal(10, DecimalData.fromUnscaledLong(11, 5, 0), 5);
        assertThat(row.getDecimal(10, 5, 0)).isEqualTo(DecimalData.fromUnscaledLong(11, 5, 0));
        setter.setDecimal(11, DecimalData.fromBigDecimal(new BigDecimal(12), 20, 0), 20);
        assertThat(row.getDecimal(11, 20, 0))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(12), 20, 0));

        setter.setTimestamp(16, TimestampData.fromEpochMillis(456L), 3);
        assertThat(row.getTimestamp(16, 3)).isEqualTo(TimestampData.fromEpochMillis(456L));
        setter.setTimestamp(
                17,
                TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-01 00:00:00.123456789")),
                9);
        assertThat(row.getTimestamp(17, 9))
                .isEqualTo(
                        TimestampData.fromTimestamp(
                                Timestamp.valueOf("1970-01-01 00:00:00.123456789")));

        // test null
        assertThat(row.isNullAt(0)).isFalse();
        setter.setNullAt(0);
        assertThat(row.isNullAt(0)).isTrue();
    }
}
