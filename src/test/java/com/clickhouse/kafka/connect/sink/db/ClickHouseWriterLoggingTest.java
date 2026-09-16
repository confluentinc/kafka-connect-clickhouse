package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests proving that a record's field values never reach the connector's ERROR logs on the
 * value-conversion and insert-failure paths of {@link ClickHouseWriter}.
 *
 * <p>These cover the sensitive-log audit findings for
 * {@code ClickHouseWriter.java:458} (DateTime parse error, F1),
 * {@code ClickHouseWriter.java:639} (value-conversion error, F2) and
 * {@code ClickHouseWriter.java:941/:952} (raw insert exception, F3).
 *
 * <p>The unit-test logging backend is slf4j-simple, which writes to {@code System.err}. Each test
 * captures {@code System.err}, drives the failing path with a synthetic canary value, asserts the
 * expected error log actually fired (a positive control that guards against a false pass if capture
 * breaks) and asserts the canary is absent from the captured log.
 */
public class ClickHouseWriterLoggingTest {

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code body} with {@code System.err} redirected and returns everything it logged. */
    private static String captureStderr(ThrowingRunnable body) {
        PrintStream original = System.err;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            body.run();
        } catch (Exception ignored) {
            // The paths under test are expected to throw; the assertions are only on the log.
        } finally {
            System.err.flush();
            System.setErr(original);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = ClickHouseWriter.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    // F1 - ClickHouseWriter.java:458: the DateTimeParseException must not carry the field value.
    @Test
    public void dateTime64ParseError_doesNotLogFieldValue() throws Exception {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        ClickHouseSinkConfig csc = mock(ClickHouseSinkConfig.class);
        when(csc.getDateTimeFormats()).thenReturn(Collections.emptyMap());
        setField(writer, "csc", csc);

        String canary = "CANARY_DATETIME_9999_99_99";
        Data value = new Data(Schema.STRING_SCHEMA, canary);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        String stderr = captureStderr(() ->
                writer.doWriteDates(Type.DateTime64, stream, value, 3, "ts_col"));

        assertTrue(stderr.contains("Error parsing DateTime64 value for column"),
                "positive control: the DateTime64 parse-error log should have fired. Captured: " + stderr);
        assertFalse(stderr.contains(canary),
                "datetime field value leaked into the ERROR log: " + stderr);
    }

    // F2 - ClickHouseWriter.java:639: the value-conversion exception must not be logged (UUID path).
    @Test
    public void valueConversionError_doesNotLogFieldValue() {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        Column column = Column.builder().type(Type.UUID).name("uuid_col").build();

        String canary = "CANARY_NOT_A_UUID_VALUE";
        Data value = new Data(Schema.STRING_SCHEMA, canary);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        String stderr = captureStderr(() ->
                writer.doWriteColValue(column, stream, value, false));

        assertTrue(stderr.contains("Error writing value of"),
                "positive control: the value-conversion error log should have fired. Captured: " + stderr);
        assertFalse(stderr.contains(canary),
                "record field value leaked into the ERROR log: " + stderr);
    }

    // F3 - ClickHouseWriter.java:952: the raw insert exception must not be logged (generic path).
    @Test
    public void insertError_doesNotLogRawException() throws Exception {
        ClickHouseWriter writer = spy(new ClickHouseWriter(new SinkTaskStatistics(0)));
        ClickHouseHelperClient chc = mock(ClickHouseHelperClient.class);
        when(chc.isUseClientV2()).thenReturn(true);
        setField(writer, "chc", chc);

        String canary = "CANARY_ROW_secret_payload_value";
        List<Record> records = new ArrayList<>();
        Table table = mock(Table.class);
        QueryIdentifier queryId = new QueryIdentifier("topic", "query-id");

        // Simulate the ClickHouse driver quoting the offending row in its exception message.
        doThrow(new RuntimeException(
                "DB::Exception: Cannot parse input: expected value near '" + canary + "'"))
                .when(writer).doInsertRawBinaryV2(records, table, queryId, false);

        String stderr = captureStderr(() ->
                writer.doInsertRawBinary(records, table, queryId, false, false));

        assertTrue(stderr.contains("Error inserting records"),
                "positive control: the insert-error log should have fired. Captured: " + stderr);
        assertFalse(stderr.contains(canary),
                "raw insert exception (with row value) leaked into the ERROR log: " + stderr);
    }

    // F3 - ClickHouseWriter.java:952 (generic branch): real insert failures arrive async as an
    // ExecutionException wrapping a ClickHouseException. The numeric ClickHouse error code is
    // structural (support triages on it) and must survive, while the driver's free-form message
    // (which can quote the offending row value) must not reach the log.
    @Test
    public void insertError_keepsClickHouseErrorCode_withoutValue() throws Exception {
        ClickHouseWriter writer = spy(new ClickHouseWriter(new SinkTaskStatistics(0)));
        ClickHouseHelperClient chc = mock(ClickHouseHelperClient.class);
        when(chc.isUseClientV2()).thenReturn(true);
        setField(writer, "chc", chc);

        String canary = "CANARY_NULL_ROW_secret_payload_value";
        List<Record> records = new ArrayList<>();
        Table table = mock(Table.class);
        QueryIdentifier queryId = new QueryIdentifier("topic", "query-id");

        // A ClickHouseException carrying error code 349 and a message that quotes the offending
        // row, wrapped in an ExecutionException the way the async driver surfaces insert failures.
        ClickHouseException chException = mock(ClickHouseException.class);
        when(chException.getErrorCode()).thenReturn(349);
        when(chException.getMessage())
                .thenReturn("Code: 349. DB::Exception: Cannot convert NULL value near '" + canary + "'");
        doThrow(new ExecutionException("insert failed", chException))
                .when(writer).doInsertRawBinaryV2(records, table, queryId, false);

        String stderr = captureStderr(() ->
                writer.doInsertRawBinary(records, table, queryId, false, false));

        assertTrue(stderr.contains("ClickHouse error code: 349"),
                "the ClickHouse error code should be preserved for triage. Captured: " + stderr);
        assertFalse(stderr.contains(canary),
                "raw insert exception (with row value) leaked into the ERROR log: " + stderr);
    }
}
