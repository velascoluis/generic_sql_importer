package org.grifols.common;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class TableToDbRowFnOrcl extends DoFn<orclTable, KV<orclTable, List<DbRow>>> {

    private static final Logger LOG = LoggerFactory.getLogger(TableToDbRowFnOrcl.class);
    public static TupleTag<KV<orclTable, List<DbRow>>> successTag = new TupleTag<KV<orclTable, List<DbRow>>>() {
    };
    public static TupleTag<String> deadLetterTag = new TupleTag<String>() {
    };
    private ValueProvider<String> connectionString;
    private ValueProvider<String> jdbcUser;
    private ValueProvider<String> jdbcPass;
    private int batchSize;
    private Connection connection;
    private int tableCount;
    private ValueProvider<Integer> offsetCount;

    public TableToDbRowFnOrcl(ValueProvider<String> connectionString, ValueProvider<String> jdbcUser, ValueProvider<String> jdbcPass,  ValueProvider<Integer> offsetCount) {
        this.connectionString = connectionString;
        this.jdbcUser = jdbcUser;
        this.jdbcPass = jdbcPass;
        this.batchSize = 0;
        this.connection = null;
        this.tableCount = 0;
        this.offsetCount = offsetCount;

    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) throws SQLException {

        orclTable orclTable = c.element();
        List<orclColumn> columnNames = orclTable.getColumnList();
        Statement statement = null;
        List<DbRow> rows = new ArrayList<>();

        for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {

            LOG.info("i value: F"+Float.toString(i));
            LOG.info("Started Restriction From: {}, To: {} ", tracker.currentRestriction().getFrom(),
                    tracker.currentRestriction().getTo());
            if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
                try {

                    String primaryKey = ServerUtil.findPrimaryKeyOrcl(columnNames);
                    if (primaryKey != null) {
                        statement = connection.createStatement();



                        /*
                        Working version:
                           String query = String.format(
                                "SELECT * FROM %s.%s ORDER BY %s",
                                orclTable.getSchema(), orclTable.getName(), primaryKey);


                        */

                        String query = String.format(
                                "SELECT * FROM (SELECT T.*, ROW_NUMBER() OVER (ORDER BY %s) RNK FROM %s.%s T) WHERE RNK BETWEEN (%d * (%d - 1) + 1) AND (%d * (%d - 1)) + %d",
                                primaryKey, orclTable.getSchema(), orclTable.getName(),this.offsetCount.get(), i, this.offsetCount.get(), i, this.offsetCount.get());

                        LOG.info("Executing query: " + query);

                        statement.executeQuery(query);
                        ResultSet rs = statement.executeQuery(query);
                        LOG.info("Statement executed");
                        ResultSetMetaData meta = rs.getMetaData();
                        int columnCount = meta.getColumnCount();
                        LOG.info("columnCount"+ Integer.toString(columnCount));
                        columnCount--;
                        while (rs.next()) {
                            List<Object> values = new ArrayList<Object>();
                            for (int colNumber = 1; colNumber <= columnCount; ++colNumber) {
                                orclColumn column = columnNames.get(colNumber - 1);
                                String columnName = column.getName();
                                Object value = rs.getObject(columnName);
                                values.add(value);
                            }

                            DbRow row = DbRow.create(values);
                            rows.add(row);

                        }
                        KV<orclTable, List<DbRow>> kv = KV.of(orclTable, rows);
                        c.output(kv);

                    }

                } catch (SQLException e) {

                    if (statement != null) {
                        statement.close();
                    }
                    LOG.error("***ERROR** Unable to query table {}", e.getMessage());
                    c.output(deadLetterTag, e.toString());

                }

            }

            else {
                // mock object
                List<Object> values = new ArrayList<Object>();
                values.add("myname");
                values.add("10");
                DbRow row = DbRow.create(values);
                rows.add(row);
                c.output(KV.of(orclTable, rows));
            }
            LOG.info("Successfully emit result for Restriction {} Table {}", i, orclTable.getFullName());

        }

    }

    @StartBundle
    public void startBundle() throws SQLException {

        if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
            this.connection = ServerUtil.getConnection(this.connectionString.get(), this.jdbcUser.get(),this.jdbcPass.get());
        }

    }

    @FinishBundle
    public void finishBundle() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(orclTable table)
            throws IOException, GeneralSecurityException, SQLException {

        if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
            startBundle();
            this.tableCount = ServerUtil.getRowCount(this.connection, null, table.getName());
            this.batchSize = (int) Math.ceil(tableCount / (float) this.offsetCount.get());
            LOG.info("****Table Name {} **** Total Number of Split**** {} **** Total row count {} **** ",
                    table.getFullName(), batchSize, tableCount);
            return new OffsetRange(1, batchSize + 1);
        } else {
            return new OffsetRange(1, 2);

        }
    }

    @SplitRestriction
    public void splitRestriction(orclTable table, OffsetRange range, OutputReceiver<OffsetRange> out) {
        for (final OffsetRange p : range.split(1, 1)) {
            out.output(p);

        }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(OffsetRange range) {
        return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));

    }

}

