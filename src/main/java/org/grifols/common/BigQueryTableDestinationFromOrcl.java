package org.grifols.common;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryTableDestinationFromOrcl extends DynamicDestinations<KV<orclTable, TableRow>, KV<String, orclTable>> {

    private static final long serialVersionUID = -2929752032929427146L;

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableDestinationFromOrcl.class);

    private ValueProvider<String> datasetName;

    public BigQueryTableDestinationFromOrcl(ValueProvider<String> datasetName) {
        this.datasetName = datasetName;

    }

    @Override
    public KV<String, orclTable> getDestination(ValueInSingleWindow<KV<orclTable, TableRow>> element) {
        KV<orclTable, TableRow> kv = element.getValue();
        orclTable table = kv.getKey();
        String key = datasetName.get() + "." + table.getFullName();
        return KV.of(key, table);

    }

    @Override
    public TableDestination getTable(KV<String, orclTable> value) {
        LOG.debug("Table Defination:{}", value.getKey());
        return new TableDestination(value.getKey(), "DB Import Table");
    }

    @Override
    public TableSchema getSchema(KV<String, orclTable> value) {

        TableSchema tableSchema = null;
        try {

            tableSchema = ServerUtil.getBigQuerySchemaFromOrcl(value.getValue().getColumnList());
            LOG.debug("***Table Schema {}", tableSchema.toString());

        } catch (Exception e) {
            LOG.error("***ERROR*** {} Unable to create dynamic schema", e.toString());
            throw new RuntimeException(e);
        }
        return tableSchema;

    }

}