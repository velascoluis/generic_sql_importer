package org.grifols.common;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;

import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class BigQueryTableRowDoFnOrcl extends DoFn<KV<orclTable, DbRow>, KV<orclTable, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowDoFnOrcl.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        orclTable orclTable = c.element().getKey();
        DbRow dbRow = c.element().getValue();
        List<orclColumn> columnNames = orclTable.getColumnList();
        List<Object> fields = dbRow.fields();
        TableRow bqRow = new TableRow();
        for (int i = 0; i < fields.size(); i++) {
            Object fieldData = fields.get(i);
            if (fieldData == null)
                continue;

            orclColumn column = columnNames.get(i);
            String columnName = column.getName();

            String sFieldData = fieldData.toString();

            if (column.getDataType().equalsIgnoreCase(orclDataType.BLOB.toString())) {
                byte[] bytesEncoded = Base64.encodeBase64(sFieldData.getBytes());
                bqRow.put(columnName, bytesEncoded);
                continue;
            }
            //Oracle adds 00:00:00.0, BQ doest support that on DATE type, so we trail that, executing a Stringreplacement
            if (column.getDataType().equalsIgnoreCase(orclDataType.DATE.toString())) {

                bqRow.put(columnName, sFieldData.replaceAll(" 00:00:00.0",""));
                LOG.debug("Processing: "+columnName+" with data:"+sFieldData.replaceAll(" 00:00:00.0","")+" of type:"+column.getDataType());
                continue;
            }

            if (!sFieldData.toLowerCase().equals("null"))
                bqRow.put(columnName, sFieldData);
            LOG.debug("Processing: "+columnName+" with data:"+sFieldData+" of type:"+column.getDataType());
        }

        c.output(KV.of(orclTable, bqRow));

    }
}
