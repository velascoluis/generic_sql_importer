package org.grifols.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@SuppressWarnings("serial")
@DefaultCoder(SerializableCoder.class)
public class orclTable implements Serializable {
    private String schema;
    private String name;
    private long key;
    @Nullable
    private DLPProperties dlpConfig;
    private List<orclColumn> columnList;

    public orclTable(long key) {
        this.key = key;
        dlpConfig = null;
        columnList = new ArrayList<>();
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFullName() {
        return (getSchema() != null && getSchema().length() > 0 ? getSchema() + "_" + getName() : getName());
    }

    public DLPProperties getDlpConfig() {
        return dlpConfig;
    }

    public void setDlpConfig(DLPProperties dlpConfig) {
        this.dlpConfig = dlpConfig;
    }

    public List<orclColumn> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<orclColumn> columnList) {
        this.columnList = columnList;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnList == null) ? 0 : columnList.hashCode());
        result = prime * result + ((dlpConfig == null) ? 0 : dlpConfig.hashCode());
        result = prime * result + (int) (key ^ (key >>> 32));
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        orclTable other = (orclTable) obj;
        if (columnList == null) {
            if (other.columnList != null)
                return false;
        } else if (!columnList.equals(other.columnList))
            return false;
        if (dlpConfig == null) {
            if (other.dlpConfig != null)
                return false;
        } else if (!dlpConfig.equals(other.dlpConfig))
            return false;
        if (key != other.key)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "orclTable [schema=" + schema + ", name=" + name +", key=" + key + ", dlpConfig="
                + dlpConfig + ", columnList=" + columnList + "]";
    }

}