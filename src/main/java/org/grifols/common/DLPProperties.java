package org.grifols.common;

import java.io.Serializable;

import javax.annotation.Nullable;

@SuppressWarnings("serial")
public class DLPProperties implements Serializable {
    private String deidTemplate;
    @Nullable
    private String inspTemplate;
    private int batchSize;
    private String tableName;

    public DLPProperties() {

    }

    public String getDeidTemplate() {
        return deidTemplate;
    }

    public void setDeidTemplate(String deidTemplate) {
        this.deidTemplate = deidTemplate;
    }

    public String getInspTemplate() {
        return inspTemplate;
    }

    public void setInspTemplate(String inspTemplate) {
        this.inspTemplate = inspTemplate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "DLPProperties [deidTemplate=" + deidTemplate + ", inspTemplate=" + inspTemplate + ", batchSize="
                + batchSize + ", tableName=" + tableName + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + batchSize;
        result = prime * result + ((deidTemplate == null) ? 0 : deidTemplate.hashCode());
        result = prime * result + ((inspTemplate == null) ? 0 : inspTemplate.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
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
        DLPProperties other = (DLPProperties) obj;
        if (batchSize != other.batchSize)
            return false;
        if (deidTemplate == null) {
            if (other.deidTemplate != null)
                return false;
        } else if (!deidTemplate.equals(other.deidTemplate))
            return false;
        if (inspTemplate == null) {
            if (other.inspTemplate != null)
                return false;
        } else if (!inspTemplate.equals(other.inspTemplate))
            return false;
        if (tableName == null) {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

}
