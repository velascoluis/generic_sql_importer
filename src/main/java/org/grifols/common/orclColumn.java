package org.grifols.common;


import java.io.Serializable;

@SuppressWarnings("serial")
public class orclColumn implements Serializable {
    private String name;
    private String defaultValue;
    private boolean nullable;
    private String dataType;
    private boolean primaryKey;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }



    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setPrimaryKey(boolean nullable) {
        this.primaryKey = nullable;
    }

    public boolean getPrimaryKey() {

        return this.primaryKey;
    }

    @Override
    public String toString() {
        return "orclColumn [name=" + name + ", dataType=" + dataType + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof orclColumn))
            return false;
        orclColumn other = (orclColumn) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

}