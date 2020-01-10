package org.grifols.common;


import java.util.HashMap;

//Oracle 11.2 datatypes

public enum orclDataType {
    BYTE(0), CHAR(1), NCHAR(2),  VARCHAR2(3), NVARCHAR2(4), NUMBER(5), FLOAT(6), LONG(7), DATE(8), BINARY_FLOAT(9),
    BINARY_DOUBLE(10), TIMESTAMP(11), RAW(12), LONG_RAW(13), ROWID(14), UROWID(15), CLOB(16), NCLOB(17), BLOB(18), BFILE(19);

    private int codeValue;

    private static HashMap<Integer, orclDataType> codeValueMap = new HashMap<Integer, orclDataType>();

    private orclDataType(int codeValue) {
        this.codeValue = codeValue;
    }

    static {
        for (orclDataType type : orclDataType.values()) {
            codeValueMap.put(type.codeValue, type);
        }
    }

    public static orclDataType getInstanceFromCodeValue(int codeValue) {
        return codeValueMap.get(codeValue);
    }

    public int getCodeValue() {
        return codeValue;
    }
}