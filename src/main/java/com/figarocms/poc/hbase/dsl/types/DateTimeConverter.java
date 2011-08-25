package com.figarocms.poc.hbase.dsl.types;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.nearinfinity.hbase.dsl.types.TypeConverter;

public class DateTimeConverter implements TypeConverter<DateTime> {

    public Class<?>[] getTypes() {
        return new Class[] { DateTime.class };
    }

    public byte[] toBytes(DateTime t) {
        return Bytes.toBytes(t.toString());
    }

    public DateTime fromBytes(byte[] t) {
        return ISODateTimeFormat.dateTime().parseDateTime(Bytes.toString(t));

    }

}
