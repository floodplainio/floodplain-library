package io.floodplain.replication.factory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

public class DateSerializer {
    public static final String DATEFORMATTER = "yyyy-MM-dd";

    public static final String DATETIMEFORMATTERMICRO = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIMEFORMATTERMILLI = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String SHORTDATETIMEFORMATTER = "yyyy-MM-dd HH:mm:ss.SS";
    public static final String CLOCKTIMEFORMATTER = "HH:mm:ss";
    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATEFORMATTER); //10
    public static final DateTimeFormatter dateTimeFormatterMicro = DateTimeFormatter.ofPattern(DATETIMEFORMATTERMICRO); //26
    public static final DateTimeFormatter dateTimeFormatterMilli = DateTimeFormatter.ofPattern(DATETIMEFORMATTERMILLI);
    public static final DateTimeFormatter shortDateTimeFormatter = DateTimeFormatter.ofPattern(SHORTDATETIMEFORMATTER); //22
    public static final DateTimeFormatter clocktimeFormatter =  DateTimeFormatter.ofPattern(CLOCKTIMEFORMATTER);

    public static Temporal parseTimeObject(String value) {
        if(value==null) {
            return null;
        }
        if(DATEFORMATTER.length() == value.length()) {
            return LocalDate.parse(value,dateFormatter);
        } else if(CLOCKTIMEFORMATTER.length() == value.length()) {
            return LocalTime.parse(value,clocktimeFormatter);
        } else if(SHORTDATETIMEFORMATTER.length() == value.length()) {
            return LocalDateTime.parse(value,shortDateTimeFormatter);
        } else if(DATETIMEFORMATTERMICRO.length() == value.length()) {
            return LocalDateTime.parse(value, dateTimeFormatterMicro);
        } else if(DATETIMEFORMATTERMILLI.length() == value.length()) {
            return LocalDateTime.parse(value, dateTimeFormatterMilli);
        } else {
            throw new RuntimeException("Invalid length of temporal value: "+value);
        }
    }

    public static String serializeTimeObject(Temporal val) {
        if(val instanceof LocalDateTime) {
            return dateTimeFormatterMicro.format(val);
        }
        if(val instanceof LocalDate) {
            return dateFormatter.format(val);
        }
        if(val instanceof LocalTime) {
            return clocktimeFormatter.format(val);
        } else {
            throw new RuntimeException("Invalid temporal type: "+val);
        }
    }
}
