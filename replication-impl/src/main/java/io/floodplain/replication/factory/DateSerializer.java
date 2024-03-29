/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.replication.factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

public class DateSerializer {
    public static final String DATEFORMATTER = "yyyy-MM-dd";
    private static final Logger logger = LoggerFactory.getLogger(DateSerializer.class);

    public static final String DATETIMEFORMATTERMICRO = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIMEFORMATTERMILLI = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATETIMEFORMATTERMILLIZONE = "yyyy-MM-dd HH:mm:ss.SSSXXXXX";
    public static final String SHORTDATETIMEFORMATTER = "yyyy-MM-dd HH:mm:ss.SS";
    public static final String CLOCKTIMEFORMATTER = "HH:mm:ss";
    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATEFORMATTER); //10
    public static final DateTimeFormatter dateTimeFormatterMicro = DateTimeFormatter.ofPattern(DATETIMEFORMATTERMICRO); //26
    public static final DateTimeFormatter dateTimeFormatterMilli = DateTimeFormatter.ofPattern(DATETIMEFORMATTERMILLI);
    public static final DateTimeFormatter shortDateTimeFormatter = DateTimeFormatter.ofPattern(SHORTDATETIMEFORMATTER); //22
    public static final DateTimeFormatter clocktimeFormatter =  DateTimeFormatter.ofPattern(CLOCKTIMEFORMATTER);
    public static final DateTimeFormatter zonedTimeFormatter =  DateTimeFormatter.ofPattern(DATETIMEFORMATTERMILLIZONE);

    public static Temporal parseTimeObject(String value) {
        if(value==null) {
            return null;
        }
        if(value.length()>10 && value.charAt(10)=='T') {
            return Instant.parse(value);
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
        } else if(value.length() > DATETIMEFORMATTERMILLI.length()) {
            try {
                return ZonedDateTime.parse(value,zonedTimeFormatter);
            } catch (DateTimeException e) {
                logger.error("Invalid length of temporal value: "+value,e);
                return null;
            }
        } else {
            logger.error("Invalid length of temporal value: "+value);
            return null;
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
        }
        if(val instanceof ZonedDateTime) {
            return zonedTimeFormatter.format(val);
        } else {
            throw new RuntimeException("Invalid temporal type: "+val);
        }
    }
}
