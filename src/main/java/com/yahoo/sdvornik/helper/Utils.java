package com.yahoo.sdvornik.helper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    private static final Pattern pattern = Pattern.compile("([^{},']+)", Pattern.MULTILINE);

    public static final int NULL_INT = Integer.MAX_VALUE;

    public static final long NULL_LONG  = Long.MAX_VALUE;

    public static int half_round(double value) {
        return (int)Math.floor(value);
    }


    public static String[] splitString(String parsedStr) {
        Matcher m = pattern.matcher(parsedStr);
        List<String> list = new ArrayList<>();
        while(m.find()) {
            list.add(m.group(0));
        }

        return list.toArray(new String[list.size()]);
    }

    public static int getInt(ResultSet rs, String fieldName) throws SQLException {
        int intValue = rs.getInt(fieldName);
        return rs.wasNull() ? Integer.MAX_VALUE : intValue;
    }

    public static long getLong(ResultSet rs, String fieldName) throws SQLException {
        int longValue = rs.getInt(fieldName);
        return rs.wasNull() ? Long.MAX_VALUE : longValue;
    }

    public static double getDouble(ResultSet rs, String fieldName) throws SQLException {
        double doubleValue = rs.getDouble(fieldName);
        return rs.wasNull() ? Double.MAX_VALUE : doubleValue;
    }
}
