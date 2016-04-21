/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.test.mock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import com.alpine.plugin.core.io.OperatorInfo;
import scala.Tuple2;
import scala.collection.JavaConversions;

import com.alpine.plugin.core.OperatorParameters;
import com.google.gson.internal.LinkedTreeMap;
import scala.collection.mutable.Map;

/**
 * Plugin 1.0 operator parameters.
 */
public class OperatorParametersMock implements OperatorParameters, Serializable {

    private static final String SPARK_PARAM = "sparkSettings";

    private HashMap<String, Object> parameterMap;

    private final String operatorName;
    private final String operatorUUID;

    public OperatorParametersMock(String operatorName, String operatorUUID) {
        this.operatorName = operatorName;
        this.operatorUUID = operatorUUID;
        this.parameterMap = new HashMap<>();
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(operatorUUID, operatorName);
    }

    public boolean contains(String parameterId) {
        return this.parameterMap.containsKey(parameterId);
    }

    public void setValue(String parameterId, Object value) {
        this.parameterMap.put(parameterId, value);
    }

    public Object getValue(String parameterId) {
        return this.parameterMap.get(parameterId);
    }

    public String[] getStringArrayValue(String parameterId) {
        Object o = this.parameterMap.get(parameterId);
        if (o instanceof ArrayList) {
            ArrayList<String> arrayList = (ArrayList<String>) o;
            String[] vs = new String[arrayList.size()];
            arrayList.toArray(vs);
            return vs;
        } else {
            throw new AssertionError(
                    "An array parameter value must be stored in memory as an " +
                            "array list."
            );
        }
    }

    public Tuple2<String, String[]> getTabularDatasetSelectedColumns(String parameterId) {
        // The hashmap or the linkedmap in this case is used as a pair.
        // I.e., there's only one key and one value
        // (which happens to be an arraylist).
        Object o = this.parameterMap.get(parameterId);
        if (o instanceof LinkedTreeMap) {
            LinkedTreeMap<String, ArrayList<String>> value =
                    (LinkedTreeMap<String, ArrayList<String>>) o;
            String key = value.keySet().iterator().next();
            ArrayList<String> arrayList = value.get(key);
            String[] vs = new String[arrayList.size()];
            arrayList.toArray(vs);
            return new Tuple2<>(key, vs);
        } else {
            HashMap<String, ArrayList<String>> value =
                    (HashMap<String, ArrayList<String>>) o;
            String key = value.keySet().iterator().next();
            ArrayList<String> arrayList = value.get(key);
            String[] vs = new String[arrayList.size()];
            arrayList.toArray(vs);
            return new Tuple2<>(key, vs);
        }
    }

    public Tuple2<String, String> getTabularDatasetSelectedColumn(String parameterId) {
        // The hashmap or the linkedmap in this case is used as a pair.
        // I.e., there's only one key and one value.
        Object o = this.parameterMap.get(parameterId);
        if (o instanceof LinkedTreeMap) {
            LinkedTreeMap<String, String> value =
                    (LinkedTreeMap<String, String>) o;
            String key = value.keySet().iterator().next();
            String v = value.get(key);
            return new Tuple2<>(key, v);
        } else {
            HashMap<String, String> value =
                    (HashMap<String, String>) o;
            String key = value.keySet().iterator().next();
            String v = value.get(key);
            return new Tuple2<>(key, v);
        }
    }

    public String getStringValue(String parameterId) {
        return this.parameterMap.get(parameterId).toString();
    }

    public int getIntValue(String parameterId) {
        Object parameterValue = this.parameterMap.get(parameterId);
        if (parameterValue instanceof Double) {
            // Gson might parse an integer value as a double.
            return ((Double) parameterValue).intValue();
        } else {
            return (Integer) parameterValue;
        }
    }

    public double getDoubleValue(String parameterId) {
        Object parameterValue = this.parameterMap.get(parameterId);
        if (parameterValue instanceof Integer) {
            return ((Integer) parameterValue).doubleValue();
        } else {
            return (Double) parameterValue;
        }
    }

    public scala.collection.Iterator<String> getParameterIds() {
        return JavaConversions.asScalaIterator(
                this.parameterMap.keySet().iterator()
        );
    }

    @Override
    public Map<String, String> getAdvancedSparkParameters() {
        return new scala.collection.mutable.HashMap<String, String>();
    }

    @Override
    public boolean equals(Object operatorParameters) {
        return operatorParameters != null
                && operatorParameters instanceof OperatorParametersMock
                && this.parameterMap.equals(((OperatorParametersMock) operatorParameters).parameterMap);
    }
}
