/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.test.mock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import com.alpine.plugin.core.dialog.ChorusFile;
import com.alpine.plugin.core.io.OperatorInfo;
import com.alpine.plugin.core.utils.ChorusAPICaller;
import scala.Tuple2;
import scala.collection.JavaConversions;

import com.alpine.plugin.core.OperatorParameters;
import scala.collection.mutable.Map;

/**
 * Plugin 1.0 operator parameters.
 */
public class OperatorParametersMock implements OperatorParameters, Serializable {

    private HashMap<String, Object> parameterMap;

    private final String operatorName;
    private final String operatorUUID;
    public String sessionId; //I am not sure if this is useful to expose
    public ChorusAPICallerMock chorusAPICaller;

    public OperatorParametersMock(String operatorName, String uuid, ChorusAPICallerMock apiCallerMock) {
        this.operatorName = operatorName;
        this.operatorUUID = uuid;
        this.sessionId = Math.random() + "";
        this.chorusAPICaller = apiCallerMock;
        this.parameterMap = new HashMap<>();
    }

    public OperatorParametersMock(String operatorName, String operatorUUID) {
        this.operatorName = operatorName;
        this.operatorUUID = operatorUUID;
        this.parameterMap = new HashMap<>();
        this.sessionId = Math.random() + "";
        this.chorusAPICaller = ChorusAPICallerMock.apply();
    }

    public OperatorParametersMock updateChorusAPICaller(ChorusAPICallerMock newAPICaller) {
        this.chorusAPICaller = newAPICaller;
        return this;
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

    @Override
    public ChorusAPICaller getChorusAPICaller() {
        return chorusAPICaller;
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
        } else if (o instanceof java.util.Map) {
            return getTabularDatasetSelectedColumnNames(parameterId);
        } else {
            throw new AssertionError(
                    "An array parameter value must be stored in memory as an " +
                            "array list or a hashmap."
            );
        }
    }

    public Tuple2<String, String[]> getTabularDatasetSelectedColumns(String parameterId) {
        // The hashmap or the linkedmap in this case is used as a pair.
        // I.e., there's only one key and one value
        // (which happens to be an arraylist).
        Object o = this.parameterMap.get(parameterId);
        if (o instanceof java.util.Map) {
            java.util.Map<String, ArrayList<String>> value =
                    (java.util.Map<String, ArrayList<String>>) o;
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

    public String[] getTabularDatasetSelectedColumnNames(String parameterId) {
        return getTabularDatasetSelectedColumns(parameterId)._2();
    }

    public Tuple2<String, String> getTabularDatasetSelectedColumn(String parameterId) {
        // The hashmap or the linkedmap in this case is used as a pair.
        // I.e., there's only one key and one value.
        Object o = this.parameterMap.get(parameterId);
        if (o instanceof java.util.Map) {
            java.util.Map<String, String> value =
                    (java.util.Map<String, String>) o;
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

    @Override
    public ChorusFile getChorusFile(String parameterId) {
        //return type is the same as getTabularDatasetSelectedColumn
        return (ChorusFile) this.parameterMap.get(parameterId);
    }

    public String getTabularDatasetSelectedColumnName(String parameterId) {
        return getTabularDatasetSelectedColumn(parameterId)._2();
    }

    public String getStringValue(String parameterId) {
        Object param = parameterMap.get(parameterId);
        if (param instanceof java.util.Map) {
            //Is a column selector
            return getTabularDatasetSelectedColumnName(parameterId);
        } else if (param instanceof ChorusFile) {
            return getChorusFile(parameterId).fileId();
        } else {
            return param.toString();
        }
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
        return new scala.collection.mutable.HashMap<>();
    }

    @Override
    public boolean equals(Object operatorParameters) {
        return operatorParameters != null
                && operatorParameters instanceof OperatorParametersMock
                && this.parameterMap.equals(((OperatorParametersMock) operatorParameters).parameterMap);
    }
}
