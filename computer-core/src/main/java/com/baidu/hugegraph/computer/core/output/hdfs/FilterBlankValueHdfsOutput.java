package com.baidu.hugegraph.computer.core.output.hdfs;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class FilterBlankValueHdfsOutput extends HdfsOutput {

    private static final String EMPTY_COLLECTION = "[]";

    @Override
    protected boolean filter(Vertex vertex) {
        String value;
        return vertex.value() != null &&
               StringUtils.isNotEmpty(value = vertex.value().string()) &&
               !EMPTY_COLLECTION.equals(value);
    }
}
