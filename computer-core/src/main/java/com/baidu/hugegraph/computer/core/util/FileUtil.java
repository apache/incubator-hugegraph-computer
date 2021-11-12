package com.baidu.hugegraph.computer.core.util;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class FileUtil {

    public static void deleteFilesQuietly(List<String> files) {
        deleteFilesQuietly(files.toArray(new String[0]));
    }

    public static void deleteFilesQuietly(String... files) {
        for (String file : files) {
            FileUtils.deleteQuietly(new File(file));
        }
    }
}
