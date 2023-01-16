/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.core.store.file.seqfile;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hugegraph.util.E;

import com.google.common.collect.Lists;

public class ValueFile {

    private static final String SEGMENT_NAME_PREFIX = "value_file_";
    private static final String SEGMENT_NAME_REGEX = SEGMENT_NAME_PREFIX +
                                                     "[0-9]+";
    private static final Pattern FILE_NUM_PATTERN = Pattern.compile("[0-9]+");

    private ValueFile() {
        // pass
    }

    public static List<File> scanSegment(File dir) {
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");

        File[] segments = dir.listFiles((dirName, name) -> {
                            return name.matches(SEGMENT_NAME_REGEX);
                          });
        assert segments != null;

        Arrays.sort(segments,
                    Comparator.comparingInt(ValueFile::idFromSegment));
        return Lists.newArrayList(segments);
    }

    public static File segmentFromId(File dir, int segmentId) {
        E.checkArgument(dir.isDirectory(),
                        "The parameter dir must be a directory");
        String name = SEGMENT_NAME_PREFIX + segmentId;
        return Paths.get(dir.getAbsolutePath(), name).toFile();
    }

    public static long fileLength(File dir) {
        return ValueFile.scanSegment(dir)
                        .stream()
                        .mapToLong(File::length)
                        .sum();
    }

    private static int idFromSegment(File segment) {
        String fileName = segment.getName();
        Matcher matcher = FILE_NUM_PATTERN.matcher(fileName);
        E.checkState(matcher.find(),
                     "Can't get segment id from illegal file name: '%s'",
                     fileName);
        return Integer.parseInt(matcher.group());
    }
}
