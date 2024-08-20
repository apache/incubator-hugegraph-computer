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

package org.apache.hugegraph.computer.core.snapshot;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.ContainerInfo;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.partition.Partitioner;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.network.buffer.FileRegionBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvManager;
import org.apache.hugegraph.computer.core.sender.MessageSendManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;

public class SnapshotManager implements Manager {

    private static final Logger LOG = Log.logger(SnapshotManager.class);
    public static final String NAME = "worker_snapshot";

    private final MessageSendManager sendManager;
    private final MessageRecvManager recvManager;

    private final ContainerInfo workerInfo;
    private final Partitioner partitioner;
    private final int partitionCount;
    private final boolean loadSnapshot;
    private final boolean writeSnapshot;
    private final String snapshotName;

    private MinioClient minioClient;
    private String bucketName;

    public SnapshotManager(ComputerContext context, MessageSendManager sendManager,
                           MessageRecvManager recvManager, ContainerInfo workerInfo) {
        this.loadSnapshot = context.config().get(ComputerOptions.SNAPSHOT_LOAD);
        this.writeSnapshot = context.config().get(ComputerOptions.SNAPSHOT_WRITE);

        this.sendManager = sendManager;
        this.recvManager = recvManager;
        this.recvManager.setSnapshotManager(this);

        this.workerInfo = workerInfo;
        this.partitioner = context.config().createObject(ComputerOptions.WORKER_PARTITIONER);
        this.partitionCount = context.config().get(ComputerOptions.JOB_PARTITIONS_COUNT);
        this.snapshotName = context.config().get(ComputerOptions.SNAPSHOT_NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        String endpoint = config.get(ComputerOptions.SNAPSHOT_MINIO_ENDPOINT);
        this.bucketName = config.get(ComputerOptions.SNAPSHOT_MINIO_BUCKET_NAME);

        if (StringUtils.isNotEmpty(endpoint) && StringUtils.isNotEmpty(this.bucketName)) {
            String accessKey = config.get(ComputerOptions.SNAPSHOT_MINIO_ACCESS_KEY);
            String secretKey = config.get(ComputerOptions.SNAPSHOT_MINIO_SECRET_KEY);
            this.minioClient = MinioClient.builder()
                                          .endpoint(endpoint)
                                          .credentials(accessKey, secretKey)
                                          .build();

            try {
                boolean bucketExist = this.minioClient.bucketExists(
                        BucketExistsArgs.builder()
                                        .bucket(this.bucketName)
                                        .build());
                if (!bucketExist) {
                    this.minioClient.makeBucket(MakeBucketArgs.builder()
                                                              .bucket(this.bucketName)
                                                              .build());
                }
            } catch (Exception e) {
                throw new ComputerException("Failed to initialize bucket %s", this.bucketName, e);
            }
        }
    }

    @Override
    public void close(Config config) {
        // pass
    }

    public boolean loadSnapshot() {
        return this.loadSnapshot;
    }

    public boolean writeSnapshot() {
        return this.writeSnapshot;
    }

    public void upload(MessageType messageType, int partitionId, List<String> outputFiles) {
        if (this.loadSnapshot()) {
            LOG.info("No later {} snapshots have to be uploaded",
                      messageType.name().toLowerCase(Locale.ROOT));
            return;
        }
        this.uploadObjects(messageType, partitionId, outputFiles);
    }

    public void load() {
        int id = this.workerInfo.id();
        for (int partitionId = 0; partitionId < this.partitionCount; partitionId++) {
            if (this.partitioner.workerId(partitionId) == id) {
                // TODO: Do not need to send control message to all workers
                this.sendManager.startSend(MessageType.VERTEX);
                this.downloadObjects(MessageType.VERTEX, partitionId);
                this.sendManager.finishSend(MessageType.VERTEX);

                this.sendManager.startSend(MessageType.EDGE);
                this.downloadObjects(MessageType.EDGE, partitionId);
                this.sendManager.finishSend(MessageType.EDGE);
            }
        }
    }

    private void uploadObjects(MessageType messageType, int partitionId,
                               List<String> outputFiles) {
        String dirName = this.generateObjectDirName(messageType, partitionId);

        try {
            this.clearObjectsIfExist(dirName);
        } catch (Exception e) {
            throw new ComputerException("Failed to clear out-dated snapshots from %s", dirName, e);
        }

        LOG.info("Upload {} snapshots for partition {}",
                  messageType.name().toLowerCase(Locale.ROOT), partitionId);
        for (String outputFile : outputFiles) {
            String objectName = dirName + new File(outputFile).getName();
            this.uploadObject(outputFile, objectName);
        }
    }

    private void downloadObjects(MessageType messageType, int partitionId) {
        LOG.info("Load {} snapshots for partition {}",
                  messageType.name().toLowerCase(Locale.ROOT), partitionId);
        String dirName = this.generateObjectDirName(messageType, partitionId);

        try {
            Iterable<Result<Item>> snapshotFiles = this.minioClient.listObjects(
                    ListObjectsArgs.builder()
                                   .bucket(this.bucketName)
                                   .prefix(dirName)
                                   .build());

            if (!snapshotFiles.iterator().hasNext()) {
                throw new ComputerException("Empty snapshot directory %s", dirName);
            }

            for (Result<Item> result : snapshotFiles) {
                Item item = result.get();
                int size = (int) item.size();
                String objectName = item.objectName();

                String outputPath = this.recvManager.genOutputPath(messageType, partitionId);
                this.downloadObject(objectName, outputPath);

                FileRegionBuffer fileRegionBuffer = new FileRegionBuffer(size, outputPath);
                this.recvManager.handle(messageType, partitionId, fileRegionBuffer);
            }
        } catch (Exception e) {
            throw new ComputerException("Failed to download snapshots from %s", dirName, e);
        }
    }

    private void uploadObject(String fileName, String objectName) {
        try {
            this.minioClient.uploadObject(UploadObjectArgs.builder()
                                                          .bucket(this.bucketName)
                                                          .object(objectName)
                                                          .filename(fileName)
                                                          .build());
        } catch (Exception e) {
            throw new ComputerException("Failed to upload snapshot %s to %s",
                                         fileName, objectName, e);
        }
    }

    private void downloadObject(String objectName, String outputPath) {
        try {
            this.minioClient.downloadObject(DownloadObjectArgs.builder()
                                                              .bucket(this.bucketName)
                                                              .object(objectName)
                                                              .filename(outputPath)
                                                              .build());
        } catch (Exception e) {
            throw new ComputerException("Failed to download snapshot from %s to %s",
                                         objectName, outputPath, e);
        }
    }

    private void clearObjectsIfExist(String dirName) throws Exception {
        List<DeleteObject> objects = new LinkedList<>();
        Iterable<Result<Item>> snapshotFiles = this.minioClient.listObjects(
                ListObjectsArgs.builder()
                               .bucket(this.bucketName)
                               .prefix(dirName)
                               .build());
        if (!snapshotFiles.iterator().hasNext()) {
            return;
        }

        LOG.info("Clear out-dated snapshots from {} first", dirName);
        for (Result<Item> result : snapshotFiles) {
            Item item = result.get();
            objects.add(new DeleteObject(item.objectName()));
        }
        Iterable<Result<DeleteError>> results =
                minioClient.removeObjects(RemoveObjectsArgs.builder()
                                                           .bucket(this.bucketName)
                                                           .objects(objects)
                                                           .build());
        for (Result<DeleteError> result : results) {
            DeleteError error = result.get();
            throw new ComputerException("Failed to delete snapshot %s, error message: %s",
                                         error.objectName(), error.message());
        }
    }

    private String generateObjectDirName(MessageType messageType, int partitionId) {
        // dir name: {SNAPSHOT_NAME}/{PARTITIONER}/{PARTITION_COUNT}/VERTEX/{PARTITION_ID}/
        Path path = Paths.get(this.snapshotName,
                              this.partitioner.getClass().getSimpleName(),
                              String.valueOf(this.partitionCount),
                              messageType.name(),
                              String.valueOf(partitionId));
        return path + "/";
    }
}
