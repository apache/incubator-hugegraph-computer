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

package org.apache.hugegraph.computer.core.network.buffer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.network.TransportUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.FileDescriptor;

public class FileRegionBuffer implements NetworkBuffer {

    private final int length;
    private String path;

    public FileRegionBuffer(int length) {
        this.length = length;
    }

    public FileRegionBuffer(int length, String path) {
        this.length = length;
        this.path = path;
    }

    /**
     * Use zero-copy transform from socket channel to file
     * @param channel
     * @param targetPath
     * @return channelFuture
     */
    public ChannelFuture transformFromChannel(SocketChannel channel,
                                              String targetPath) {
        assert channel.eventLoop().inEventLoop();
        ChannelPromise channelPromise = channel.newPromise();
        try {
            if (channel instanceof EpollSocketChannel) {
                // Use splice zero-copy if io mode is epoll
                FileDescriptor fd = FileDescriptor.from(targetPath);
                try {
                    ((EpollSocketChannel) channel).spliceTo(fd, 0,
                                                            this.length,
                                                            channelPromise);
                    channelPromise.addListener(future -> fd.close());
                } catch (Throwable throwable) {
                    fd.close();
                    throw throwable;
                }
            } else {
                // Use memory map zero-copy if io mode is not epoll
                try (RandomAccessFile file = new RandomAccessFile(targetPath,
                                                                  Constants.FILE_MODE_WRITE)) {
                    FileChannel fileChannel = file.getChannel();
                    NioSocketChannel nioChannel = (NioSocketChannel) channel;
                    ReadableByteChannel javaChannel = (ReadableByteChannel)
                                                      nioChannel.unsafe().ch();
                    fileChannel.transferFrom(javaChannel, 0, this.length);
                    channelPromise.setSuccess();
                    fileChannel.close();
                }
            }
            this.path = targetPath;
        } catch (Throwable throwable) {
            channelPromise.setFailure(throwable);
            throw new ComputerException(
                  "Failed to transform from socket to file, " +
                  "targetPath:%s, remoteAddress:%s",
                  throwable, targetPath, TransportUtil.remoteAddress(channel));
        }
        return channelPromise;
    }

    public String path() {
        return this.path;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public NetworkBuffer retain() {
        return this;
    }

    @Override
    public NetworkBuffer release() {
        return this;
    }

    @Override
    public int referenceCount() {
        return -1;
    }

    @Override
    public ByteBuffer nioByteBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf nettyByteBuf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] copyToByteArray() {
        throw new UnsupportedOperationException();
    }
}
