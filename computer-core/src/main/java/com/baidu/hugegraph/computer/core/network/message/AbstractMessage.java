/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.hugegraph.computer.core.network.message;

import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;

import io.netty.buffer.ByteBuf;

/**
 * Abstract class for messages which optionally
 * contain sequenceNumber, partition, body.
 * <p>
 * HugeGraph://
 *
 *    0             2                3                    4
 *    +---------------------------------------------------+
 *  0 | magic 2byte | version 1byte  | message-type 1byte |
 *    +---------------------------------------------------+
 *  4 |                sequence-number 4byte              |
 *    +---------------------------------------------------+
 *  8 |                partition 4byte                    |
 *    +---------------------------------------------------+
 * 12 |                body-length 4byte                  |
 *    +---------------------------------------------------+
 * 16 |               body-content（length: body-length）  |
 *    +---------------------------------------------------+
 * </p>
 */
public abstract class AbstractMessage implements Message {

  // magic(2) version(1) message-type (1) seq(4) partition(4) body-length(4)
  public static final int FRAME_HEADER_LENGTH = 2 + 1 + 1 + 4 + 4 + 4;
  public static final int LENGTH_FIELD_OFFSET = 2 + 1 + 1 + 4 + 4;
  public static final int LENGTH_FIELD_LENGTH = 4;
  // The length of the header part where all messages are the same
  public static final int INIT_FRAME_HEADER_LENGTH = 2 + 1;

  // MAGIC_NUMBER = "HG"
  public static final short MAGIC_NUMBER = 0x4847;
  public static final byte PROTOCOL_VERSION = 1;

  private final int partition;
  private final ManagedBuffer body;
  private final int bodyLength;

  protected AbstractMessage() {
    this(0, null);
  }

  protected AbstractMessage(ManagedBuffer body) {
    this(0, body);
  }

  protected AbstractMessage(int partition,
                            ManagedBuffer body) {
    this.partition = partition;
    if (body != null) {
      this.body = body;
      this.bodyLength = body.size();
    } else {
      this.body = null;
      this.bodyLength = 0;
    }
  }

  @Override
  public void encode(ByteBuf buf) {
    this.type().encode(buf);
    buf.writeInt(this.sequenceNumber());
    buf.writeInt(this.partition());
    if (this.bodyLength() > 0) {
      buf.writeInt(this.bodyLength());
      buf.writeBytes(this.body().nettyByteBuf());
    } else {
      buf.writeInt(0);
    }
  }

  @Override
  public int partition() {
    return this.partition;
  }

  @Override
  public int bodyLength() {
    return this.bodyLength;
  }

  @Override
  public ManagedBuffer body() {
    return this.bodyLength() > 0 ? this.body : null;
  }
}
