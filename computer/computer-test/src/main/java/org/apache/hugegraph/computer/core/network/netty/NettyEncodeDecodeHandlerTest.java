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

package org.apache.hugegraph.computer.core.network.netty;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hugegraph.computer.core.network.MockUnDecodeMessage;
import org.apache.hugegraph.computer.core.network.buffer.NettyBuffer;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.buffer.NioBuffer;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.network.message.StartMessage;
import org.apache.hugegraph.computer.core.network.netty.codec.FrameDecoder;
import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

public class NettyEncodeDecodeHandlerTest extends AbstractNetworkTest {

    @Override
    protected void initOption() {
    }

    @Test
    public void testSendMsgWithEncoderExceptionMock() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.startSession();

        int requestId = 1;
        int partition = 1;
        byte[] bytes = StringEncodeUtil.encode("mock msg");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        NetworkBuffer body = new NioBuffer(buffer);
        DataMessage dataMessage = new DataMessage(null, requestId,
                                                  partition, body);
        ChannelFutureListenerOnWrite listener =
        new ChannelFutureListenerOnWrite(clientHandler);
        ChannelFutureListenerOnWrite spyListener = Mockito.spy(listener);
        client.channel().writeAndFlush(dataMessage)
              .addListener(spyListener);

        Mockito.verify(clientHandler, Mockito.timeout(3000L).times(1))
               .onChannelActive(Mockito.any());
        Mockito.verify(clientHandler, Mockito.timeout(3000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
        Mockito.verify(spyListener, Mockito.timeout(3000L).times(1))
               .onFailure(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSendMsgWithDecodeException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.channel().writeAndFlush(new MockUnDecodeMessage());

        Mockito.verify(serverHandler, Mockito.timeout(2000L).times(1))
               .onChannelActive(Mockito.any());
        Mockito.verify(serverHandler, Mockito.timeout(2000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSendMsgWithFrameDecode() {
        FrameDecoder frameDecoder = new FrameDecoder();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(frameDecoder);
        NetworkBuffer buffer = new NettyBuffer(Unpooled.buffer());
        ByteBuf buf = buffer.nettyByteBuf();
        StartMessage.INSTANCE.encode(buf);
        boolean writeInbound = embeddedChannel.writeInbound(buf);
        Assert.assertTrue(writeInbound);
        Assert.assertTrue(embeddedChannel.finish());
        buffer.release();
    }

    @Test
    public void testSendMsgWithFrameDecodeMagicError() {
        FrameDecoder frameDecoder = new FrameDecoder();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(frameDecoder);
        NetworkBuffer buffer = new NettyBuffer(Unpooled.buffer());
        short magicError = 10;
        ByteBuf buf = buffer.nettyByteBuf();
        StartMessage.INSTANCE.encode(buf);
        buf.setShort(0, magicError);

        embeddedChannel.writeInbound(buf);
        Assert.assertFalse(embeddedChannel.finish());
        Assert.assertNull(embeddedChannel.readInbound());
    }

    @Test
    public void testSendMsgWithFrameDecodeVersionError() {
        FrameDecoder frameDecoder = new FrameDecoder();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(frameDecoder);
        NetworkBuffer buffer = new NettyBuffer(Unpooled.buffer());
        byte versionError = 10;
        ByteBuf buf = buffer.nettyByteBuf();
        StartMessage.INSTANCE.encode(buf);
        buf.setByte(2, versionError);

        embeddedChannel.writeInbound(buf);
        Assert.assertFalse(embeddedChannel.finish());
        Assert.assertNull(embeddedChannel.readInbound());
    }

    @Test
    public void testClientDecodeException() throws IOException {
        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            UnitTestBase.sleep(200L);
            Channel channel = invocationOnMock.getArgument(0);
            channel.writeAndFlush(new MockUnDecodeMessage());
            return null;
        }).when(serverProtocol).initializeServerPipeline(Mockito.any(),
                                                         Mockito.any());

        this.oneClient();

        Mockito.verify(clientHandler, Mockito.timeout(15_000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testMessageRelease() {
        int requestId = 99;
        int partition = 1;
        byte[] bytes = StringEncodeUtil.encode("mock msg");
        ByteBuf buf = Unpooled.directBuffer().writeBytes(bytes);
        try {
            NettyBuffer managedBuffer = new NettyBuffer(buf);
            DataMessage dataMessage = new DataMessage(MessageType.MSG,
                                                      requestId, partition,
                                                      managedBuffer);
            Assert.assertEquals("DataMessage[messageType=MSG," +
                                "sequenceNumber=99,partition=1,hasBody=true," +
                                "bodyLength=8]", dataMessage.toString());
            Assert.assertEquals(1, managedBuffer.referenceCount());
            dataMessage.release();
            Assert.assertEquals(0, managedBuffer.referenceCount());
        } finally {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
    }
}
