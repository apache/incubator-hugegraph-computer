/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.network.netty;

import static com.baidu.hugegraph.computer.core.network.TransportUtil.encodeString;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.computer.core.network.MockRequestMessage;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NioManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.message.PingMessage;
import com.baidu.hugegraph.computer.core.network.message.PongMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.computer.core.network.netty.codec.FrameDecoder;
import com.baidu.hugegraph.testutil.Assert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

public class NettyEncodeDecodeHandlerTest extends AbstractNetworkTest {

    @Override
    protected void initOption() {
    }

    @Test
    public void testSendMsgWithMock() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        int requestId = 99;
        int partition = 1;
        ByteBuffer buffer = ByteBuffer.wrap(encodeString("mock msg"));
        ManagedBuffer body = new NioManagedBuffer(buffer);
        DataMessage dataMessage = new DataMessage(MessageType.MSG, requestId,
                                                  partition, body);
        client.channel().writeAndFlush(dataMessage)
              .addListener(new ChannelFutureListenerOnWrite(clientHandler));;

        Mockito.verify(clientHandler, Mockito.times(1))
               .channelActive(client.connectionID());

        Mockito.verify(serverHandler, Mockito.times(1))
               .channelActive(Mockito.any());
    }

    @Test
    public void testSendMsgWithEncoderExceptionMock() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        int requestId = 99;
        int partition = 1;
        ByteBuffer buffer = ByteBuffer.wrap(encodeString("mock msg"));
        ManagedBuffer body = new NioManagedBuffer(buffer);
        DataMessage dataMessage = new DataMessage(null, requestId,
                                                  partition, body);
        client.channel().writeAndFlush(dataMessage)
              .addListener(new ChannelFutureListenerOnWrite(clientHandler));

        Mockito.verify(clientHandler, Mockito.timeout(2000).times(1))
              .channelActive(Mockito.any());
        Mockito.verify(clientHandler, Mockito.timeout(2000).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSendMsgWithDecodeException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.channel().writeAndFlush(new MockRequestMessage());

        Mockito.verify(serverHandler, Mockito.timeout(2000).times(1))
               .channelActive(Mockito.any());
        Mockito.verify(serverHandler, Mockito.timeout(2000).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSendMsgWithFrameDecode() {
        FrameDecoder frameDecoder = new FrameDecoder();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(frameDecoder);
        ManagedBuffer buffer = new NettyManagedBuffer(Unpooled.buffer());
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
        ManagedBuffer buffer = new NettyManagedBuffer(Unpooled.buffer());
        short magicError = 10;
        ByteBuf buf = buffer.nettyByteBuf();
        StartMessage.INSTANCE.encode(buf);
        buf.setShort(0, magicError);

        Assert.assertThrows(Exception.class, () -> {
            embeddedChannel.writeInbound(buf);
        }, e -> {
            Assert.assertContains("received incorrect magic number",
                                  e.getMessage());
        });

        Assert.assertFalse(embeddedChannel.finish());
        buffer.release();
    }

    @Test
    public void testSendMsgWithFrameDecodeVersionError() {
        FrameDecoder frameDecoder = new FrameDecoder();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(frameDecoder);
        ManagedBuffer buffer = new NettyManagedBuffer(Unpooled.buffer());
        byte versionError = 10;
        ByteBuf buf = buffer.nettyByteBuf();
        StartMessage.INSTANCE.encode(buf);
        buf.setByte(2, versionError);

        Assert.assertThrows(Exception.class, () -> {
            embeddedChannel.writeInbound(buf);
        }, e -> {
            Assert.assertContains("received incorrect protocol version",
                                  e.getMessage());
        });

        Assert.assertFalse(embeddedChannel.finish());
        buffer.release();
    }

    @Test
    public void testClientDecodeException() throws Exception {
        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            Channel channel = invocationOnMock.getArgument(0);
            channel.writeAndFlush(new MockRequestMessage());
            return null;
        }).when(serverProtocol).initializeServerPipeline(Mockito.any(),
                                                         Mockito.any());

        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Mockito.verify(clientHandler, Mockito.timeout(2000).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSendOtherMessageType() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        int requestId = 99;
        int partition = 1;
        ByteBuffer buffer = ByteBuffer.wrap(encodeString("mock msg"));
        ManagedBuffer body = new NioManagedBuffer(buffer);
        DataMessage dataMessage = new DataMessage(MessageType.MSG, requestId,
                                                  partition, body);
        client.channel().writeAndFlush(dataMessage);

        client.channel().writeAndFlush(new AckMessage(requestId));

        FailMessage failMsg = FailMessage.createFailMessage(requestId,
                                                            "fail msg");
        client.channel().writeAndFlush(failMsg);

        client.channel().writeAndFlush(PingMessage.INSTANCE);

        client.channel().writeAndFlush(PongMessage.INSTANCE);

        client.channel().writeAndFlush(StartMessage.INSTANCE);

        client.channel().writeAndFlush(new FinishMessage(requestId));
    }
}
