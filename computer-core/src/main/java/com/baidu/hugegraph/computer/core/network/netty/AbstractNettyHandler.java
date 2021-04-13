package com.baidu.hugegraph.computer.core.network.netty;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractNettyHandler
       extends SimpleChannelInboundHandler<Message> {

    protected void processFailMessage(ChannelHandlerContext ctx,
                                      FailMessage failMessage,
                                      TransportHandler handler) {
        int errorCode = failMessage.errorCode();
        String msg = failMessage.message();
        TransportException exception = new TransportException(errorCode, msg);
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        handler.exceptionCaught(exception, connectionId);
    }
}
