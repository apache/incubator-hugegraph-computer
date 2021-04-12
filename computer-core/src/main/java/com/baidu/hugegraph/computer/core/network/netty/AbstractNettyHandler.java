package com.baidu.hugegraph.computer.core.network.netty;

import static com.baidu.hugegraph.computer.core.network.TransportUtil.remoteConnectionID;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractNettyHandler
       extends SimpleChannelInboundHandler<Message> {

    protected void processFailMessage(ChannelHandlerContext ctx,
                                      FailMessage failMessage,
                                      TransportHandler handler) {
        if (failMessage.hasBody()) {
            try {
                String failMsg = failMessage.failMessage();
                TransportException exception = new TransportException(failMsg);
                ConnectionId connectionID = remoteConnectionID(ctx.channel());
                handler.exceptionCaught(exception, connectionID);
            } finally {
                failMessage.release();
            }
        }
    }
}
