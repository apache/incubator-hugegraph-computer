package com.baidu.hugegraph.computer.core.network.netty;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.PingMessage;
import com.baidu.hugegraph.computer.core.network.message.PongMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractNettyHandler
       extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = Log.logger(AbstractNettyHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
                                throws Exception {
        Channel channel = ctx.channel();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Receive remote message from '{}', message: {}",
                      TransportUtil.remoteAddress(channel), msg);
        }

        if (msg instanceof StartMessage) {
            this.processStartMessage(ctx, channel, (StartMessage) msg);
        } else if (msg instanceof FinishMessage) {
            this.processFinishMessage(ctx, channel, (FinishMessage) msg);
        } else if (msg instanceof DataMessage) {
            this.processDataMessage(ctx, channel, (DataMessage) msg);
        } else if (msg instanceof AckMessage) {
            this.processAckMessage(ctx, channel, (AckMessage) msg);
        } else if (msg instanceof FailMessage) {
            this.processFailMessage(ctx, channel, (FailMessage) msg);
        } else if (msg instanceof PingMessage) {
            this.processPingMessage(ctx, channel, (PingMessage) msg);
        } else if (msg instanceof PongMessage) {
            this.processPongMessage(ctx, channel, (PongMessage) msg);
        }
    }

    protected void processStartMessage(ChannelHandlerContext ctx,
                                       Channel channel,
                                       StartMessage startMessage) {
    }

    protected void processFinishMessage(ChannelHandlerContext ctx,
                                        Channel channel,
                                        FinishMessage finishMessage) {
    }

    protected void processDataMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      DataMessage dataMessage) {
    }

    protected void processAckMessage(ChannelHandlerContext ctx,
                                     Channel channel,
                                     AckMessage ackMessage) {
    }

    protected void processFailMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      FailMessage failMessage) {
        int errorCode = failMessage.errorCode();
        String msg = failMessage.message();
        TransportException exception = new TransportException(errorCode, msg);
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.transportHandler().exceptionCaught(exception, connectionId);
    }

    protected void processPingMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PingMessage pingMessage) {
        ctx.writeAndFlush(PongMessage.INSTANCE);
    }

    protected void processPongMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PongMessage pongMessage) {
    }

    protected void respondFail(ChannelHandlerContext ctx, int failId,
                               int errorCode, String message) {
    }

    protected abstract TransportHandler transportHandler();
}
