package com.baidu.hugegraph.computer.core.network.netty;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.IllegalArgException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.message.PingMessage;
import com.baidu.hugegraph.computer.core.network.message.PongMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.computer.core.network.session.TransportSession;
import org.apache.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
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

        MessageType msgType = msg.type();

        if (msgType.category() == MessageType.Category.DATA) {
            this.processDataMessage(ctx, channel, (DataMessage) msg);
            return;
        }

        switch (msgType) {
            case START:
                this.processStartMessage(ctx, channel, (StartMessage) msg);
                break;
            case FAIL:
                this.processFailMessage(ctx, channel, (FailMessage) msg);
                break;
            case ACK:
                this.processAckMessage(ctx, channel, (AckMessage) msg);
                break;
            case FINISH:
                this.processFinishMessage(ctx, channel, (FinishMessage) msg);
                break;
            case PING:
                this.processPingMessage(ctx, channel, (PingMessage) msg);
                break;
            case PONG:
                this.processPongMessage(ctx, channel, (PongMessage) msg);
                break;
            default:
                throw new IllegalArgException("Unknown message type: %s",
                                              msgType);
        }
    }

    protected abstract void processStartMessage(ChannelHandlerContext ctx,
                                                Channel channel,
                                                StartMessage startMessage);

    protected abstract void processFinishMessage(ChannelHandlerContext ctx,
                                                 Channel channel,
                                                 FinishMessage finishMessage);

    protected abstract void processDataMessage(ChannelHandlerContext ctx,
                                               Channel channel,
                                               DataMessage dataMessage);

    protected abstract void processAckMessage(ChannelHandlerContext ctx,
                                              Channel channel,
                                              AckMessage ackMessage);

    protected void processPingMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PingMessage pingMessage) {
        ctx.writeAndFlush(PongMessage.INSTANCE)
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    protected void processPongMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PongMessage pongMessage) {
        // No need to deal with the pongMessage, it only for keep-alive
    }

    protected void processFailMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      FailMessage failMessage) {
        int errorCode = failMessage.errorCode();

        TransportException exception = new TransportException(
                                       errorCode,
                                       "Remote error from '%s', cause: %s",
                                       TransportUtil.remoteAddress(channel),
                                       failMessage.message());

        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.transportHandler().exceptionCaught(exception, connectionId);
    }

    @Deprecated
    protected void ackFailMessage(ChannelHandlerContext ctx, int failId,
                                  int errorCode, String message) {
        long timeout = this.session().conf().writeSocketTimeout();
        FailMessage failMessage = new FailMessage(failId, errorCode, message);
        ctx.writeAndFlush(failMessage).awaitUninterruptibly(timeout);
    }

    protected abstract TransportSession session();

    protected abstract TransportHandler transportHandler();
}
