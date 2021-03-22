package ch.psi.daq.retrieval.subnodes;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;

class ChanInit extends ChannelInitializer<SocketChannel> {

    ChanInit(RawClient client) {
        this.client = client;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        //ch.pipeline().addLast(new IdleStateHandler(200, 200, 200));
        //ch.pipeline().addLast(new ReadTimeoutHandler(200));
        ch.pipeline().addLast(new ChanHandler(client));
    }

    RawClient client;

}
