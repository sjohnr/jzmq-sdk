package org.zeromq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sender {

  static final byte[] PING = "ping".getBytes();

  public static void main(String[] args) throws InterruptedException {
    final ZMQ.Context ctx = ZMQ.context(1);

    new Thread(new Runnable() {
      @Override
      public void run() {
        byte[] routing = null;
        ZMQ.Poller poller = new ZMQ.Poller(2);
        ZMQ.Socket frontend = createFrontend(ctx);
        ZMQ.Socket backend = createBackend(ctx);

        poller.register(backend, ZMQ.Poller.POLLIN); // 0
        poller.register(frontend, ZMQ.Poller.POLLIN); // 1

        for (; ; ) {
          poller.poll(1000);
          // check backend: whether this is PING or mere reply.
          if (poller.pollin(0)) {
            for (; ; ) {
              // .recv()
              List<byte[]> msg = new ArrayList<byte[]>();
              if (recv(backend, msg) == 0) {
                break;
              }
              // check whether this is PING request.
              if (Arrays.equals(msg.get(2/*PAYLOAD*/), PING)) {
                routing = msg.get(0/*IDENTITY*/);
                System.out.println("Got receiver: " + Arrays.hashCode(routing));
              }
            }
          }
          // check frontend: recv on frontend and send on backend eventually to the receiver.
          if (routing != null && poller.pollin(1)) {
            for (; ; ) {
              // .recv()
              List<byte[]> msg = new ArrayList<byte[]>();
              if (recv(frontend, msg) == 0) {
                break;
              }
              // .send()
              List<byte[]> msgCopy = new ArrayList<byte[]>();
              send(backend, routing, msg, msgCopy);
            }
          }
        }
      }
    }).start();

    Thread.sleep(1000); // wait 1 sec for inproc:// protocol (we are on 3.2.2)

    ZMQ.Socket sender = createSender(ctx);

    for (; ; ) {
      List<byte[]> msg = new ArrayList<byte[]>();
      msg.add(new byte[0]); // empty frame for DEALER
      msg.add(("hello zmq world / timestamp=" + System.currentTimeMillis()).getBytes()); // set PAYLOAD
      int i = 0;
      for (byte[] frame : msg) {
        sender.send(frame, ++i < msg.size() ? ZMQ.SNDMORE : ZMQ.DONTWAIT);
      }
    }
  }

  static ZMQ.Socket createBackend(ZMQ.Context ctx) {
    ZMQ.Socket backend = ctx.socket(ZMQ.ROUTER);
    backend.setSendTimeOut(-1);
    backend.setRouterMandatory(true);
    backend.setRcvHWM(0); // the limit is only processing power.
    backend.bind("tcp://*:6060");
    return backend;
  }

  static ZMQ.Socket createFrontend(ZMQ.Context ctx) {
    ZMQ.Socket frontend = ctx.socket(ZMQ.ROUTER);
    frontend.bind("tcp://*:5050");
    return frontend;
  }

  static ZMQ.Socket createSender(ZMQ.Context ctx) {
    ZMQ.Socket sender = ctx.socket(ZMQ.DEALER);
    sender.setSendTimeOut(-1);
    sender.connect("tcp://localhost:5050");
    return sender;
  }

  static int recv(ZMQ.Socket socket, List<byte[]> msg) {
    for (; ; ) {
      byte[] f = socket.recv(ZMQ.DONTWAIT);
      if (f == null) {
        return 0;
      }
      msg.add(f);
      if (!socket.hasReceiveMore()) {
        return 1;
      }
    }
  }

  static void send(ZMQ.Socket backend, byte[] routing, List<byte[]> msg, List<byte[]> msgCopy) {
    msgCopy.add(routing);
    msgCopy.add(new byte[0]); // empty frame for DEALER
    msgCopy.add(msg.get(2/*PAYLOAD*/));
    int i = 0;
    for (byte[] frame : msgCopy) {
      backend.send(frame, ++i < msgCopy.size() ? ZMQ.SNDMORE : ZMQ.DONTWAIT);
    }
  }
}
