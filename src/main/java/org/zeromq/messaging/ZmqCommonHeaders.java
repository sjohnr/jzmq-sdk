package org.zeromq.messaging;

/** Common ZMQ message headers. */
public final class ZmqCommonHeaders {

  public static final String ZMQ_MESSAGE_TYPE_PING = "PING";

  public static enum Header {
    ZMQ_MESSAGE_TYPE("zmq_msg_type");

    private final String id;
    private final byte[] bin;

    private Header(String id) {
      this.id = id;
      this.bin = id.getBytes();
    }

    public String id() {
      return id;
    }

    public byte[] bin() {
      return bin;
    }
  }

  private ZmqCommonHeaders() {
  }
}
