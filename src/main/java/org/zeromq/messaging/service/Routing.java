package org.zeromq.messaging.service;

public interface Routing {

  void put(byte[] identity, byte[] payload);

  byte[] get(byte[] identity, byte[] payload);

  int available();
}
