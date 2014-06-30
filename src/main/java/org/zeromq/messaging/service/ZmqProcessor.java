package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

public interface ZmqProcessor {

  void onMessage(ZmqFrames route,
                 byte[] payload,
                 ZmqChannel router,
                 ZmqRouting masterRouting,
                 ZmqRouting slaveRouting,
                 byte[] masterIdentity,
                 byte[] slaveIdentity) throws Exception;

  void onMasterMessage(ZmqFrames route,
                       byte[] payload,
                       ZmqChannel router,
                       ZmqRouting masterRouting,
                       ZmqRouting slaveRouting,
                       byte[] masterIdentity,
                       byte[] slaveIdentity) throws Exception;

  void onSlaveMessage(ZmqFrames route,
                      byte[] payload,
                      ZmqChannel router,
                      ZmqRouting masterRouting,
                      ZmqRouting slaveRouting,
                      byte[] masterIdentity,
                      byte[] slaveIdentity) throws Exception;
}
