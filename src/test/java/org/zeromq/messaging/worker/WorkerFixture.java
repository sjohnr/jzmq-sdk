package org.zeromq.messaging.worker;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.service.Worker;
import org.zeromq.messaging.service.ZmqProcessor;
import org.zeromq.messaging.service.ZmqRouting;
import org.zeromq.support.thread.ZmqProcess;

import java.util.UUID;

class WorkerFixture extends BaseFixture {

  WorkerFixture(ZmqContext ctx) {
    super(ctx);
  }

  void master(Props master, Props router, ZmqRouting slaveRouting, ZmqProcessor processor) {
    worker(master, null, router, null, slaveRouting, processor);
  }

  void slave(Props slave, Props router, ZmqRouting masterRouting, ZmqProcessor processor) {
    worker(null, slave, router, masterRouting, null, processor);
  }

  void worker(Props master,
              Props slave,
              Props router,
              ZmqRouting masterRouting,
              ZmqRouting slaveRouting,
              ZmqProcessor processor) {

    Worker.Builder builder = Worker.builder();
    if (master != null) {
      byte[] id = ("" + UUID.randomUUID().getMostSignificantBits()).getBytes();
      builder.withMaster(Props.builder(master).withIdentity(id).build());
    }
    if (slave != null) {
      byte[] id = ("" + UUID.randomUUID().getMostSignificantBits()).getBytes();
      builder.withSlave(Props.builder(slave).withIdentity(id).build());
    }

    with(ZmqProcess.builder()
                   .with(builder.with(ctx)
                                .withPollTimeout(100)
                                .withRouter(router)
                                .withMasterRouting(masterRouting)
                                .withSlaveRouting(slaveRouting)
                                .with(processor)
                                .build()
                   )
                   .build()
    );
  }
}
