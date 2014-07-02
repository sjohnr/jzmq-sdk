package org.zeromq.messaging.worker;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.service.Processor;
import org.zeromq.messaging.service.Routing;
import org.zeromq.messaging.service.Worker;
import org.zeromq.support.thread.ZmqProcess;

import java.util.UUID;

class WorkerFixture extends BaseFixture {

  WorkerFixture(ZmqContext ctx) {
    super(ctx);
  }

  void master(Props router, Props master, Processor processor) {
    worker(router, master, null, null, new FairRouting(), processor);
  }

  void slave(Props router, Props slave, Processor processor) {
    worker(router, null, slave, new FairRouting(), null, processor);
  }

  void master(Props master, Props router, Routing slaveRouting, Processor processor) {
    worker(router, master, null, null, slaveRouting, processor);
  }

  void slave(Props router, Props slave, Routing masterRouting, Processor processor) {
    worker(router, null, slave, masterRouting, null, processor);
  }

  void worker(Props router, Props master, Props slave, Processor processor) {
    worker(router, master, slave, new FairRouting(), new FairRouting(), processor);
  }

  void worker(Props router,
              Props master,
              Props slave,
              Routing masterRouting,
              Routing slaveRouting,
              Processor processor) {

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
