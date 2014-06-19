package org.zeromq.support;

/** Marker interface saying that component which implements it has "destroy" routine. */
public interface HasDestroy {

  void destroy();
}
