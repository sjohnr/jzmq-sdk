package org.zeromq.support;

/**
 * Generic abstraction around operation of transformation something(the source) to something(the target).
 *
 * @param <SOURCE> type of object which plays role of a "source" of transformation.
 * @param <TARGET> type of object which is "target" to which "source" should be adapted.
 */
public interface ObjectAdapter<SOURCE, TARGET> {

  TARGET convert(SOURCE src);
}
