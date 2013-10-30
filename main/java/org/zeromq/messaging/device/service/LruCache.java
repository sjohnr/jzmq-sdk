/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging.device.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.device.ZmqSocketIdentityStorage;
import org.zeromq.support.ZmqUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LruCache implements ZmqSocketIdentityStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LruCache.class);

  private static final int DEFAULT_TTL = 1000;

  private final long ttl;
  private final Comparator<byte[]> identityComparator;

  private Cache<Long, ZmqFrames> _identityCache;

  //// CONSTRUCTORS

  public LruCache() {
    this.ttl = DEFAULT_TTL;
    this.identityComparator = null;
  }

  public LruCache(long ttl) {
    this(ttl, null);
  }

  public LruCache(long ttl, Comparator<byte[]> identityComparator) {
    this.ttl = ttl > 0 ? ttl : DEFAULT_TTL;
    this.identityComparator = identityComparator;
  }

  //// METHODS

  @Override
  public void store(ZmqFrames backendIdentities) {
    ensureInitialized();

    long identityHash = ZmqUtils.makeHash(backendIdentities);
    _identityCache.put(identityHash, backendIdentities);
    LOG.trace("++ socket_identity={}.", identityHash);
  }

  @Override
  public ZmqFrames obtain(ZmqFrames frontendIdentities) throws ZmqException {
    ensureInitialized();

    Set<Long> keys = _identityCache.asMap().keySet();
    ImmutableMap<Long, ZmqFrames> allPresent = _identityCache.getAllPresent(keys);

    if (allPresent.isEmpty()) {
      throw ZmqException.socketIdentityStorageIsEmpty();
    }

    byte[] lastFrontendIdentity = frontendIdentities.getLast();
    Long targetIdentityHash = null;
    ZmqFrames targetIdentity = null;
    for (Map.Entry<Long, ZmqFrames> entry : allPresent.entrySet()) {
      ZmqFrames backendIdentities = entry.getValue();
      byte[] lastBackendIdentity = backendIdentities.getLast();
      if (identityComparator == null ||
          identityComparator.compare(lastFrontendIdentity, lastBackendIdentity) == 0) {
        targetIdentityHash = entry.getKey();
        targetIdentity = backendIdentities;
        break;
      }
    }

    if (targetIdentityHash != null) {
      _identityCache.invalidate(targetIdentityHash);
      LOG.trace("-- socket_identity={}.", targetIdentityHash);
      return targetIdentity;
    }

    throw ZmqException.socketIdentityNotMatched();
  }

  @Override
  public int size() {
    ensureInitialized();
    Set<Long> keys = _identityCache.asMap().keySet();
    return _identityCache.getAllPresent(keys).size();
  }

  private void ensureInitialized() {
    if (_identityCache == null) {
      _identityCache = CacheBuilder.newBuilder()
                                   .concurrencyLevel(1)
                                   .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                                   .maximumSize(Long.MAX_VALUE)
                                   .build();
    }
  }
}
