/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.test.avro;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.avro.ProtocolSerializer;
import org.apache.reef.wake.avro.ProtocolSerializerNamespace;
import org.apache.reef.wake.impl.MultiObserverImpl;
import org.apache.reef.wake.remote.*;
import org.apache.reef.wake.test.avro.message.AvroTestMessage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

/**
 *  Verify the protocol serializer can serialize and deserialize messages
 *  exchanged between two remote manager classes.
 */
public final class ProtocolSerializerTest {

  private static final Logger LOG = Logger.getLogger(ProtocolSerializerTest.class.getName());

  @Rule
  public final TestName name = new TestName();

  private RemoteManagerFactory remoteManagerFactory;
  private ProtocolSerializer serializer;

  @Before
  public void setup() throws InjectionException {

    final Tang tang = Tang.Factory.getTang();

    final Configuration config = tang.newConfigurationBuilder()
        .bindNamedParameter(ProtocolSerializerNamespace.class, "org.apache.reef.wake.test.avro.message")
        .build();

    final Injector injector = tang.newInjector(config);

    remoteManagerFactory = injector.getInstance(RemoteManagerFactory.class);
    serializer = injector.getInstance(ProtocolSerializer.class);
  }

  /**
   * Verify Avro message can be serialized and deserialized
   * between two remote managers.
   */
  @Test
  public void testProtocolSerializerTest() throws InterruptedException {

    final int[] numbers = {12, 25};
    final String[] strings = {"The first string", "The second string"};

    // Queues for storing messages byte messages.
    final BlockingQueue<byte[]> queue1 = new LinkedBlockingQueue<>();
    final BlockingQueue<byte[]> queue2 = new LinkedBlockingQueue<>();

    // Remote managers for sending and receiving byte messages.
    final RemoteManager remoteManager1 = remoteManagerFactory.getInstance("RemoteManagerOne");
    final RemoteManager remoteManager2 = remoteManagerFactory.getInstance("RemoteManagerTwo");

    // Register message handlers for byte level messages.
    remoteManager1.registerHandler(byte[].class, new ByteMessageObserver(queue1));
    remoteManager2.registerHandler(byte[].class, new ByteMessageObserver(queue2));

    final EventHandler<byte[]> sender1 = remoteManager1.getHandler(remoteManager2.getMyIdentifier(), byte[].class);
    final EventHandler<byte[]> sender2 = remoteManager2.getHandler(remoteManager1.getMyIdentifier(), byte[].class);

    sender1.onNext(serializer.write(new AvroTestMessage(numbers[0], strings[0]), 1));
    sender2.onNext(serializer.write(new AvroTestMessage(numbers[1], strings[1]), 2));

    final AvroMessageObserver avroObserver1 = new AvroMessageObserver();
    final AvroMessageObserver avroObserver2 = new AvroMessageObserver();

    serializer.read(queue1.take(), avroObserver1);
    serializer.read(queue2.take(), avroObserver2);

    assertEquals(numbers[0], avroObserver2.getNumber());
    assertEquals(strings[0], avroObserver2.getDataString());

    assertEquals(numbers[1], avroObserver1.getNumber());
    assertEquals(strings[1], avroObserver1.getDataString());
  }

  private final class ByteMessageObserver implements EventHandler<RemoteMessage<byte[]>> {

    private final BlockingQueue<byte[]> queue;

    /**
     * @param queue Queue where incoming messages will be stored.
     */
    ByteMessageObserver(final BlockingQueue<byte[]> queue) {
      this.queue = queue;
    }

    /**
     * Deserialize and direct incoming messages to the registered MuiltiObserver event handler.
     * @param message A RemoteMessage<byte[]> object which will be deserialized.
     */
    public void onNext(final RemoteMessage<byte[]> message) {
      queue.add(message.getMessage());
    }
  }

  /**
   * Processes messages from the network remote manager.
   */
  public final class AvroMessageObserver extends MultiObserverImpl {

    private int number;
    private String dataString;

    // Accessors
    int getNumber() {
      return number;
    }

    String getDataString() {
      return dataString;
    }

    /**
     * Processes protocol messages from the C# side of the bridge.
     * @param identifier A long value which is the unique message identifier.
     * @param message A reference to the received avro test message.
     */
    public void onNext(final long identifier, final AvroTestMessage message) {
      number = message.getNumber();
      dataString = message.getData().toString();
    }
  }
}
