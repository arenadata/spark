/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect.service

import java.io.File
import java.nio.file.Files
import java.security.KeyStore
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}

import scala.jdk.CollectionConverters._

import io.grpc.{ManagedChannel, Status, StatusRuntimeException}
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.netty.buffer.ByteBufAllocator
import io.netty.util.ReferenceCountUtil

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, SSLOptions}
import org.apache.spark.connect.proto
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_BINDING_PORT
import org.apache.spark.util.Utils

/**
 * Tests TLS of the Spark Connect GRPC service, configured in the `spark.ssl.connect` namespace or
 * passed to `SparkConnectService.startWithTls`.
 */
class SparkConnectServiceTlsSuite extends SparkFunSuite with LocalSparkContext {

  private val password = "password"

  // The test certificates are issued to CN=Unknown without subject alternative names, so the TLS
  // clients connect to localhost with this authority to pass the host name verification.
  private val certificateAuthority = "Unknown"

  // Cipher suites that HTTP/2 allows with the RSA key of the test key store.
  private val cipherSuite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
  private val otherCipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"

  private lazy val keyStore = copyAndGetResourceFile("keystore", ".jks")
  private lazy val trustStore = copyAndGetResourceFile("truststore", ".jks")
  private lazy val untrustedKeyStore = copyAndGetResourceFile("untrusted-keystore", ".jks")

  override def afterEach(): Unit = {
    try {
      if (SparkConnectService.started) {
        SparkConnectService.stop()
      }
    } finally {
      super.afterEach()
    }
  }

  private def baseConf(): SparkConf = new SparkConf()
    .setAppName(getClass().getName())
    .set(SparkLauncher.SPARK_MASTER, "local[1]")
    .set(CONNECT_GRPC_BINDING_PORT.key, "0")

  private def tlsConf(): SparkConf = baseConf()
    .set("spark.ssl.connect.enabled", "true")
    .set("spark.ssl.connect.keyStore", keyStore.getAbsolutePath)
    .set("spark.ssl.connect.keyStorePassword", password)
    .set("spark.ssl.connect.keyPassword", password)

  private def clientAuthConf(): SparkConf = tlsConf()
    .set("spark.ssl.connect.needClientAuth", "true")
    .set("spark.ssl.connect.trustStore", trustStore.getAbsolutePath)
    .set("spark.ssl.connect.trustStorePassword", password)

  // Starts the service with the spark.ssl.connect options of `conf` and returns its port.
  private def startService(conf: SparkConf): Int = {
    sc = new SparkContext(conf)
    SparkConnectService.start(sc)
    SparkConnectService.localPort
  }

  // Starts the service with the server certificate of the test key store and returns its port.
  private def startServiceWithTls(
      protocols: Seq[String] = Seq.empty,
      cipherSuites: Seq[String] = Seq.empty): Int = {
    if (sc == null) {
      sc = new SparkContext(baseConf())
    }
    SparkConnectService.startWithTls(sc, keyManagerFactory(keyStore), protocols, cipherSuites)
  }

  private def loadKeyStore(file: File): KeyStore = {
    val store = KeyStore.getInstance("JKS")
    Utils.tryWithResource(Files.newInputStream(file.toPath)) { in =>
      store.load(in, password.toCharArray)
    }
    store
  }

  private def keyManagerFactory(file: File): KeyManagerFactory = {
    val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    factory.init(loadKeyStore(file), password.toCharArray)
    factory
  }

  private def tlsChannel(
      port: Int,
      clientKeyStore: Option[File] = None,
      protocol: Option[String] = None,
      clientCipherSuite: Option[String] = None): ManagedChannel = {
    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(loadKeyStore(trustStore))
    val sslContextBuilder = GrpcSslContexts.forClient().trustManager(trustManagerFactory)
    clientKeyStore.foreach(file => sslContextBuilder.keyManager(keyManagerFactory(file)))
    protocol.foreach(p => sslContextBuilder.protocols(p))
    clientCipherSuite.foreach(c => sslContextBuilder.ciphers(Seq(c).asJava))
    NettyChannelBuilder
      .forAddress("localhost", port)
      .sslContext(sslContextBuilder.build())
      .overrideAuthority(certificateAuthority)
      .build()
  }

  private def plaintextChannel(port: Int): ManagedChannel = {
    NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build()
  }

  // Releases a session that does not exist, which needs no SparkSession, then closes the channel.
  private def assertRpcSucceeds(channel: ManagedChannel): Unit = {
    try {
      val sessionId = UUID.randomUUID().toString
      val request = proto.ReleaseSessionRequest
        .newBuilder()
        .setUserContext(proto.UserContext.newBuilder().setUserId("tls-user").build())
        .setSessionId(sessionId)
        .build()
      val response = proto.SparkConnectServiceGrpc
        .newBlockingStub(channel)
        .withDeadlineAfter(1, TimeUnit.MINUTES)
        .releaseSession(request)
      assert(response.getSessionId === sessionId)
    } finally {
      channel.shutdownNow()
    }
  }

  private def assertUnavailable(channel: ManagedChannel): Unit = {
    val e = intercept[StatusRuntimeException] {
      assertRpcSucceeds(channel)
    }
    assert(e.getStatus.getCode === Status.Code.UNAVAILABLE)
  }

  test("spark.ssl.connect: TLS client completes an RPC, plaintext client fails") {
    val port = startService(tlsConf())
    assertRpcSucceeds(tlsChannel(port))
    assertUnavailable(plaintextChannel(port))
  }

  test("spark.ssl.connect: needClientAuth requires a trusted client certificate") {
    val port = startService(clientAuthConf())
    assertUnavailable(tlsChannel(port))
    assertUnavailable(tlsChannel(port, clientKeyStore = Some(untrustedKeyStore)))
    assertRpcSucceeds(tlsChannel(port, clientKeyStore = Some(keyStore)))
  }

  test("spark.ssl.connect: protocol and enabledAlgorithms restrict the TLS handshake") {
    val port = startService(
      tlsConf()
        .set("spark.ssl.connect.protocol", "TLSv1.2")
        .set("spark.ssl.connect.enabledAlgorithms", cipherSuite))
    assertRpcSucceeds(tlsChannel(port, protocol = Some("TLSv1.2")))
    assertUnavailable(tlsChannel(port, protocol = Some("TLSv1.3")))
    assertUnavailable(
      tlsChannel(port, protocol = Some("TLSv1.2"), clientCipherSuite = Some(otherCipherSuite)))
  }

  test("spark.ssl.connect: protocol that is not a protocol version fails the start") {
    // `TLS` is a valid spark.ssl.protocol for the other namespaces, which use it as the
    // SSLContext algorithm.
    val e = intercept[IllegalArgumentException] {
      startService(tlsConf().set("spark.ssl.connect.protocol", "TLS"))
    }
    assert(e.getMessage.contains("spark.ssl.connect.protocol"))
    assert(!SparkConnectService.started)
  }

  test("spark.ssl.connect: enabledAlgorithms without a supported cipher suite fails the start") {
    // SSLOptions keeps unsupported algorithms in tests (SPARK_TESTING), so the service checks them.
    val e = intercept[IllegalArgumentException] {
      startService(tlsConf().set("spark.ssl.connect.enabledAlgorithms", "TLS_NOT_A_CIPHER_SUITE"))
    }
    assert(e.getMessage.contains("spark.ssl.connect.enabledAlgorithms"))
    assert(!SparkConnectService.started)
  }

  test("spark.ssl.connect: missing keyStore fails the start") {
    val keyStorePassword = "key-store-password"
    val e = intercept[IllegalArgumentException] {
      startService(
        baseConf()
          .set("spark.ssl.connect.enabled", "true")
          .set("spark.ssl.connect.keyStorePassword", keyStorePassword))
    }
    assert(e.getMessage.contains("spark.ssl.connect.keyStore"))
    assert(!e.getMessage.contains(keyStorePassword))
    assert(!SparkConnectService.started)
  }

  test("spark.ssl.connect: global spark.ssl.enabled alone keeps Spark Connect in plaintext") {
    val port = startService(
      baseConf()
        .set("spark.ssl.enabled", "true")
        .set("spark.ssl.keyStore", keyStore.getAbsolutePath)
        .set("spark.ssl.keyStorePassword", password)
        .set("spark.ssl.keyPassword", password))
    assertRpcSucceeds(plaintextChannel(port))
    assertUnavailable(tlsChannel(port))
  }

  test("spark.ssl.connect: the service restarts with and without TLS") {
    val tlsPort = startService(tlsConf())
    assertRpcSucceeds(tlsChannel(tlsPort))
    SparkConnectService.stop()

    withSparkEnvConfs("spark.ssl.connect.enabled" -> "false") {
      SparkConnectService.start(sc)
    }
    val plaintextPort = SparkConnectService.localPort
    assertRpcSucceeds(plaintextChannel(plaintextPort))
    assertUnavailable(tlsChannel(plaintextPort))
    SparkConnectService.stop()

    SparkConnectService.start(sc)
    val restartedTlsPort = SparkConnectService.localPort
    assertRpcSucceeds(tlsChannel(restartedTlsPort))
    assertUnavailable(plaintextChannel(restartedTlsPort))
  }

  test("spark.ssl.connect: TLS context follows the protocol, cipher suite and client auth") {
    val sslContext = SparkConnectService.createSslContext(
      SSLOptions(
        enabled = true,
        keyStore = Some(keyStore),
        keyStorePassword = Some(password),
        needClientAuth = true,
        trustStore = Some(trustStore),
        trustStorePassword = Some(password),
        protocol = Some("TLSv1.2"),
        enabledAlgorithms = Set(cipherSuite)))
    assert(sslContext.isServer)
    val engine = sslContext.newEngine(ByteBufAllocator.DEFAULT)
    try {
      assert(engine.getEnabledProtocols.contains("TLSv1.2"))
      assert(!engine.getEnabledProtocols.contains("TLSv1.3"))
      assert(engine.getEnabledCipherSuites.toSeq === Seq(cipherSuite))
      assert(engine.getNeedClientAuth)
    } finally {
      ReferenceCountUtil.release(engine)
    }
  }

  test("startWithTls: TLS client completes an RPC, plaintext client fails") {
    val port = startServiceWithTls()
    assertRpcSucceeds(tlsChannel(port))
    assertUnavailable(plaintextChannel(port))
  }

  test("startWithTls: returns the port that the server is bound to") {
    val port = startServiceWithTls()
    assert(port > 0)
    assert(port === SparkConnectService.localPort)
    assert(port === SparkConnectService.bindingAddress.getPort)
  }

  test("startWithTls: ignores the spark.ssl.connect options") {
    // Read by startWithTls, these options would make it serve plaintext with `enabled` false and
    // fail with `enabled` true.
    sc = new SparkContext(
      baseConf()
        .set("spark.ssl.connect.keyStore", keyStore.getAbsolutePath + ".missing")
        .set("spark.ssl.connect.protocol", "TLS")
        .set("spark.ssl.connect.needClientAuth", "true"))
    Seq("false", "true").foreach { enabled =>
      withSparkEnvConfs("spark.ssl.connect.enabled" -> enabled) {
        val port = startServiceWithTls()
        assertRpcSucceeds(tlsChannel(port))
        assertUnavailable(plaintextChannel(port))
        SparkConnectService.stop()
      }
    }
  }

  test("startWithTls: protocols and cipherSuites restrict the TLS handshake") {
    val port = startServiceWithTls(protocols = Seq("TLSv1.2"), cipherSuites = Seq(cipherSuite))
    assertRpcSucceeds(tlsChannel(port, protocol = Some("TLSv1.2")))
    assertUnavailable(tlsChannel(port, protocol = Some("TLSv1.3")))
    assertUnavailable(
      tlsChannel(port, protocol = Some("TLSv1.2"), clientCipherSuite = Some(otherCipherSuite)))
  }

  test("startWithTls: protocol that is not a protocol version fails the start") {
    val e = intercept[IllegalArgumentException] {
      startServiceWithTls(protocols = Seq("TLSv1.3", "TLS"))
    }
    assert(e.getMessage.contains("protocols"))
    assert(e.getMessage.contains("TLS"))
    assert(!SparkConnectService.started)
  }

  test("startWithTls: cipherSuites without a supported cipher suite fail the start") {
    val e = intercept[IllegalArgumentException] {
      startServiceWithTls(cipherSuites = Seq("TLS_NOT_A_CIPHER_SUITE"))
    }
    assert(e.getMessage.contains("cipherSuites"))
    assert(!SparkConnectService.started)
  }

  test("startWithTls: fails while the service is running") {
    val plaintextPort = startService(baseConf())
    val e = intercept[IllegalStateException] {
      startServiceWithTls()
    }
    assert(e.getMessage.contains("already started"))
    assertRpcSucceeds(plaintextChannel(plaintextPort))
    SparkConnectService.stop()

    val tlsPort = startServiceWithTls()
    intercept[IllegalStateException] {
      startServiceWithTls()
    }
    assertRpcSucceeds(tlsChannel(tlsPort))
  }

  test("startWithTls: start keeps the running service") {
    val port = startServiceWithTls()
    // spark.ssl.connect.enabled is not set, so a new server would serve plaintext.
    SparkConnectService.start(sc)
    assert(SparkConnectService.localPort === port)
    assertRpcSucceeds(tlsChannel(port))
    assertUnavailable(plaintextChannel(port))
  }

  test("startWithTls: failure after the server is bound shuts the server down") {
    sc = new SparkContext(baseConf())
    // The listener of the service cannot be added to a stopped listener bus.
    sc.listenerBus.stop()
    intercept[IllegalStateException] {
      startServiceWithTls()
    }
    assert(!SparkConnectService.started)
    assert(SparkConnectService.server.awaitTermination(1, TimeUnit.MINUTES))
    assertUnavailable(tlsChannel(SparkConnectService.bindingAddress.getPort))
  }

  test("startWithTls: starts again after stop") {
    val port = startServiceWithTls()
    assertRpcSucceeds(tlsChannel(port))
    SparkConnectService.stop()

    val restartedPort = startServiceWithTls()
    assertRpcSucceeds(tlsChannel(restartedPort))
    assertUnavailable(plaintextChannel(restartedPort))
  }
}
