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
package org.apache.spark.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * A {@code spark.ui.filters} filter that authenticates the <b>whole</b> Spark Web UI
 * with SPNEGO/Kerberos ({@code type=kerberos}) or pseudo ({@code type=simple}), by
 * exposing Hadoop's
 * {@code org.apache.hadoop.security.authentication.server.AuthenticationFilter} under
 * a stable Spark class name.
 *
 * <p>Its sole purpose is <b>configuration parity with Spark 4</b>: the same
 * {@code spark.ui.filters=org.apache.spark.filter.AuthenticationFilter} value and the
 * same {@code spark.org.apache.spark.filter.AuthenticationFilter.param.*} keys work on
 * Spark 3.5 and Spark 4 alike (and are shorter than naming Hadoop's class directly).
 *
 * <p>On Spark 3.5 the UI is {@code javax.servlet} and Hadoop's AuthenticationFilter is
 * a real {@code javax.servlet.Filter}, so this is a <b>thin passthrough</b> that just
 * forwards {@code init}/{@code doFilter}/{@code destroy} to it - there is none of the
 * jakarta-to-servlet bridging the Spark 4 filter of the same name needs. The delegate
 * is created reflectively so this class compiles against the shaded
 * {@code hadoop-client-api} on the build classpath (whose AuthenticationFilter
 * implements a relocated servlet API); at runtime the unshaded {@code hadoop-auth}
 * supplies the concrete filter. Filter init params are forwarded unchanged, so
 * Hadoop's filter sees {@code type}, {@code kerberos.principal}, {@code kerberos.keytab}
 * and friends.
 *
 * <p>Authentication only - pair it with UI TLS ({@code spark.ssl.ui.enabled=true}, or
 * {@code spark.ssl.historyServer.*} on the History Server). To apply a different policy
 * to the Prometheus {@code /metrics/*} endpoints (for a scraper that cannot do
 * Kerberos), use {@link MetricsAuthFilter} instead, which embeds this filter.
 */
public class AuthenticationFilter implements Filter {

  private static final String HADOOP_FILTER =
      "org.apache.hadoop.security.authentication.server.AuthenticationFilter";

  private Filter delegate;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    delegate = createDelegate();
    delegate.init(filterConfig); // params forwarded unchanged
  }

  /**
   * Hadoop's javax {@code AuthenticationFilter}, loaded reflectively to avoid a
   * compile-time reference to the shaded {@code hadoop-client-api} type (whose
   * AuthenticationFilter implements a relocated {@code javax.servlet.Filter} and so
   * cannot be cast to the real one). At runtime the unshaded {@code hadoop-auth}
   * supplies a real {@code javax.servlet.Filter}. Package-private and overridable so
   * tests can inject a stub without Kerberos/Hadoop.
   */
  Filter createDelegate() throws ServletException {
    try {
      return (Filter) Class.forName(HADOOP_FILTER).getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new ServletException(
          "Hadoop AuthenticationFilter (" + HADOOP_FILTER + ") is not on the classpath", e);
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    delegate.doFilter(request, response, chain);
  }

  @Override
  public void destroy() {
    if (delegate != null) {
      delegate.destroy();
      delegate = null;
    }
  }
}
