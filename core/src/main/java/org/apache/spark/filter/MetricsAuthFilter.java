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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Collections;
import java.util.List;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * {@code spark.ui.filters} filter that applies DIFFERENT policies to the
 * Spark Prometheus metrics endpoints ({@code /metrics/*}) and to the rest of the
 * Spark UI - something Spark config alone cannot do (filters, ACLs and the TLS
 * connector are all UI-wide). It supports three independent, composable pieces:
 *
 * <ol>
 *   <li><b>Metrics auth</b> - {@code /metrics/*} is guarded by a shared bearer
 *       token. If the {@code token} param is OMITTED, metrics are left OPEN (the
 *       "no token" mode) - useful when the scraper cannot present credentials but
 *       the rest of the UI still must be protected.</li>
 *   <li><b>UI auth via SPNEGO/pseudo</b> - when the {@code spnego.type} param is
 *       present, every NON-metrics path is delegated to the built-in
 *       {@link AuthenticationFilter} (this fork's jakarta-compatible Hadoop
 *       AuthenticationFilter: {@code type=kerberos} for SPNEGO, {@code simple}
 *       for pseudo). The metrics path NEVER hits SPNEGO, so vmagent/Prometheus
 *       (which cannot do Kerberos) can still scrape.</li>
 *   <li><b>Fall-through</b> - with neither param set for a given path, the
 *       request passes to Spark's own filters/ACLs unchanged.</li>
 * </ol>
 *
 * <p>WHY delegate instead of just listing both filters in {@code spark.ui.filters}:
 * every filter in that list runs on every request, so a separately-listed
 * {@code AuthenticationFilter} would still challenge {@code /metrics/*} and block
 * the scraper. Embedding it and dispatching by path is the only way to exempt
 * metrics. Register ONLY this filter in {@code spark.ui.filters}, not
 * {@code AuthenticationFilter} as well.
 *
 * <p>If you do NOT need path-scoping (i.e. you are happy protecting the WHOLE UI,
 * metrics included), skip this class and use the built-in
 * {@code org.apache.spark.ui.JWSFilter} (bearer) or
 * {@code org.apache.spark.filter.AuthenticationFilter} (SPNEGO) directly.
 *
 * <h3>Modes (combinations of the two params)</h3>
 * <pre>
 *   token set,   no spnego  -> metrics bearer-guarded; UI untouched (Spark ACLs)
 *   token unset, no spnego  -> metrics OPEN;           UI untouched
 *   token set,   spnego set -> metrics bearer-guarded; UI SPNEGO
 *   token unset, spnego set -> metrics OPEN;           UI SPNEGO   (common case:
 *                              open scrape, Kerberos for humans)
 * </pre>
 *
 * <h3>Wire-up (driver / Spark Connect, port 4040)</h3>
 * This class ships in {@code spark-core}, so it is already on the driver classpath -
 * no extra jar is needed. Register it and configure its params via
 * {@code spark.<fully-qualified-class>.param.<name>}:
 * <pre>
 * --conf spark.ui.filters=org.apache.spark.filter.MetricsAuthFilter
 * # metrics token - OMIT this line for open metrics:
 * --conf spark.org.apache.spark.filter.MetricsAuthFilter.param.token=&lt;shared-secret&gt;
 * # optional SPNEGO for the rest of the UI (params carry a 'spnego.' prefix):
 * --conf spark.org.apache.spark.filter.MetricsAuthFilter.param.spnego.type=kerberos
 * --conf spark.org.apache.spark.filter.MetricsAuthFilter.param.spnego.kerberos.\
 *   principal=HTTP/_HOST@REALM
 * --conf spark.org.apache.spark.filter.MetricsAuthFilter.param.spnego.kerberos.\
 *   keytab=/etc/keytabs/spnego.keytab
 * # confidentiality: also enable TLS so the token is not sent in cleartext
 * --conf spark.ssl.ui.enabled=true
 * </pre>
 * On the History Server the registration is identical, but TLS is
 * {@code spark.ssl.historyServer.*} instead of {@code spark.ssl.ui.*}.
 *
 * <h3>Scraper side (vmagent / Prometheus)</h3>
 * <pre>
 * authorization:            # only when a token is configured; drop it for open metrics
 *   type: Bearer
 *   credentials_file: /etc/scrape-auth/token
 * </pre>
 *
 * <p>NOTE: authentication only - not encryption. Pair with TLS so the bearer
 * token and the metrics travel encrypted.
 */
public class MetricsAuthFilter implements Filter {

  /** Path segment that identifies the Spark metrics endpoints. */
  private static final String METRICS_PREFIX = "/metrics/";

  /** Init-param prefix carrying the embedded AuthenticationFilter's config. */
  private static final String SPNEGO_PREFIX = "spnego.";

  /** null => metrics are OPEN (no token required). */
  private byte[] expectedToken;

  /** null => non-metrics paths fall through untouched (no SPNEGO). */
  private Filter uiDelegate;

  @Override
  public void init(FilterConfig cfg) throws ServletException {
    // Fed by spark.<fully-qualified-filter-class>.param.token - exactly what
    // JettyUtils.addFilters reads via conf.getAllWithPrefix("spark.$filter.param.").
    // Absent/empty token => open metrics.
    String token = cfg.getInitParameter("token");
    this.expectedToken = (token == null || token.isEmpty())
        ? null
        : token.getBytes(StandardCharsets.UTF_8);

    // Optionally protect the rest of the UI with the built-in SPNEGO/pseudo
    // filter. Enabled by presence of a 'spnego.type' param.
    if (cfg.getInitParameter(SPNEGO_PREFIX + "type") != null) {
      Filter delegate = createUiDelegate();
      delegate.init(new PrefixedFilterConfig(cfg, SPNEGO_PREFIX));
      this.uiDelegate = delegate;
    }
  }

  /**
   * The built-in filter that non-metrics UI paths are delegated to. On this
   * (Spark 4.2.x) build it is the fork's jakarta {@link AuthenticationFilter}.
   * Package-private and overridable so tests can inject a stub without Kerberos.
   */
  Filter createUiDelegate() {
    return new AuthenticationFilter();
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    HttpServletResponse resp = (HttpServletResponse) response;

    String uri = req.getRequestURI();
    if (uri != null && uri.contains(METRICS_PREFIX)) {
      // Metrics path: bearer token OR open (no-token mode). Never SPNEGO, so the
      // scraper is not challenged with Kerberos.
      if (expectedToken != null && !bearerMatches(req)) {
        resp.setHeader("WWW-Authenticate", "Bearer");
        resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid or missing bearer token");
        return; // short-circuit
      }
      chain.doFilter(request, response); // straight to the metrics servlet
      return;
    }

    // Non-metrics UI path: SPNEGO if configured, else fall through to Spark.
    if (uiDelegate != null) {
      uiDelegate.doFilter(request, response, chain); // authenticates, then continues chain
    } else {
      chain.doFilter(request, response);
    }
  }

  private boolean bearerMatches(HttpServletRequest req) {
    String auth = req.getHeader("Authorization");
    if (auth == null || !auth.startsWith("Bearer ")) {
      return false;
    }
    // Constant-time compare so the token cannot be recovered via response timing.
    byte[] presented = auth.substring(7).getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(presented, expectedToken);
  }

  @Override
  public void destroy() {
    if (uiDelegate != null) {
      uiDelegate.destroy();
      uiDelegate = null;
    }
  }

  /**
   * A {@link FilterConfig} view that exposes only the init params starting with a
   * given prefix, with the prefix stripped - so the embedded
   * {@link AuthenticationFilter} sees {@code type}, {@code kerberos.principal},
   * ... rather than {@code spnego.type}, {@code spnego.kerberos.principal}, and
   * never sees our own {@code token}.
   */
  private static final class PrefixedFilterConfig implements FilterConfig {
    private final FilterConfig delegate;
    private final String prefix;

    PrefixedFilterConfig(FilterConfig delegate, String prefix) {
      this.delegate = delegate;
      this.prefix = prefix;
    }

    @Override
    public String getFilterName() {
      return delegate.getFilterName();
    }

    @Override
    public ServletContext getServletContext() {
      return delegate.getServletContext();
    }

    @Override
    public String getInitParameter(String name) {
      return delegate.getInitParameter(prefix + name);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      List<String> stripped = new ArrayList<>();
      Enumeration<String> all = delegate.getInitParameterNames();
      while (all.hasMoreElements()) {
        String name = all.nextElement();
        if (name.startsWith(prefix)) {
          stripped.add(name.substring(prefix.length()));
        }
      }
      return Collections.enumeration(stripped);
    }
  }
}
