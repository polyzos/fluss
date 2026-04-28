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

import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import HomepageIntroduce from '@site/src/components/HomepageIntroduce';
import {useEffect, useRef} from 'react';

import styles from './index.module.css';

/**
 * Toggle a body-level class while the hero is in view, so we can drive the
 * navbar's transparent → solid transition entirely from CSS, without timing
 * tricks or pixel-based scroll thresholds. Works on any viewport size.
 */
function useHeroVisibilityClass(ref: React.RefObject<HTMLElement>) {
    useEffect(() => {
        const el = ref.current;
        if (!el || typeof window === 'undefined') return;
        // Mark as on-hero immediately on mount so the navbar starts transparent.
        document.body.classList.add('fluss-on-hero');

        const observer = new IntersectionObserver(
            ([entry]) => {
                if (entry.isIntersecting) {
                    document.body.classList.add('fluss-on-hero');
                } else {
                    document.body.classList.remove('fluss-on-hero');
                }
            },
            {
                // Trigger when the hero has scrolled out far enough that
                // ~64px (one navbar height) of it remains under the navbar.
                rootMargin: '-64px 0px 0px 0px',
                threshold: 0,
            },
        );
        observer.observe(el);
        return () => {
            observer.disconnect();
            document.body.classList.remove('fluss-on-hero');
        };
    }, [ref]);
}

const SLACK_INVITE =
    'https://join.slack.com/t/apache-fluss/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw';

function HeroDiagram() {
    // Inline SVG illustrating: producers → Fluss (log + PK + lakehouse) → Flink/Spark/Iceberg
    // Layout uses a 620×480 grid with three columns:
    //   - Producers      x=20 → 145   (width 125)
    //   - Fluss core     x=215 → 450  (width 235)
    //   - Consumers      x=475 → 615  (width 140)
    return (
        <svg
            viewBox="0 0 620 480"
            xmlns="http://www.w3.org/2000/svg"
            role="img"
            aria-labelledby="heroDiagramTitle heroDiagramDesc">
            <title id="heroDiagramTitle">Apache Fluss data flow</title>
            <desc id="heroDiagramDesc">
                Producers stream events into Apache Fluss columnar storage. Apache
                Flink and SQL engines like Spark query the data with sub-second
                freshness; Fluss tiers data natively to Apache Iceberg, Apache
                Paimon, and Lance.
            </desc>

            <defs>
                <linearGradient id="hgFlussFill" x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0%" stopColor="#1A47B8" />
                    <stop offset="100%" stopColor="#0B1E47" />
                </linearGradient>
                <linearGradient id="hgFlow" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#22D3EE" stopOpacity="0" />
                    <stop offset="50%" stopColor="#22D3EE" stopOpacity="1" />
                    <stop offset="100%" stopColor="#22D3EE" stopOpacity="0" />
                </linearGradient>
                <filter id="hgGlow" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur stdDeviation="6" result="b" />
                    <feMerge>
                        <feMergeNode in="b" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>
            </defs>

            {/* ----- Producers (left column) ----- */}
            <g>
                {[80, 160, 240].map((y, i) => (
                    <g key={i}>
                        <rect
                            x="20" y={y} width="125" height="50"
                            rx="10"
                            fill="rgba(255,255,255,0.06)"
                            stroke="rgba(255,255,255,0.18)"
                        />
                        <circle cx="42" cy={y + 25} r="4" fill="#22D3EE" />
                        <text x="60" y={y + 30}
                              fill="rgba(219,234,254,0.92)"
                              fontFamily="Inter, sans-serif"
                              fontSize="14"
                              fontWeight="500">
                            {['CDC', 'Events', 'IoT'][i]}
                        </text>
                    </g>
                ))}
                <text x="20" y="320"
                      fill="rgba(147,197,253,0.7)"
                      fontFamily="Inter, sans-serif"
                      fontSize="10"
                      fontWeight="700"
                      letterSpacing="0.1em">
                    PRODUCERS
                </text>
            </g>

            {/* ----- Flow lines: producers → Fluss ----- */}
            <g stroke="url(#hgFlow)" strokeWidth="2" fill="none">
                <path d="M145 105 C 180 105, 195 165, 215 192" />
                <path d="M145 185 C 185 185, 200 220, 215 246" />
                <path d="M145 265 C 185 265, 200 295, 215 311" />
            </g>

            {/* ----- Fluss core (center column) ----- */}
            <g>
                <rect
                    x="215" y="80" width="235" height="320"
                    rx="22"
                    fill="url(#hgFlussFill)"
                    stroke="rgba(34,211,238,0.4)"
                    strokeWidth="1.5"
                    filter="url(#hgGlow)"
                />
                <text x="332" y="125"
                      textAnchor="middle"
                      fill="#FFFFFF"
                      fontFamily="Inter, sans-serif"
                      fontSize="17"
                      fontWeight="700"
                      letterSpacing="-0.005em">
                    Apache Fluss
                </text>
                <text x="332" y="148"
                      textAnchor="middle"
                      fill="rgba(147,197,253,0.85)"
                      fontFamily="Inter, sans-serif"
                      fontSize="9.5"
                      fontWeight="600"
                      letterSpacing="0.14em">
                    COLUMNAR STREAMING STORAGE
                </text>

                {/* Internal lanes */}
                <g>
                    {/* Append-only Log */}
                    <rect x="232" y="170" width="200" height="44" rx="10"
                          fill="rgba(34,211,238,0.12)"
                          stroke="rgba(34,211,238,0.3)" />
                    <text x="332" y="197"
                          textAnchor="middle"
                          fill="#FFFFFF"
                          fontFamily="Inter, sans-serif"
                          fontSize="13"
                          fontWeight="600">
                        Append-only Log
                    </text>

                    {/* Primary-Key Table */}
                    <rect x="232" y="224" width="200" height="44" rx="10"
                          fill="rgba(124,58,237,0.18)"
                          stroke="rgba(147,197,253,0.3)" />
                    <text x="332" y="251"
                          textAnchor="middle"
                          fill="#FFFFFF"
                          fontFamily="Inter, sans-serif"
                          fontSize="13"
                          fontWeight="600">
                        Primary-Key Table
                    </text>

                    {/* Lakehouse-Native Storage (taller, two-line) */}
                    <rect x="232" y="278" width="200" height="66" rx="10"
                          fill="rgba(255,255,255,0.06)"
                          stroke="rgba(255,255,255,0.16)" />
                    <text x="332" y="302"
                          textAnchor="middle"
                          fill="#FFFFFF"
                          fontFamily="Inter, sans-serif"
                          fontSize="12"
                          fontWeight="600">
                        Lakehouse-Native Storage
                    </text>
                    <text x="332" y="325"
                          textAnchor="middle"
                          fill="rgba(147,197,253,0.85)"
                          fontFamily="Inter, sans-serif"
                          fontSize="10"
                          fontWeight="500"
                          letterSpacing="0.04em">
                        Iceberg · Paimon · Lance
                    </text>
                </g>
            </g>

            {/* ----- Flow lines: Fluss → consumers ----- */}
            <g stroke="url(#hgFlow)" strokeWidth="2" fill="none">
                <path d="M450 192 C 460 192, 465 150, 475 118" />
                <path d="M450 246 C 460 246, 465 235, 475 228" />
                <path d="M450 311 C 460 311, 465 325, 475 338" />
            </g>

            {/* ----- Consumers (right column) ----- */}
            <g>
                {[
                    {y: 90, label: 'Apache Flink'},
                    {y: 200, label: 'Apache Spark'},
                    {y: 310, label: 'Iceberg · Paimon'},
                ].map((c, i) => (
                    <g key={i}>
                        <rect
                            x="475" y={c.y} width="140" height="56"
                            rx="10"
                            fill="rgba(255,255,255,0.06)"
                            stroke="rgba(255,255,255,0.18)"
                        />
                        <circle cx="498" cy={c.y + 28} r="4" fill="#A3E635" />
                        <text x="516" y={c.y + 33}
                              fill="rgba(219,234,254,0.92)"
                              fontFamily="Inter, sans-serif"
                              fontSize="13"
                              fontWeight="500">
                            {c.label}
                        </text>
                    </g>
                ))}
                <text x="475" y="395"
                      fill="rgba(147,197,253,0.7)"
                      fontFamily="Inter, sans-serif"
                      fontSize="10"
                      fontWeight="700"
                      letterSpacing="0.1em">
                    CONSUMERS / LAKE
                </text>
            </g>
        </svg>
    );
}

function HomepageHeader({heroRef}: {heroRef: React.RefObject<HTMLElement>}) {
    return (
        <header ref={heroRef} className={styles.heroBanner}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.heroInner}>
                    <div>
                        <span className={styles.heroEyebrow}>
                            <span className={styles.dot} />
                            Apache Software Foundation · Incubating · Apache 2.0
                        </span>

                        <h1 className={styles.heroTitle}>
                            Streaming Storage for{' '}
                            <span className={styles.accent}>Real-Time Analytics &amp; AI</span>
                        </h1>

                        <p className={styles.heroSubtitle}>
                            Apache Fluss (Incubating) is a streaming storage built
                            for real-time analytics &amp; AI which can serve as the
                            real-time data layer for Lakehouse architectures. With
                            its columnar stream and real-time update capabilities,
                            Fluss integrates seamlessly with Apache Flink to enable
                            high-throughput, low-latency, cost-effective streaming
                            data warehouses tailored for real-time applications.
                        </p>

                        <div className={styles.heroCtas}>
                            <Link
                                className={styles.btnPrimary}
                                to="/docs/quickstart/flink">
                                Get Started
                                <span aria-hidden="true">→</span>
                            </Link>

                            <Link
                                className={styles.btnSecondary}
                                to="https://github.com/apache/fluss">
                                <img
                                    src="img/github_icon.svg"
                                    alt=""
                                    aria-hidden="true"
                                    className={styles.btnIcon}
                                />
                                View on GitHub
                            </Link>

                            <Link
                                className={styles.btnGhost}
                                to="/docs/quickstart/flink">
                                Read the Docs ↗
                            </Link>
                        </div>
                    </div>

                    <div className={styles.heroDiagram} aria-hidden="false">
                        <HeroDiagram />
                    </div>
                </div>

                <div className={styles.trustStrip} role="region" aria-label="Project trust signals">
                    <span className={styles.trustItem}>
                        <strong>Apache 2.0</strong> licensed
                    </span>
                    <span className={styles.trustDivider} aria-hidden="true" />
                    <span className={styles.trustItem}>
                        Built with <strong>Apache Flink</strong>, <strong>Iceberg</strong>, <strong>Paimon</strong>
                    </span>
                    <span className={styles.trustDivider} aria-hidden="true" />
                    <span className={styles.trustItem}>
                        <strong>Open governance</strong> at the ASF
                    </span>
                    <span className={styles.trustDivider} aria-hidden="true" />
                    <span className={styles.trustItem}>
                        <Link
                            to="https://github.com/apache/fluss/releases"
                            style={{color: 'inherit', textDecoration: 'underline'}}>
                            View latest release →
                        </Link>
                    </span>
                </div>
            </div>
        </header>
    );
}

function SystemsTaxSection() {
    const beforeStack = [
        {
            label: 'Message broker',
            sub: 'Kafka, for streaming event transport.',
        },
        {
            label: 'Stream processor',
            sub: 'Flink or Spark, for derived features and aggregations.',
        },
        {
            label: 'Online store',
            sub: 'Redis or DynamoDB, for sub-millisecond feature lookup.',
        },
        {
            label: 'Offline store',
            sub: 'Iceberg or Parquet on S3, for training and history.',
        },
        {
            label: 'Sync layer',
            sub: 'Bespoke pipelines + freshness monitors that drift silently.',
        },
    ];

    const afterRequirements = [
        'Event log: durable, replayable, offset-ordered.',
        'KV store: sub-millisecond point lookups on the same leader.',
        'Streaming compute substrate: leader-resident state, no Flink slot state.',
        'Cold archive: open-format tiering to Iceberg / Paimon / Lance.',
        'Vector-compatible layer: multi-modal context for ML and AI.',
        'First-class audit trail: deterministic, replayable by design.',
    ];

    return (
        <section className={styles.taxSection}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>The multiple-systems tax</span>
                    <h2 className={styles.sectionTitle}>
                        Five systems, four seams, continuous engineering tax.
                    </h2>
                    <p className={styles.sectionLead}>
                        A conventional real-time AI stack stitches together a message broker,
                        a stream processor, an online store, an offline store, and a
                        synchronisation layer. Every boundary is a seam where data silently
                        diverges. Apache Fluss collapses that stack into one substrate.
                    </p>
                </div>

                <div className={styles.taxGrid}>
                    <div className={styles.taxColumn}>
                        <span className={clsx(styles.taxLabel, styles.taxLabelBefore)}>
                            Before · fragmented stack
                        </span>
                        <div className={styles.taxStack}>
                            {beforeStack.map((s, i) => (
                                <div key={i} className={styles.taxStackItem}>
                                    <span className={styles.taxStackIndex} aria-hidden="true">
                                        {String(i + 1).padStart(2, '0')}
                                    </span>
                                    <div>
                                        <div className={styles.taxStackTitle}>{s.label}</div>
                                        <div className={styles.taxStackSub}>{s.sub}</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                        <p className={styles.taxFootnote}>
                            5 systems · 4 sync boundaries · continuous engineering tax
                        </p>
                    </div>

                    <div className={styles.taxArrow} aria-hidden="true">
                        <svg viewBox="0 0 60 24" xmlns="http://www.w3.org/2000/svg">
                            <defs>
                                <linearGradient id="taxArrowGrad" x1="0" x2="1">
                                    <stop offset="0" stopColor="#22D3EE" />
                                    <stop offset="1" stopColor="#2563EB" />
                                </linearGradient>
                            </defs>
                            <path
                                d="M2 12 L52 12 M44 4 L54 12 L44 20"
                                fill="none"
                                stroke="url(#taxArrowGrad)"
                                strokeWidth="2.5"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                            />
                        </svg>
                    </div>

                    <div className={styles.taxColumn}>
                        <span className={clsx(styles.taxLabel, styles.taxLabelAfter)}>
                            After · unified substrate
                        </span>
                        <div className={styles.taxAfterCard}>
                            <div className={styles.taxAfterHeader}>
                                <div className={styles.taxAfterTitle}>Apache Fluss</div>
                                <div className={styles.taxAfterSub}>
                                    One columnar streaming store that natively speaks
                                    every requirement of a real-time AI platform.
                                </div>
                            </div>
                            <ul className={styles.taxAfterList}>
                                {afterRequirements.map((r, i) => (
                                    <li key={i}>
                                        <span className={styles.taxCheck} aria-hidden="true">✓</span>
                                        {r}
                                    </li>
                                ))}
                            </ul>
                        </div>
                        <p className={styles.taxFootnote}>
                            1 substrate · 0 sync boundaries · single source of truth
                        </p>
                    </div>
                </div>
            </div>
        </section>
    );
}

function CompareSection() {
    const rows = [
        {
            label: 'Storage model',
            kafka: 'Append-only log, row-oriented',
            lake: 'Columnar tables (batch-friendly)',
            fluss: 'Columnar log + primary-key tables',
        },
        {
            label: 'Freshness',
            kafka: 'Seconds (logs only)',
            lake: 'Minutes to hours (commit-bound)',
            fluss: 'Sub-second, end-to-end',
        },
        {
            label: 'Direct analytical query',
            kafka: 'No (consume + transform)',
            lake: 'Yes, but on cold data',
            fluss: 'Yes, on live + cold data',
        },
        {
            label: 'Lake integration',
            kafka: 'Via separate pipeline',
            lake: 'Native (it is the lake)',
            fluss: 'Native tiering to Iceberg & Paimon',
        },
        {
            label: 'State / lookup',
            kafka: 'External KV store required',
            lake: 'Not designed for it',
            fluss: 'Built-in lookups & upserts',
        },
    ];

    return (
        <section className={styles.section}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.sectionHeader}>
                    <span className={styles.eyebrow}>Where Fluss fits</span>
                    <h2 className={styles.sectionTitle}>
                        Streams, tables, and the lake, in one storage layer.
                    </h2>
                    <p className={styles.sectionLead}>
                        Kafka is great for transport. The lakehouse is great for analytics.
                        Apache Fluss closes the gap between them with columnar streaming
                        storage that is queryable in seconds and tiers natively to your lake.
                    </p>
                </div>

                <div className={styles.compareWrap}>
                    <table className={styles.compareTable}>
                        <thead>
                            <tr>
                                <th scope="col"></th>
                                <th scope="col">Kafka</th>
                                <th scope="col">Iceberg / Paimon alone</th>
                                <th scope="col" className={styles.colHighlight}>Apache Fluss</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((r) => (
                                <tr key={r.label}>
                                    <td>{r.label}</td>
                                    <td>{r.kafka}</td>
                                    <td>{r.lake}</td>
                                    <td className={styles.colHighlight}>{r.fluss}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </section>
    );
}

function FlinkSection() {
    return (
        <section className={clsx(styles.section, styles.sectionWash)}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.flinkBand}>
                    <div>
                        <span className={styles.eyebrow}>Apache Flink integration</span>
                        <h2 className={styles.sectionTitle}>Flink&apos;s natural storage layer.</h2>
                        <p className={styles.sectionLead}>
                            Apache Fluss is designed to be a first-class storage layer for
                            Apache Flink as a source, sink, lookup-join target, and
                            CDC-friendly substrate. Define a catalog, point at Fluss, and
                            you have streaming and analytical access on the same tables.
                        </p>
                        <Link className={styles.btnPrimary} to="/docs/quickstart/flink">
                            Run the Flink quickstart
                            <span aria-hidden="true">→</span>
                        </Link>
                        <div className={styles.versionBadges} aria-label="Supported Flink versions">
                            <span className={styles.badge}>Flink 1.18</span>
                            <span className={styles.badge}>Flink 1.19</span>
                            <span className={styles.badge}>Flink 1.20</span>
                            <span className={styles.badge}>Flink 2.x</span>
                        </div>
                    </div>

                    <div className={styles.codeCard} aria-label="Flink SQL example">
                        <div className={styles.codeChrome} aria-hidden="true">
                            <span /><span /><span />
                        </div>
                        <pre className={styles.codeBody}>
{`-- Register Apache Fluss as a Flink catalog
`}<span className="tk-keyword">CREATE CATALOG</span>{` fluss `}<span className="tk-keyword">WITH</span>{` (
  `}<span className="tk-string">'type'</span>{`             = `}<span className="tk-string">'fluss'</span>{`,
  `}<span className="tk-string">'bootstrap.servers'</span>{` = `}<span className="tk-string">'fluss-server:9123'</span>{`
);

`}<span className="tk-keyword">USE CATALOG</span>{` fluss;

`}<span className="tk-comment">-- Stream + query the same table, with sub-second freshness</span>{`
`}<span className="tk-keyword">SELECT</span>{` user_id, `}<span className="tk-fn">SUM</span>{`(amount)
`}<span className="tk-keyword">FROM</span>{`   orders
`}<span className="tk-keyword">GROUP BY</span>{` user_id;`}
                        </pre>
                    </div>
                </div>
            </div>
        </section>
    );
}

function FreshnessSection() {
    return (
        <section className={styles.section}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Lakehouse freshness</span>
                    <h2 className={styles.sectionTitle}>
                        Close the lakehouse freshness gap.
                    </h2>
                    <p className={styles.sectionLead}>
                        Traditional lake pipelines trade freshness for scale. Apache Fluss
                        keeps lake-grade economics while moving end-to-end latency from
                        hours to seconds.
                    </p>
                </div>

                <div className={styles.freshness} role="list">
                    <div className={styles.freshTile} role="listitem">
                        <div className={styles.freshLatency}>Hours</div>
                        <div className={styles.freshLabel}>Traditional batch</div>
                        <div className={styles.freshSub}>
                            Periodic ETL into the lake. Dashboards lag by hours.
                        </div>
                    </div>

                    <div className={styles.freshTile} role="listitem">
                        <div className={styles.freshLatency}>Minutes</div>
                        <div className={styles.freshLabel}>Micro-batch / streaming ETL</div>
                        <div className={styles.freshSub}>
                            Lower latency, but at the cost of duplicated pipelines and
                            small-file overhead.
                        </div>
                    </div>

                    <div className={clsx(styles.freshTile, styles.freshTileHighlight)} role="listitem">
                        <div className={styles.freshLatency}>Seconds</div>
                        <div className={styles.freshLabel}>Apache Fluss + Iceberg / Paimon</div>
                        <div className={styles.freshSub}>
                            One storage layer for hot and cold. Live queries on streaming
                            data, with native lake tiering.
                        </div>
                    </div>
                </div>
            </div>
        </section>
    );
}

function QuickstartSection() {
    return (
        <section className={clsx(styles.section, styles.sectionWash)}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Developer quickstart</span>
                    <h2 className={styles.sectionTitle}>Run Apache Fluss in minutes.</h2>
                    <p className={styles.sectionLead}>
                        Stand up a local cluster, register a catalog, and start streaming.
                        The full quickstart walks you through Flink, Spark, and the Java
                        client.
                    </p>
                </div>

                <div className={styles.codeCard} style={{maxWidth: 880, margin: '0 auto'}}>
                    <div className={styles.codeChrome} aria-hidden="true">
                        <span /><span /><span />
                    </div>
                    <pre className={styles.codeBody}>
{`# 1. Start a local cluster
`}<span className="tk-fn">./bin/local-cluster.sh</span>{` start

`}<span className="tk-comment"># 2. Open the Flink SQL client and register Fluss</span>{`
`}<span className="tk-fn">./bin/sql-client.sh</span>{`

`}<span className="tk-keyword">CREATE CATALOG</span>{` fluss `}<span className="tk-keyword">WITH</span>{` (
  `}<span className="tk-string">'type'</span>{` = `}<span className="tk-string">'fluss'</span>{`,
  `}<span className="tk-string">'bootstrap.servers'</span>{` = `}<span className="tk-string">'localhost:9123'</span>{`
);

`}<span className="tk-comment"># 3. You're streaming and querying, on the same table.</span>
                    </pre>
                </div>

                <div style={{textAlign: 'center', marginTop: 'var(--fluss-space-8)'}}>
                    <Link className={styles.btnPrimary} to="/docs/quickstart/flink">
                        Open the full quickstart
                        <span aria-hidden="true">→</span>
                    </Link>
                </div>
            </div>
        </section>
    );
}

function CommunitySection() {
    return (
        <section className={clsx(styles.section, styles.sectionDark)}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Community</span>
                    <h2 className={styles.sectionTitle}>
                        Built in the open, governed by the ASF.
                    </h2>
                    <p className={styles.sectionLead}>
                        Apache Fluss is developed openly by a global community of
                        contributors. Join the discussion, file an issue, or send a patch.
                    </p>
                </div>

                <div className={styles.statsRow} aria-label="Project signals">
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>Apache 2.0</div>
                        <div className={styles.statLabel}>Open-source license</div>
                    </div>
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>ASF</div>
                        <div className={styles.statLabel}>Apache Software Foundation governance</div>
                    </div>
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>Flink-first</div>
                        <div className={styles.statLabel}>Streaming integration</div>
                    </div>
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>Lake-native</div>
                        <div className={styles.statLabel}>Iceberg &amp; Paimon tiering</div>
                    </div>
                </div>

                <div className={styles.communityGrid}>
                    <Link className={styles.communityCard} to="https://github.com/apache/fluss">
                        <div className={styles.communityTitle}>GitHub</div>
                        <div>Source code, issues, and pull requests.</div>
                        <div className={styles.communityArrow}>Open repository →</div>
                    </Link>

                    <Link className={styles.communityCard} to={SLACK_INVITE}>
                        <div className={styles.communityTitle}>Slack</div>
                        <div>Real-time chat with users and committers.</div>
                        <div className={styles.communityArrow}>Join the workspace →</div>
                    </Link>

                    <Link className={styles.communityCard} to="/community/welcome">
                        <div className={styles.communityTitle}>Contribute</div>
                        <div>Welcome guide, mailing lists, and how to send your first patch.</div>
                        <div className={styles.communityArrow}>Get started →</div>
                    </Link>
                </div>
            </div>
        </section>
    );
}

function FinalCta() {
    return (
        <section className={styles.ctaBand}>
            <div className={clsx('container', styles.container)}>
                <h2 className={styles.ctaTitle}>
                    Start streaming. Start querying. Same storage.
                </h2>
                <p className={styles.ctaSub}>
                    Apache Fluss is free, open-source, and Apache 2.0 licensed.
                    Drop it into your data platform today.
                </p>
                <div className={styles.ctaActions}>
                    <Link className={styles.btnPrimary} to="/docs/quickstart/flink">
                        Get Started
                        <span aria-hidden="true">→</span>
                    </Link>
                    <Link className={styles.btnSecondary} to="https://github.com/apache/fluss">
                        <img
                            src="img/github_icon.svg"
                            alt=""
                            aria-hidden="true"
                            className={styles.btnIcon}
                        />
                        Star on GitHub
                    </Link>
                </div>
            </div>
        </section>
    );
}

export default function Home(): JSX.Element {
    const heroRef = useRef<HTMLElement>(null);
    useHeroVisibilityClass(heroRef);

    return (
        <Layout
            title=""
            description="Apache Fluss is an open-source columnar streaming storage system. Sub-second freshness, primary-key tables, first-class Apache Flink integration, and native tiering to Apache Iceberg and Apache Paimon."
            wrapperClassName={clsx(styles.homepageWrapper, 'fluss-home')}>
            <HomepageHeader heroRef={heroRef}/>
            <main>
                <HomepageIntroduce/>
                <SystemsTaxSection/>
                <HomepageFeatures/>
                <CompareSection/>
                <FlinkSection/>
                <FreshnessSection/>
                <QuickstartSection/>
                <CommunitySection/>
                <FinalCta/>
            </main>
        </Layout>
    );
}
