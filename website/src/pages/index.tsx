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
    // Inline SVG: producers → Apache Fluss Cluster (Log Tables + PK Tables)
    // → engines (Flink, Trino, Spark, ML/AI), with a Lakehouse tier below
    // (Iceberg / Paimon / Lance) connected by a bidirectional Tiering
    // Service edge.
    //
    // Layout uses a single shared vertical centre y = 220:
    //   - The Fluss block sits at y = 110-330 (centre 220).
    //   - The 3 producer boxes are vertically symmetric around 220.
    //   - The 4 consumer boxes are vertically symmetric around 220.
    //   - All arrows fan from / converge to the block's mid-edges.
    //
    // Every edge uses the same flowing cyan style and the same arrowhead
    // marker; the marker's `auto-start-reverse` orientation lets the
    // bidirectional Tiering Service arrow reuse the same head shape on
    // both ends. The animation respects prefers-reduced-motion.
    return (
        <svg
            viewBox="0 0 720 500"
            xmlns="http://www.w3.org/2000/svg"
            role="img"
            aria-labelledby="heroDiagramTitle heroDiagramDesc">
            <title id="heroDiagramTitle">Apache Fluss data flow</title>
            <desc id="heroDiagramDesc">
                Producers (API Clients, Change Data Capture, Flink/Spark) on
                the left feed Apache Fluss in the centre. Fluss contains Log
                Tables and PK Tables, with column pruning, predicate
                pushdowns, and realtime updates. Query engines (Apache Flink,
                Trino, Apache Spark, StarRocks, DuckDB) read on the right via
                a Union Read that merges Fluss and the Lakehouse. Data tiers
                durably to a Lakehouse (Iceberg / Paimon / Lance) below via a
                bidirectional Tiering Service edge.
            </desc>

            <defs>
                {/* Single cyan arrowhead, shared by every edge. Filled
                    triangle for a cleaner, more professional look on dashed
                    lines. auto-start-reverse lets it serve as both
                    marker-start and marker-end on the bidirectional
                    Tiering Service arrow. */}
                <marker id="hgArrowLive" viewBox="0 0 10 10" refX="9" refY="5"
                        markerWidth="8" markerHeight="8"
                        orient="auto-start-reverse">
                    <path d="M0 0 L 10 5 L 0 10 Z" fill="#22D3EE" />
                </marker>
                {/* Scoped animation; respects prefers-reduced-motion. */}
                <style
                    dangerouslySetInnerHTML={{
                        __html: `
                            .fluss-hero-live {
                                animation: flussHeroFlow 5s linear infinite;
                            }
                            @keyframes flussHeroFlow {
                                0%   { stroke-dashoffset: 32; }
                                100% { stroke-dashoffset: 0; }
                            }
                            @media (prefers-reduced-motion: reduce) {
                                .fluss-hero-live { animation: none; }
                            }
                        `,
                    }}
                />
            </defs>

            <g fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" fontSize="14">

                {/* Producers (3 boxes, vertically centred around y = 220).
                    Width 200 so the longest label, "Change Data Capture",
                    fits cleanly at the shared 14px monospace size. */}
                {[
                    {y: 127, label: 'API Clients'},
                    {y: 197, label: 'Change Data Capture'},
                    {y: 267, label: 'Flink/Spark'},
                ].map((p, i) => (
                    <g key={i}>
                        <rect x="20" y={p.y} width="200" height="50" rx="10"
                              fill="#0A2A6B"
                              stroke="rgba(147,184,255,0.35)"
                              strokeWidth="1" />
                        <text x="120" y={p.y + 30} textAnchor="middle"
                              fill="#E6ECFA">
                            {p.label}
                        </text>
                    </g>
                ))}

                {/* Producer arrows into Fluss. Each arrow uses strong
                    horizontal tangents at BOTH endpoints (control points
                    pulled most of the way across the gap) so the merging
                    arrows read as smooth S-curves rather than near-straight
                    diagonals, the pattern most professional architecture
                    diagrams use for converging edges.
                    The middle arrow has no vertical offset to traverse, so
                    it stays a clean straight line. */}
                {[150, 220, 290].map((y, i) => {
                    const isStraight = y === 220;
                    const d = isStraight
                        ? `M220 ${y} L 260 ${y}`
                        : `M220 ${y} C 252 ${y}, 228 220, 260 220`;
                    return (
                        <path
                            key={i}
                            className="fluss-hero-live"
                            d={d}
                            stroke="#22D3EE"
                            strokeWidth="1.75"
                            strokeDasharray="4 4"
                            fill="none"
                            markerEnd="url(#hgArrowLive)"
                        />
                    );
                })}

                {/* Apache Fluss Cluster (the hot tier) */}
                <rect x="260" y="110" width="220" height="220" rx="16"
                      fill="#0A2A6B"
                      stroke="rgba(34,211,238,0.5)"
                      strokeWidth="1.25" />
                <text x="370" y="142" textAnchor="middle"
                      fill="#A5F3FC" fontSize="16" fontWeight="600">
                    Apache Fluss
                </text>

                {/* Internal pieces (centred around block centre y = 220) */}
                <g>
                    <rect x="290" y="170" width="160" height="44" rx="8"
                          fill="#061B3F"
                          stroke="rgba(147,184,255,0.35)" />
                    <text x="370" y="198" textAnchor="middle"
                          fill="#E6ECFA" fontSize="14">Log Tables</text>

                    <rect x="290" y="222" width="160" height="44" rx="8"
                          fill="#061B3F"
                          stroke="rgba(147,184,255,0.35)" />
                    <text x="370" y="250" textAnchor="middle"
                          fill="#E6ECFA" fontSize="14">PK Tables</text>
                </g>

                {/* Capability caption (three lines so it fits inside the
                    block). Uses Fluss blue-300 so it reads as a distinct
                    secondary accent, not a desaturated grey caption. */}
                <text textAnchor="middle"
                      fill="#93B8FF" fontSize="12"
                      fontWeight="500" opacity="0.95">
                    <tspan x="370" y="284">Column Pruning,</tspan>
                    <tspan x="370" y="302">Predicate Pushdowns,</tspan>
                    <tspan x="370" y="320">Realtime Updates</tspan>
                </text>

                {/* Query Engines box. Vertically centred on y = 220 (the
                    same axis as the Fluss block) and tall enough to fit
                    five engine names with even spacing. */}
                <text x="625" y="108" textAnchor="middle"
                      fill="#22D3EE" fontSize="11"
                      fontWeight="600" letterSpacing="1.4">
                    QUERY ENGINES
                </text>
                <rect x="540" y="120" width="170" height="200" rx="14"
                      fill="#0A2A6B"
                      stroke="rgba(34,211,238,0.5)"
                      strokeWidth="1.25" />
                <text x="625" y="150" textAnchor="middle"
                      fill="#A5F3FC" fontSize="16" fontWeight="600">
                    Engines
                </text>
                <line x1="560" y1="166" x2="690" y2="166"
                      stroke="rgba(147,184,255,0.25)" strokeWidth="1" />
                {[
                    {y: 192, label: 'Apache Flink'},
                    {y: 220, label: 'Trino'},
                    {y: 248, label: 'Apache Spark'},
                    {y: 276, label: 'StarRocks'},
                    {y: 304, label: 'DuckDB'},
                ].map((e, i) => (
                    <text key={i} x="625" y={e.y} textAnchor="middle"
                          fill="#E6ECFA" fontSize="14">
                        {e.label}
                    </text>
                ))}

                {/* Y-junction merging Fluss and Lakehouse reads into a single
                    Union Read edge that points into the Engines box.
                    Merge node at (510, 220). */}

                {/* Fluss → merge (hot tier, straight horizontal feed) */}
                <path
                    className="fluss-hero-live"
                    d="M480 220 L 510 220"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                />

                {/* Lakehouse → merge (cold tier, arcs up to the same node) */}
                <path
                    className="fluss-hero-live"
                    d="M480 433 C 520 433, 510 280, 510 220"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                />

                {/* Merged segment → Engines (single arrow, carries the
                    Union Read label) */}
                <path
                    className="fluss-hero-live"
                    d="M510 220 L 540 220"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                    markerEnd="url(#hgArrowLive)"
                />
                <text x="525" y="210" textAnchor="middle"
                      fill="#22D3EE" fontSize="11"
                      fontWeight="600" letterSpacing="0.6">
                    Union Read
                </text>

                {/* Tiering Service: bidirectional animated cyan edge */}
                <path className="fluss-hero-live"
                      d="M370 333 L 370 388"
                      stroke="#22D3EE"
                      strokeWidth="1.75"
                      strokeDasharray="4 4"
                      fill="none"
                      markerStart="url(#hgArrowLive)"
                      markerEnd="url(#hgArrowLive)" />
                <text x="385" y="365"
                      fill="#A5F3FC" opacity="0.9" fontSize="13">
                    Tiering Service
                </text>

                {/* Lakehouse (cold tier). Width and x matched to the Fluss
                    block above so they form a single, tightly aligned
                    centre column. */}
                <rect x="260" y="390" width="220" height="86" rx="12"
                      fill="#061B3F"
                      stroke="rgba(59,130,246,0.55)"
                      strokeDasharray="3 3" />
                <text x="370" y="420" textAnchor="middle"
                      fill="#A5F3FC" fontSize="16" fontWeight="600">
                    Lakehouse
                </text>
                <text x="370" y="450" textAnchor="middle"
                      fill="#C2CCE2" fontSize="14">
                    Iceberg / Paimon / Lance
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

                    <div className={styles.heroDiagramColumn}>
                        <div className={styles.heroDiagram} aria-hidden="false">
                            <h2 className={styles.heroDiagramTitle}>
                                Powering The Streaming Lakehouse
                            </h2>
                            <HeroDiagram />
                        </div>
                    </div>
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
                        <div className={styles.statValue}>Streams As Tables</div>
                        <div className={styles.statLabel}>Streaming integration</div>
                    </div>
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>Lakehouse-Native</div>
                        <div className={styles.statLabel}>Open Table Format Tiering</div>
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
