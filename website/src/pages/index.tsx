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
import {useEffect, useRef, useState} from 'react';
import {Highlight} from 'prism-react-renderer';
import flussPrismDark from '@site/src/utils/prismDark';

import styles from './index.module.css';

/**
 * Canonical Fluss + Flink SQL snippet, sourced from
 * docs/engine-flink/getting-started.md.
 */
const HERO_FLINK_SQL = `-- Register Apache Fluss as a Flink catalog
CREATE CATALOG fluss_catalog WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;

-- Create a primary-key table
CREATE TABLE pk_table (
  shop_id    BIGINT,
  user_id    BIGINT,
  num_orders INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH ('bucket.num' = '4');

INSERT INTO pk_table VALUES (1234, 1234, 1);
SELECT * FROM pk_table WHERE shop_id = 1234;
`;

/**
 * Canonical Fluss + Spark SQL snippet, sourced from
 * docs/engine-spark/getting-started.md.
 */
const HERO_SPARK_SQL = `-- Register Apache Fluss as a Spark catalog (via spark-sql --conf):
--   spark.sql.catalog.fluss_catalog = org.apache.fluss.spark.SparkCatalog
--   spark.sql.catalog.fluss_catalog.bootstrap.servers = localhost:9123
USE fluss_catalog;

-- Create the same primary-key table
CREATE TABLE pk_table (
  shop_id    BIGINT,
  user_id    BIGINT,
  num_orders INT
) TBLPROPERTIES (
  'primary.key' = 'shop_id,user_id',
  'bucket.num'  = '4'
);

INSERT INTO pk_table VALUES (1234, 1234, 1);
SELECT * FROM pk_table ORDER BY shop_id;
`;

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
    // → engines, with a Lakehouse tier below connected by a bidirectional
    // Tiering Service edge.
    //
    // ViewBox is 1000 × 400 (2.5:1) — wider than tall, so the canvas reads
    // as a horizontal flow at the page width. The Fluss block exploits the
    // extra width by laying out Log Tables and PK Tables SIDE BY SIDE
    // (rather than stacked), which lets the block be ~25% shorter without
    // sacrificing legibility. All text sizes are reduced ~2px across the
    // board so the diagram doesn't feel cramped at the new aspect ratio.
    //
    // Vertical centerline (where producer arrows aim and Union Read flows)
    // is y = 140. Tiering edge sits in the y=240→290 gap between Fluss and
    // Lakehouse.
    return (
        <svg
            viewBox="0 0 1000 400"
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
                <marker id="hgArrowLive" viewBox="0 0 10 10" refX="9" refY="5"
                        markerWidth="7" markerHeight="7"
                        orient="auto-start-reverse">
                    <path d="M0 0 L 10 5 L 0 10 Z" fill="#22D3EE" />
                </marker>
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

            <g fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" fontSize="12">

                {/* Producers (3 boxes, vertically distributed around the
                    Fluss block centerline y = 140). */}
                {[
                    {y: 58, label: 'API Clients'},
                    {y: 118, label: 'Change Data Capture'},
                    {y: 178, label: 'Flink/Spark'},
                ].map((p, i) => (
                    <g key={i}>
                        <rect x="20" y={p.y} width="200" height="44" rx="10"
                              fill="#0A2A6B"
                              stroke="rgba(147,184,255,0.35)"
                              strokeWidth="1" />
                        <text x="120" y={p.y + 27} textAnchor="middle"
                              fill="#E6ECFA">
                            {p.label}
                        </text>
                    </g>
                ))}

                {/* Producer arrows into Fluss. All converge on the Fluss
                    block's left-edge midpoint (290, 140). Top + bottom use
                    smooth S-curves; the middle is a near-straight line. */}
                {[80, 140, 200].map((y, i) => {
                    const isStraight = y === 140;
                    const d = isStraight
                        ? `M220 ${y} L 290 ${y}`
                        : `M220 ${y} C 258 ${y}, 252 140, 290 140`;
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

                {/* Apache Fluss Cluster (the hot tier).
                    Wider + shorter than before: 400 × 200 (was 220 × 220).
                    The extra width lets Log Tables + PK Tables sit
                    side-by-side instead of stacked, which is what reduces
                    the height. */}
                <rect x="290" y="40" width="400" height="200" rx="14"
                      fill="#0A2A6B"
                      stroke="rgba(34,211,238,0.5)"
                      strokeWidth="1.25" />
                <text x="490" y="68" textAnchor="middle"
                      fill="#A5F3FC" fontSize="14" fontWeight="600">
                    Apache Fluss
                </text>

                {/* Internal pieces — Log Tables (left) + PK Tables (right). */}
                <g>
                    <rect x="315" y="92" width="170" height="40" rx="8"
                          fill="#061B3F"
                          stroke="rgba(147,184,255,0.35)" />
                    <text x="400" y="117" textAnchor="middle"
                          fill="#E6ECFA" fontSize="12">Log Tables</text>

                    <rect x="495" y="92" width="170" height="40" rx="8"
                          fill="#061B3F"
                          stroke="rgba(147,184,255,0.35)" />
                    <text x="580" y="117" textAnchor="middle"
                          fill="#E6ECFA" fontSize="12">PK Tables</text>
                </g>

                {/* Capability caption — single line now that the block is
                    wide enough. */}
                <text textAnchor="middle"
                      fill="#93B8FF" fontSize="11"
                      fontWeight="500" opacity="0.95">
                    <tspan x="490" y="172">
                        Column Pruning · Predicate Pushdown · Realtime Updates
                    </tspan>
                    <tspan x="490" y="194">
                        Sub-second Freshness
                    </tspan>
                </text>

                {/* Query Engines box (right column). */}
                <text x="870" y="32" textAnchor="middle"
                      fill="#22D3EE" fontSize="10"
                      fontWeight="600" letterSpacing="1.4">
                    QUERY ENGINES
                </text>
                <rect x="760" y="40" width="220" height="200" rx="14"
                      fill="#0A2A6B"
                      stroke="rgba(34,211,238,0.5)"
                      strokeWidth="1.25" />
                <text x="870" y="70" textAnchor="middle"
                      fill="#A5F3FC" fontSize="14" fontWeight="600">
                    Engines
                </text>
                <line x1="780" y1="86" x2="960" y2="86"
                      stroke="rgba(147,184,255,0.25)" strokeWidth="1" />
                {[
                    {y: 112, label: 'Apache Flink'},
                    {y: 138, label: 'Trino'},
                    {y: 164, label: 'Apache Spark'},
                    {y: 190, label: 'StarRocks'},
                    {y: 216, label: 'DuckDB'},
                ].map((e, i) => (
                    <text key={i} x="870" y={e.y} textAnchor="middle"
                          fill="#E6ECFA" fontSize="12">
                        {e.label}
                    </text>
                ))}

                {/* Y-junction merging Fluss and Lakehouse reads into a single
                    Union Read edge that points into the Engines box.
                    Merge node at (725, 140). */}
                <path
                    className="fluss-hero-live"
                    d="M690 140 L 725 140"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                />
                {/* Lakehouse → merge (cold tier, arcs up). */}
                <path
                    className="fluss-hero-live"
                    d="M690 325 C 725 325, 725 220, 725 140"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                />
                {/* Merge → Engines (carries the Union Read label). */}
                <path
                    className="fluss-hero-live"
                    d="M725 140 L 760 140"
                    stroke="#22D3EE"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                    markerEnd="url(#hgArrowLive)"
                />
                <text x="742" y="130" textAnchor="middle"
                      fill="#22D3EE" fontSize="10"
                      fontWeight="600" letterSpacing="0.6">
                    Union Read
                </text>

                {/* Tiering Service: bidirectional animated cyan edge in the
                    gap between Fluss bottom and Lakehouse top. */}
                <path className="fluss-hero-live"
                      d="M490 240 L 490 290"
                      stroke="#22D3EE"
                      strokeWidth="1.75"
                      strokeDasharray="4 4"
                      fill="none"
                      markerStart="url(#hgArrowLive)"
                      markerEnd="url(#hgArrowLive)" />
                <text x="505" y="270"
                      fill="#A5F3FC" opacity="0.9" fontSize="11">
                    Tiering Service
                </text>

                {/* Lakehouse (cold tier). Width matches Fluss above so the
                    centre column reads as one tightly aligned stack. */}
                <rect x="290" y="290" width="400" height="70" rx="10"
                      fill="#061B3F"
                      stroke="rgba(59,130,246,0.55)"
                      strokeDasharray="3 3" />
                <text x="490" y="316" textAnchor="middle"
                      fill="#A5F3FC" fontSize="14" fontWeight="600">
                    Lakehouse
                </text>
                <text x="490" y="343" textAnchor="middle"
                      fill="#C2CCE2" fontSize="12">
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
                            Apache Fluss (Incubating) is an open-source,
                            lakehouse-native streaming storage. It collapses the
                            message broker, online KV store, stream-processing
                            state backend, and lakehouse offline store into a
                            single coherent foundation, making the Lakehouse
                            truly real-time.
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

                    <div className={styles.heroCodeColumn}>
                        <HeroCodePanel />
                    </div>
                </div>
            </div>
        </header>
    );
}

/**
 * Renders a SQL snippet with Prism syntax highlighting using the project's
 * shared dark Prism theme (so colours match the Fluss palette). The theme's
 * background is overridden because the surrounding code card already paints
 * its own background.
 */
function HeroSqlBlock({code}: {code: string}) {
    return (
        <Highlight code={code.trimEnd()} language="sql" theme={flussPrismDark}>
            {({className, tokens, getLineProps, getTokenProps}) => (
                <pre
                    role="tabpanel"
                    className={clsx(styles.codeBody, className)}
                    style={{background: 'transparent'}}>
                    {tokens.map((line, i) => (
                        <div key={i} {...getLineProps({line})}>
                            {line.map((token, key) => (
                                <span key={key} {...getTokenProps({token})} />
                            ))}
                        </div>
                    ))}
                </pre>
            )}
        </Highlight>
    );
}

function HeroCodePanel() {
    const [active, setActive] = useState<'flink' | 'spark'>('flink');
    return (
        <div className={styles.codeCard} aria-label="Apache Fluss code example">
            <div className={styles.heroCodeHeader}>
                <span className={styles.heroCodeDots} aria-hidden="true">
                    <span /><span /><span />
                </span>
                <div className={styles.heroCodeTabs} role="tablist" aria-label="Engine">
                    <button
                        type="button"
                        role="tab"
                        aria-selected={active === 'flink'}
                        className={clsx(
                            styles.heroCodeTab,
                            active === 'flink' && styles.heroCodeTabActive,
                        )}
                        onClick={() => setActive('flink')}>
                        Flink SQL
                    </button>
                    <button
                        type="button"
                        role="tab"
                        aria-selected={active === 'spark'}
                        className={clsx(
                            styles.heroCodeTab,
                            active === 'spark' && styles.heroCodeTabActive,
                        )}
                        onClick={() => setActive('spark')}>
                        Spark SQL
                    </button>
                </div>
            </div>

            <HeroSqlBlock code={active === 'flink' ? HERO_FLINK_SQL : HERO_SPARK_SQL} />
        </div>
    );
}

function ArchitectureSection() {
    return (
        <section className={clsx(styles.section, styles.sectionDark, styles.archSection)}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Architecture</span>
                    <h2 className={clsx(styles.sectionTitle, styles.archTitle)}>
                        Unlocking the Streamhouse Architecture
                    </h2>
                </div>
                <div className={styles.archDiagram}>
                    <HeroDiagram />
                </div>
            </div>
        </section>
    );
}

function SystemsTaxSection() {
    const beforeStack = [
        {
            label: 'Message broker',
            sub: 'Kafka, for event transport.',
        },
        {
            label: 'Stream processor',
            sub: 'Flink or Spark, for derived features and aggregations.',
        },
        {
            label: 'Online store',
            sub: 'Redis or DynamoDB, for sub-millisecond lookup.',
        },
        {
            label: 'Offline store',
            sub: 'Iceberg or Parquet on S3, for training and history.',
        },
        {
            label: 'Sync layer',
            sub: 'bespoke pipelines and freshness monitors that drift silently.',
        },
    ];

    const afterRequirements: {label: string; sub: string}[] = [
        {
            label: 'Event log',
            sub: 'durable, replayable, offset-ordered streams',
        },
        {
            label: 'PK lookup',
            sub: 'sub-millisecond key/value lookups',
        },
        {
            label: 'State externalization for Flink',
            sub: 'Delta Joins, Partial Updates, Deduplication and Aggregation Merge Engine use Fluss as a state store',
        },
        {
            label: 'Open-format cold tier',
            sub: 'automatic tiering to Iceberg · Paimon · Lance',
        },
        {
            label: 'Multi-modal ready',
            sub: 'Lance integration for vectors and ML context',
        },
        {
            label: 'Deterministic audit trail',
            sub: '$changelog & $binlog virtual tables, replayable by design',
        },
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
                            5 systems · 4 sync boundaries owned by you · continuous engineering tax
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
                                    One columnar streaming store designed for the
                                    real-time AI data plane.
                                </div>
                            </div>
                            <ul className={styles.taxAfterList}>
                                {afterRequirements.map((r) => (
                                    <li key={r.label}>
                                        <span className={styles.taxCheck} aria-hidden="true">✓</span>
                                        <span>
                                            <strong>{r.label}</strong>
                                            {' · '}
                                            {r.sub}
                                        </span>
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
    const rows: {dimension: string; kafka: string; fluss: string}[] = [
        {
            dimension: 'Positioning',
            kafka: 'Distributed event streaming platform / durable commit log',
            fluss: 'Streaming storage for real-time analytics, lakehouse-native',
        },
        {
            dimension: 'Storage model',
            kafka: 'Append-only row log; tiered to S3 · GCS · ABFS via KIP-405',
            fluss: 'Columnar Arrow IPC log & LSM-backed KV index for PK Tables; tiered into Paimon · Iceberg · Lance as the cold layer ("shared data": one logical table, two physical layouts)',
        },
        {
            dimension: 'Metadata plane · partitioning',
            kafka: 'KRaft controllers · hash-keyed topic partitions',
            fluss: 'CoordinatorServer & TabletServers · buckets & first-class partitioned tables',
        },
        {
            dimension: 'Logical unit · writes',
            kafka: 'Topic (log only)',
            fluss: 'Tables as the core abstraction: Log Tables for append-only streams and Primary Key Tables for native upserts, partial updates, and deletes',
        },
        {
            dimension: 'Schema · CDC · types',
            kafka: 'External Schema Registry; CDC via Connect · Debezium; nested types out-of-band in payload',
            fluss: 'First-class schemas with evolution; native $changelog · $binlog virtual tables; native ARRAY · MAP · ROW with deep nesting',
        },
        {
            dimension: 'Read path',
            kafka: 'No server-side pruning or predicate pushdown; no native PK lookup',
            fluss: 'Server-side zero-copy column · partition · predicate pushdown; built-in PK lookup via LSM',
        },
        {
            dimension: 'State externalization (with Flink)',
            kafka: 'App holds join state (RocksDB) and aggregation state',
            fluss: 'Delta Joins externalize join state to Fluss; Aggregation Merge Engine pushes aggregation into storage; merge engines: Default · FirstRow · Versioned · Aggregation',
        },
        {
            dimension: 'Strong fit',
            kafka: 'Event-driven systems · log aggregation · microservice pub/sub · cross-language transport',
            fluss: 'Real-time analytics on wide tables · streaming lakehouse · dimension joins · CDC-heavy pipelines · Flink-centric stacks',
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
                    {/* Override .sectionLead's default 720px max-width so the
                        lead spans the same width as the comparison table below
                        and fills each line edge-to-edge before wrapping. */}
                    <p className={styles.sectionLead} style={{maxWidth: 'none'}}>
                        Kafka is great for transport. The lakehouse is great for analytics.
                        Apache Fluss closes the gap between them with columnar streaming
                        storage that is queryable in seconds and tiers natively to your lake.
                    </p>
                </div>

                <div className={styles.compareWrap}>
                    <table className={styles.compareTable}>
                        <thead>
                            <tr>
                                <th scope="col">Dimension</th>
                                <th scope="col">Apache Kafka</th>
                                <th scope="col" className={styles.colHighlight}>Apache Fluss</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((r) => (
                                <tr key={r.dimension}>
                                    <td>{r.dimension}</td>
                                    <td>{r.kafka}</td>
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
                <ArchitectureSection/>
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
