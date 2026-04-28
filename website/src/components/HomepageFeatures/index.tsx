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
import React from 'react';
import styles from './styles.module.css';
import Heading from '@theme/Heading';

type Pillar = {
    number: string;
    title: string;
    summary: string;
    body: string;
    basis: string;
    Svg: React.ComponentType<React.ComponentProps<'svg'>>;
};

const PILLARS: Pillar[] = [
    {
        number: '01',
        title: 'Unified Architecture',
        summary: 'One system for messaging, applications, analytics, and AI.',
        body: 'Consolidates the roles previously played by a message queue, a key-value store, and an OLAP engine into a single coherent platform that serves transports, lookups, and queries from the same data.',
        basis: 'Dual representation of PK Tables (append-only log + leader-side RocksDB KV).',
        Svg: require('@site/static/img/feature_update.svg').default,
    },
    {
        number: '02',
        title: 'Streaming & Lakehouse Unification',
        summary: 'One copy of data across the real-time and batch layers.',
        body: 'Synchronised metadata across tiers. The hot Fluss tier and the cold open-format tier share the same logical schema and are queryable as a single substrate, so streaming and historical queries hit one source of truth.',
        basis: 'Tiering Service and Union Read across Iceberg, Paimon, and Lance.',
        Svg: require('@site/static/img/feature_lake.svg').default,
    },
    {
        number: '03',
        title: 'Compute–Storage Separation',
        summary: 'Lean, elastic, stateless compute with fast recovery.',
        body: 'Decoupled storage enables stateless compute that recovers in seconds, not minutes, and runs up to 85% cheaper than comparable Kafka-based topologies. State lives on the Fluss leader, not in Flink task slots, which collapses recovery time and decouples RPO from RTO.',
        basis: 'Stateless compute model with leader-resident state and KV snapshots.',
        Svg: require('@site/static/img/feature_real_time.svg').default,
    },
    {
        number: '04',
        title: 'Columnar Streaming Analytics',
        summary: 'Pruning that compounds.',
        body: 'Efficient streaming queries over compressed columnar data, with server-side projection pushdown in ARROW format. Column projection, predicate pushdown, and partition pruning compound into order-of-magnitude reductions in I/O and network transfer.',
        basis: 'ARROW log format and the compound pruning stack on the TabletServer.',
        Svg: require('@site/static/img/feature_column.svg').default,
    },
    {
        number: '05',
        title: 'Feature & Context Stores',
        summary: 'Multi-modal data on one substrate, ready for ML and AI.',
        body: 'Row, columnar, and vector formats on the same store. Online feature serving and RAG-ready semantic context live alongside structured analytics. The feature store, context store, and real-time entity profile collapse into one PK Table accessed through different views.',
        basis: 'Unified substrate spanning structured features and vector context.',
        Svg: require('@site/static/img/feature_query.svg').default,
    },
    {
        number: '06',
        title: 'Ecosystem Openness',
        summary: 'Open formats. No vendor lock-in.',
        body: 'Shared open storage readable by Apache Flink, Apache Spark, Trino, StarRocks, and DuckDB. Open formats end to end: native connectors for the hot Fluss tier, with Iceberg, Paimon, and Lance for the cold tier.',
        basis: 'Open lake formats throughout, governed at the Apache Software Foundation.',
        Svg: require('@site/static/img/feature_changelog.svg').default,
    },
];

function PillarCard({number, title, summary, body, basis, Svg}: Pillar) {
    return (
        <article className={styles.card}>
            <div className={styles.cardTop}>
                <div className={styles.iconWrap} aria-hidden="true">
                    <Svg className={styles.icon} role="img" />
                </div>
                <span className={styles.number} aria-hidden="true">{number}</span>
            </div>
            <Heading as="h3" className={styles.title}>{title}</Heading>
            <p className={styles.summary}>{summary}</p>
            <p className={styles.body}>{body}</p>
            <p className={styles.basis}>
                <span className={styles.basisLabel}>Architectural basis</span>
                {basis}
            </p>
        </article>
    );
}

export default function HomepageFeatures(): JSX.Element {
    return (
        <section className={styles.features}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.header}>
                    <span className={styles.eyebrow}>Six capability pillars</span>
                    <Heading as="h2" className={styles.heading}>
                        The benefits, grounded in the architecture.
                    </Heading>
                    <p className={styles.lead}>
                        Each pillar is a direct consequence of a specific architectural
                        mechanism, not a marketing claim. Together they collapse the
                        fragmented real-time stack into a single coherent foundation.
                    </p>
                </div>

                <div className={styles.grid}>
                    {PILLARS.map((p) => (
                        <PillarCard key={p.number} {...p} />
                    ))}
                </div>
            </div>
        </section>
    );
}
