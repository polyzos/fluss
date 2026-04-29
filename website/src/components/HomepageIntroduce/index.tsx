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
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type ValueProp = {
  title: string;
  body: string;
  Icon: React.ComponentType<React.ComponentProps<'svg'>>;
};

const VALUE_PROPS: ValueProp[] = [
  {
    title: 'Sub-second Freshness',
    body: 'Columnar streaming over Apache Arrow with server-side column pruning and predicate pushdown. Analytics, features, and agents read new rows within sub-second latency of write. Column projection, predicate pushdown, and partition pruning compound into order-of-magnitude reductions in network transfer.',
    Icon: require('@site/static/img/feature_real_time.svg').default,
  },
  {
    title: 'One Data Copy Across Hot & Cold Layer',
    body: 'The hot Fluss tier and the cold open-format tier (Iceberg, Paimon, Lance) share a single logical schema with synchronised metadata. The Tiering Service and Union Read mechanism make both layers queryable as one substrate, eliminating the dual-write fork between streaming and batch.',
    Icon: require('@site/static/img/feature_lake.svg').default,
  },
  {
    title: 'Stream/Table Duality In One System',
    body: 'PK Tables hold both an append-only changelog and a latest state in RocksDB, like database tables. Sub-millisecond key/value lookups are feasible, consolidating roles previously split across a message queue, a key-value store, and an OLAP engine.',
    Icon: require('@site/static/img/feature_update.svg').default,
  },
  {
    title: 'Stateless, elastic compute',
    body: 'Fluss acts as a shared-state store, allowing compute engines like Apache Flink to be stateless. Recovery collapses from minutes to seconds, RPO is decoupled from RTO, and benchmarked topologies run up to 85% cheaper than comparable Kafka-based equivalents.',
    Icon: require('@site/static/img/feature_lookup.svg').default,
  },
];

function ValuePropCard({title, body, Icon}: ValueProp) {
  return (
    <div className={styles.card}>
      <div className={styles.cardIcon} aria-hidden="true">
        <Icon role="img" />
      </div>
      <Heading as="h3" className={styles.cardTitle}>{title}</Heading>
      <p className={styles.cardBody}>{body}</p>
    </div>
  );
}

export default function HomepageIntroduce(): JSX.Element {
  return (
    <section className={styles.introduce}>
      <div className={clsx('container', styles.container)}>
        <div className={styles.header}>
          <span className={styles.eyebrow}>What is Apache Fluss?</span>
          <Heading as="h2" className={styles.title}>
            One Platform. Streams, tables, and the lake.
          </Heading>
          <p className={styles.lead}>
            <b>Apache Fluss (Incubating)</b> is an open-source columnar streaming
            storage system. It collapses the message broker, online KV store,
            stream-processing state backend, and lakehouse offline store into a
            single coherent foundation, so the systems above it don&apos;t have
            to keep them in sync.
          </p>
        </div>

        <div className={styles.grid}>
          {VALUE_PROPS.map((vp) => (
            <ValuePropCard key={vp.title} {...vp} />
          ))}
        </div>
      </div>
    </section>
  );
}
