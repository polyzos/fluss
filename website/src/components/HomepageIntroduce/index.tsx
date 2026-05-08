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
  lead: string;
  bullets: string[];
  Icon: React.ComponentType<React.ComponentProps<'svg'>>;
};

const VALUE_PROPS: ValueProp[] = [
  {
    title: 'Sub-second Freshness',
    lead: 'Columnar streaming over Apache Arrow with server-side pruning and pushdown.',
    bullets: [
      'Sub-second latency from write to read for analytics, features, and agents.',
      'Column projection, predicate pushdown, and partition pruning.',
      'Order-of-magnitude cuts in network transfer.',
    ],
    Icon: require('@site/static/img/feature_real_time.svg').default,
  },
  {
    title: 'One Data Copy Across Hot & Cold Layer',
    lead: 'Hot Fluss and cold open formats (Iceberg, Paimon, Lance) share one logical schema.',
    bullets: [
      'Synchronised metadata across hot and cold tiers.',
      'Tiering Service and Union Read query both as one substrate.',
      'No dual-write fork between streaming and batch.',
    ],
    Icon: require('@site/static/img/feature_lake.svg').default,
  },
  {
    title: 'Stream/Table Duality In One System',
    lead: 'PK Tables hold an append-only changelog and a latest state in RocksDB.',
    bullets: [
      'Sub-millisecond key/value lookups on primary keys.',
      'Database-style tables with native stream semantics.',
      'Replaces message queue + KV store + OLAP engine.',
    ],
    Icon: require('@site/static/img/feature_update.svg').default,
  },
  {
    title: 'Externalized States, Elastic Compute',
    lead: 'Fluss is a shared-state store, so engines like Apache Flink stay stateless.',
    bullets: [
      'Recovery collapses from minutes to seconds.',
      'RPO decoupled from RTO.',
      'Up to 85% cheaper than Kafka-based equivalents.',
    ],
    Icon: require('@site/static/img/feature_lookup.svg').default,
  },
];

function ValuePropCard({title, lead, bullets, Icon}: ValueProp) {
  return (
    <div className={styles.card}>
      <div className={styles.cardIcon} aria-hidden="true">
        <Icon role="img" />
      </div>
      <Heading as="h3" className={styles.cardTitle}>{title}</Heading>
      <p className={styles.cardLead}>{lead}</p>
      <ul className={styles.cardBullets}>
        {bullets.map((b) => (
          <li key={b}>{b}</li>
        ))}
      </ul>
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
            One Platform. Streams, Tables, and the Lakehouse.
          </Heading>
          <p className={styles.lead}>
            One Table, One SQL Query, Different Freshness Layers
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
