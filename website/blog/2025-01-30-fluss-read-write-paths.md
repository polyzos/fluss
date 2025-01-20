---
slug: fluss-read-write-paths
title: "Understanding Fluss Read and Write Paths"
authors: [giannis]
---

## Introduction

Fluss is an open-source streaming storage system engineered for real-time analytics, serving as a real-time data layer for Lakehouse architectures.
It bridges the gap between streaming data and data Lakehouse, in order to bring lower latencies to the data Lakehouse and better analytics to data streams.

By delving into its architecture and understanding its read and write paths, developers can contribute to its evolution and leverage its capabilities to build efficient, real-time data processing applications. 

This blog post aims to provide an overview of Fluss read and write paths, elucidate its data flow mechanisms, and help developers interested, understand and engage with the project.

## The Write Path
-----------------
At its core, Fluss stream storage is built on a Log Tablet, with a key-value (KV) index constructed over the log. This architecture enables efficient real-time updates. The relationship between the Log and KV mirrors the concept of stream-table duality: updates to the KV index generate a changelog that is written to the Log Tablet. In the event of a failure, data from the Log Tablet is used to recover the KV Tablet.

Let's start with the Flink Sink and understand how Flink writes data to Fluss.

### The Flink Sink
![Flink Client](assets/fluss_wr_paths/img1.png)

As depicted in the figure above, we have the FlinkSinkFunction and since we can have
either log tables or primary key tables, the Sink Function, consists of two implementations:
- **AppendSinkFunction:** handles writing of `append-only` data.
- **FlinkUpsertSinkFunction:** handles writing of upserts, based
- on some primary key.

Each implementation holds an instance of a TableWriter that comes from the Fluss Client package.

![TableWriter](assets/fluss_wr_paths/img2.png)
The **AppendSinkFunction** has an **AppendWriter**, while the **UpsertSinkFunction** has an **UpsertWriter**.

Again as the names suggest each writes append-only and upsert data respectively.

The **TableWriter** uses the WriterClient, which is responsible for sending data to Fluss Servers, namely the **TabletServers**.
Sending messages happens with the send() method that sends WriteRecords.

WriteRecords is a conversion from Flink's InternalRow.

For table with primary key, if we use the **UpsertWriter** to send record, it will convert to
WriteRecord with a key part, a value row part and write kind of `PUT`.

For `deletes` it will convert to a WriteRecord with a key part , an empty value row and write kind WriteKind of `DELETE`.

For none-pk table, if we use the **AppendWriter** to send records, it will
convert to a WriteRecord without key, the value row part and write kind of `APPEND`.


![WriterClient](assets/fluss_wr_paths/img3.png)

The writer consists of a pool of buffer space that holds records that haven't yet been
transmitted to the TabletServer as well as a background I/O thread that is responsible for
turning these records into requests and transmitting them to the cluster. Failure to close the
after use will leak these resources.

The `send()` method is asynchronous. When called, it adds the log record to a buffer of pending
record sends and immediately returns. This allows the wrote record to batch together individual
records for efficiency.

These batches can be of different types and produce different types of requests depending on the type:
1. It can generate a **KvWriteBatch** that sends a **PutKvRequest** to the server to handle KV data.
2. It can generate an **ArrowLogWriteBatch** or an **IndexedLogWriteBatch** that sends a **ProduceLogRequest** to the server to handle log (append-only) data.

By default log data gets stored in the arrow format.

<add a diagram here>

At this point the requests have been send to the server.

### The TabletServer
On the server side we have the TabletServers that manages the Log tablets and KV tablets via a LogManager and KVManager. 
The TabletServers also expose a TabletService, which is an RPC Gateway that receives and handles incoming requests, like the **PutKvRequest** and **ProduceLogRequest**.

Let's see how these requests are handle to store kv or log data. Let's start with the **PutKvRequest** that writes the KV data, as its more involved.
The TabletService has a **ReplicaManager** that manages **Replicas**. During writes the `putRecordsToKv(...)` is invoked. This method will put kv records
to the leader replicas of the buckets. The kv data will write will be writen to the KvTablet and the response needs to wait for the CDC log (changelog) to be replicated to the other replicas.

More specifically the **KvTablet** invokes the `putAsLeader()` method and this method: 
1. Handles the partial updates and other another merge engine if specified
2. Creates a WAL (via the LogTablet) using the arrow format by default and it writes the operations:
   * INSERT (+I)
   * UPDATE_BEFORE (-U)
   * UPDATE_AFTER (+U)
   * DELETE (-D)
3. Writes the records to an in-memory buffer called **KvPreWriteBuffer**.

The **KvPreWriteBuffer** is an in-memory pre-write buffer for putting kv records. 
The kv records will first be put into the  buffer and then be flushed to the underlying 
kv storage when the `flush()` method is invoked.

In Fluss, when putting a key-value pair, Fluss will first write WAL first. Only when the WAL 
has been persisted(with fault tolerance), can Fluss safely writing the key-value pair to the 
underlying kv storage. Otherwise, it'll cause in-consistent data in the kv storage.

For example, if Fluss writes data to the kv storage without waiting for the WAL to be persisted, 
then users can read the data from kv storage. But unfortunately, the kv storage was lost. 
Then, Fluss can never restore the piece of data to kv storage from the WAL as it hasn't 
been persisted, which will cause user can not read the data any more although the data has been ever read.

The pre-write buffer was introduced to solve this problem. 

After data has been persisted in all replicas, ack’s are send back to the client.

Handling a **ProduceLogRequest** is similar, but simpler. Similary the **ReplicaManager** invokes the `appendRecordsToLog(...)` method.
As the name suggests is appends log records to leader replicas of the buckets, and wait for them to be replicated to other replicas.
And the actual writing happens from the **LogTablet** when the `appendAsLeader()` method is invoked, which appends the messages to the active segment of the local log, assigning offsets and bucket leader epochs.

## The Read Path
Full data is the RocksDB SST file on the remote storage and incremental data is the log file in the LogStore.. Because we have saved the consistent offset on the LogTablet corresponding to the KvTablet checkpointing during KvStore persistence, we only need to read the data in the LogStore according to the offset in the checkpoint when switching.

**Full data phase:** the connector uses RocksDB to provide SstFileReader to directly read SST files in remote storage, mergeand read multiple files and Flink checkpoint during full data processing, record the location of the file. This process is similar to the stream read of Paimon (In order to simplify the implementation the first phase can read the full amount in the way of load RocksDB with poor performance and stability).

**Incremental phase:** Same as the design of Kafka Consumer, corresponding TabletServer node is requested according to the tablet id, and the TabletServer delivers the data to the consumers according to the offset of the request, the consumer maintains the consumption location information by itself.

**Query pushdown:** then the batch query contains the PK, filter condition you can directly push the PK down to TabletServer. TabletServer directly queries the results returned by RocksDB, which turns into a point query. When only some columns are selected in the streaming query, you can efficiently cut the columns of the row-store log data on the TabletServer to reduce the network bandwidth pressure on the TabletServer











Let's try and visualize the process.
![Write Path](assets/fluss_wr_paths/img4.png)

1. The Client first requests the coordinator server Leader to query the tableassignments of the table T. The client will cache the assignment information locally and the next time the cache will be directly requested and then the cache will be routed to TabletServer.
2. According to the written data key = k1 and the buckets number of the table, the data belongs to Tablet3. And according to the assignments in the previous step, the primary Tablet3 is obtained on TabletServer2. The write request is sent to TabletServer2(TS2).
3. After receiving the request, TS2 first queries the local KvTablet T-3 to obtain the old row, generates CDC data UPDATE_BEFORE and UPDATE_AFTER, and writes them to the LogTablet T-3.
4. TS2 will copy the newly written data T-3 the LogTablet to TS4 and TS1 where the replica tablet is located. After the synchronization is completed, ack is returned to TS2.
5. TS After the ack of the replication factor is collected, the write success message can be returned to the client. That is, the data has been persisted (acting as WAL). At this time TS/CS failover, the data can also be recovered.
6. Writes a new row to the KvTablet.

## Fault Tolerance & Persistence


![Fault Tolerance & Persistence](assets/fluss_wr_paths/img5.png)

The KvTablet (RocksDB) checkpoint also uses the incremental checkpoint mechanism similar
to Flink to reduce data transmission and storage.

During restoration, the TS node of the new primary LogTablet downloads the SST restoration KvTablet from the remote storage
and applies the log after the LogTablet offset to the new KvTablet. The Tablet is then re-served externally.

## Conclusion

