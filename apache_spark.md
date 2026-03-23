# Apache Spark Engineering Handbook
### A Deep Technical Reference for Distributed Systems Engineers

---

**Author's Note**

This handbook is written for engineers who have moved past "getting Spark to run" and now want to understand *why* it runs the way it does — why a job fails at 3 AM, why a shuffle is destroying your cluster, why one executor is doing 80% of the work. This is not a tutorial. It is a systems engineering guide. Every chapter assumes you are comfortable reading code, interpreting execution plans, and making architectural decisions under production pressure.

---

## Table of Contents

- Part 1 — Big Data Foundations
- Part 2 — Apache Spark Overview
- Part 3 — Spark Architecture Deep Dive
- Part 4 — Spark Execution Engine
- Part 5 — Resilient Distributed Datasets (RDD)
- Part 6 — DataFrames and Datasets
- Part 7 — Spark SQL Engine
- Part 8 — Spark Internals (Advanced)
- Part 9 — Spark Memory Management
- Part 10 — Shuffle and Network Communication
- Part 11 — Spark Performance Optimization
- Part 12 — Structured Streaming
- Part 13 — Real-Time Data Pipelines
- Part 14 — Spark with Data Lakes
- Part 15 — Debugging Spark in Production
- Part 16 — Spark UI Deep Dive
- Part 17 — Real-World Case Studies
- Part 18 — Building a Production Data Pipeline
- Part 19 — Best Practices for Spark Engineers
- Part 20 — The Future of Spark

---

# PART 1 — BIG DATA FOUNDATIONS

## 1.1 The Problem with Traditional Databases

For decades, data engineering meant one thing: a relational database on a powerful server. Oracle, SQL Server, and PostgreSQL dominated the landscape. These systems work beautifully when your data fits on a single machine and your query patterns are well-defined.

The model breaks at scale.

Consider a simple user activity log. At 1,000 users, a Postgres table works. At 1 million users, you add indexes and a read replica. At 100 million users, you add sharding. At 1 billion daily events — the scale of any modern consumer application — the relational model collapses under its own weight.

The failure modes are systemic:

| Problem | Single-Node System | Distributed System |
|---|---|---|
| Storage ceiling | Disk capacity of one machine | Petabytes across thousands of nodes |
| Compute ceiling | CPU cores of one machine | Millions of parallel tasks |
| Fault tolerance | Single point of failure | Redundant data and compute |
| Horizontal scale | Vertical only (bigger box) | Add nodes as needed |
| Cost at scale | Exponential (enterprise hardware) | Linear (commodity hardware) |

The transition from databases to distributed systems was not a choice. It was an engineering inevitability forced by data volume growth that outpaced hardware improvements by orders of magnitude.

## 1.2 Limitations of Single-Node Computing

Amdahl's Law defines the ceiling:

```
Speedup = 1 / (S + (1-S)/N)
```

Where `S` is the serial fraction of your program and `N` is the number of processors. Even with infinite processors, if 5% of your program is serial, you cannot achieve more than a 20x speedup. On a single node, you are constrained by:

- **CPU**: Even with 128 cores, you cannot parallelize infinitely
- **RAM**: DRAM capacity limits how much data you can process in-memory
- **Disk I/O**: Sequential read speeds top out around 3–7 GB/s on NVMe
- **Network**: A single node cannot receive data faster than its NIC allows

The insight of distributed computing is simple but profound: instead of making one machine faster, make many machines cooperate.

## 1.3 Distributed Storage Systems

Before distributed compute, there was distributed storage. The Google File System (GFS) paper in 2003 and its open-source implementation, HDFS, established the blueprint:

```
┌─────────────────────────────────────────────────────┐
│                  HDFS Architecture                   │
├──────────────────────────┬──────────────────────────┤
│       NameNode           │                          │
│  (Metadata + Namespace)  │     Client               │
│                          │                          │
│  File → Block mapping    │   /data/events/2024-01   │
│  Block → DataNode map    │    → Block A, B, C       │
└──────────────┬───────────┘                          │
               │                                       │
    ┌──────────┼──────────────┐                       │
    ▼          ▼              ▼                       │
┌────────┐ ┌────────┐ ┌────────┐                     │
│DataNode│ │DataNode│ │DataNode│◄────────────────────┘
│  [A][C]│ │  [A][B]│ │  [B][C]│
│        │ │        │ │        │
└────────┘ └────────┘ └────────┘
 Node 1     Node 2     Node 3
```

Key properties of distributed storage:

- **Replication factor**: Each block (default 128 MB) is stored on 3 nodes
- **Rack-awareness**: Replicas are spread across racks for fault isolation
- **Data locality**: Compute can be moved to where data lives

## 1.4 The Hadoop Ecosystem

Hadoop was the first widely-adopted distributed data platform, consisting of three core components:

```
┌──────────────────────────────────────────────────────────────┐
│                      Hadoop Ecosystem                         │
├──────────────────┬──────────────────┬───────────────────────┤
│      HDFS        │      YARN        │      MapReduce        │
│  Distributed     │  Resource        │  Distributed          │
│  Storage         │  Manager         │  Compute              │
├──────────────────┴──────────────────┴───────────────────────┤
│                   Ecosystem Tools                             │
│                                                               │
│  Hive (SQL)  │  Pig (Scripts)  │  HBase (NoSQL)            │
│  Sqoop (ETL) │  Oozie (Sched)  │  Zookeeper (Coord)        │
└──────────────────────────────────────────────────────────────┘
```

MapReduce, while revolutionary, had a fatal flaw: **every operation materialized intermediate results to disk**. A 10-step pipeline required 10 complete disk read/write cycles. For iterative algorithms (machine learning, graph processing), this was catastrophically slow.

## 1.5 Batch vs Stream Processing

There are fundamentally two data processing paradigms:

**Batch Processing**

Process data collected over a time window (hour, day, week). The full dataset is available before processing begins. Latency is acceptable. Throughput is primary.

```
[Raw Events] → [Accumulate] → [Process Batch] → [Output]
     |              |               |                |
  Real-time      Hours/Days    MapReduce/Spark    Reports
```

**Stream Processing**

Process each event as it arrives, or within a very short window. Latency is critical. The full dataset is never available — you process an infinite, continuous stream.

```
[Event] → [Process] → [Output]
  10ms      50ms       60ms
  Per-event latency
```

Most production systems need both — historical batch for correctness and completeness, streaming for real-time visibility.

## 1.6 Lambda and Kappa Architectures

**Lambda Architecture** (Nathan Marz, 2011)

Runs batch and stream pipelines in parallel and merges their outputs:

```
                    ┌──────────────────┐
                    │   Data Sources   │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              │              ▼
     ┌─────────────┐         │     ┌─────────────┐
     │ Batch Layer │         │     │ Speed Layer │
     │  (Spark)    │         │     │  (Storm/    │
     │             │         │     │   Kafka)    │
     └──────┬──────┘         │     └──────┬──────┘
            │                │            │
            ▼                │            ▼
     ┌─────────────┐         │     ┌─────────────┐
     │  Batch View │         │     │ Real-time   │
     │  (complete, │         │     │    View     │
     │  accurate)  │         │     │  (fast,     │
     └──────┬──────┘         │     │  approx)    │
            │                │     └──────┬──────┘
            └────────────────┴────────────┘
                             │
                      ┌──────▼──────┐
                      │ Query Layer │
                      └─────────────┘
```

**Problems with Lambda**: You maintain two codebases (batch and streaming) that must produce identical results. Operationally expensive, error-prone.

**Kappa Architecture** (Jay Kreps, 2014)

Use only streaming, and reprocess historical data through the stream when needed:

```
[All Data] → [Kafka (durable log)] → [Stream Processor] → [Output]
                     │
                     └── Reprocess from offset 0 for backfill
```

Kappa is simpler but requires your stream processor to be capable of high-throughput batch-like reprocessing — which is exactly what Spark Structured Streaming delivers.

---

# PART 2 — APACHE SPARK OVERVIEW

## 2.1 History and Origin

Apache Spark was created at UC Berkeley's AMPLab in 2009 by Matei Zaharia, who was studying distributed systems and frustrated by the limitations of Hadoop MapReduce. The original motivation was simple: machine learning algorithms are iterative, and iterative algorithms are brutal on MapReduce because every iteration writes intermediate results to HDFS.

The insight in Zaharia's 2012 NSDI paper, *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, was that you could maintain fault tolerance without writing to disk on every step — by tracking the *lineage* of transformations instead of materializing results.

Spark became an Apache project in 2013, hit version 1.0 in 2014, and by 2016 had displaced MapReduce as the dominant distributed compute engine. As of today, it processes more data daily than any other open-source compute framework.

## 2.2 Why Spark is Faster than MapReduce

The performance difference is not architectural magic — it is a consequence of four specific decisions:

| Design Decision | MapReduce | Spark |
|---|---|---|
| Intermediate data | Written to HDFS (disk) | Held in memory (or disk if needed) |
| Programming model | Two stages (Map + Reduce) | Arbitrary DAG of operators |
| Resource allocation | New JVM per job | Long-running executors |
| Optimization | None | Catalyst + Tungsten |

Consider a 5-step ETL pipeline:

```
MapReduce:
Read HDFS → Map → Write HDFS → Read HDFS → Reduce → Write HDFS (x5 times)
10 disk operations

Spark:
Read HDFS → Transform → Transform → Transform → Transform → Write HDFS
2 disk operations (one read, one write)
```

Benchmarks show Spark running 10–100x faster than MapReduce for iterative jobs, and 3–5x faster for single-pass ETL.

## 2.3 The Spark Ecosystem

```
┌─────────────────────────────────────────────────────────────┐
│                      Apache Spark                            │
├──────────────┬──────────────┬──────────────┬───────────────┤
│  Spark SQL   │   Spark      │    MLlib      │   GraphX      │
│  & DataFrames│  Streaming   │  (ML Library) │ (Graph Proc.) │
├──────────────┴──────────────┴──────────────┴───────────────┤
│                     Spark Core                               │
│         (RDD API, Scheduling, Memory Mgmt, I/O)             │
├──────────────┬──────────────┬──────────────┬───────────────┤
│  Standalone  │     YARN     │  Kubernetes   │    Mesos      │
│  Scheduler   │  Resource Mgr│  Orchestrator │  (legacy)     │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

**Spark Core**: The foundation. Handles task scheduling, memory management, fault recovery, I/O interaction, and the RDD API.

**Spark SQL**: Provides a DataFrame/Dataset API and a full SQL interface. Internally powered by the Catalyst optimizer and Tungsten execution engine. This is where most production Spark code lives.

**Spark Streaming / Structured Streaming**: Micro-batch stream processing built on top of the batch engine. Structured Streaming is the modern API — it treats a stream as an unbounded table and applies SQL-like operations.

**MLlib**: Distributed machine learning library. Includes classification, regression, clustering, collaborative filtering, and feature engineering tools. Operates on DataFrames (ML Pipeline API).

**GraphX**: Distributed graph processing API. Useful for PageRank, connected components, and graph analytics. Less commonly used in modern stacks (many teams use GraphFrames instead).

---

# PART 3 — SPARK ARCHITECTURE (DEEP DIVE)

## 3.1 Cluster Architecture Overview

Every Spark application runs in a master-worker topology. There is always exactly one **Driver** and one or more **Executors**.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Spark Application                         │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      Driver JVM                          │    │
│  │                                                          │    │
│  │  ┌──────────────┐   ┌──────────────┐  ┌──────────────┐ │    │
│  │  │  SparkContext │   │ DAG Scheduler│  │Task Scheduler│ │    │
│  │  │  (user code) │──▶│  (stages)    │─▶│  (tasks)     │ │    │
│  │  └──────────────┘   └──────────────┘  └──────┬───────┘ │    │
│  └─────────────────────────────────────────────┼──────────┘    │
│                                                 │                │
└─────────────────────────────────────────────────┼────────────── ┘
                                                  │
                              ┌───────────────────▼──────────────┐
                              │         Cluster Manager           │
                              │    (YARN / K8s / Standalone)      │
                              └───────────────┬──────────────────┘
                                              │
          ┌───────────────────────────────────┼───────────────────┐
          │                                   │                   │
          ▼                                   ▼                   ▼
┌──────────────────┐               ┌──────────────────┐  ┌──────────────────┐
│    Worker Node 1  │               │    Worker Node 2  │  │    Worker Node 3  │
│                   │               │                   │  │                   │
│ ┌───────────────┐ │               │ ┌───────────────┐ │  │ ┌───────────────┐ │
│ │   Executor    │ │               │ │   Executor    │ │  │ │   Executor    │ │
│ │               │ │               │ │               │ │  │ │               │ │
│ │ Task1  Task2  │ │               │ │ Task3  Task4  │ │  │ │ Task5  Task6  │ │
│ │               │ │               │ │               │ │  │ │               │ │
│ │  JVM Heap     │ │               │ │  JVM Heap     │ │  │ │  JVM Heap     │ │
│ └───────────────┘ │               └───────────────────┘  └──────────────────┘
└───────────────────┘
```

## 3.2 The Driver

The Driver is the brain of your Spark application. It runs your `main()` function and is responsible for:

- Maintaining the `SparkContext` (the entry point to all Spark functionality)
- Analyzing user code and building the logical execution plan
- Coordinating with the cluster manager to acquire executor resources
- Scheduling tasks to executors
- Tracking the state of every task
- Collecting results from executors (for actions that return data to the driver)

**Where the Driver runs depends on the deployment mode:**

```
Client Mode:
  Your laptop/submit node ← Driver runs HERE
  |
  Cluster Manager → assigns Executors on workers

Cluster Mode:
  Submit node → sends JAR to cluster
  Cluster Manager → runs Driver on a worker node
                  → assigns other Executors
```

**Production best practice**: Always use cluster mode in production. Client mode ties your job to the submitting process — if the SSH session dies, so does your job.

## 3.3 Executors

Executors are long-running JVM processes that do the actual work:

- Each executor runs on a Worker node
- Each executor is assigned a fixed amount of CPU cores and memory at startup
- Executors run tasks assigned by the Driver
- Executors cache data in memory (RDD/DataFrame cache)
- Executors communicate directly with the Driver for heartbeats and task results
- If an executor crashes, the Driver reschedules its tasks on other executors

**Executor Lifecycle:**

```
Driver requests resources
    │
    ▼
Cluster Manager launches Executor JVM
    │
    ▼
Executor registers with Driver
    │
    ▼
Driver sends tasks to Executor (serialized closures + data locations)
    │
    ▼
Executor executes tasks, writes shuffle data to disk
    │
    ▼
Executor sends task results back to Driver
    │
    ▼
Job completes → Executor JVM exits (or stays warm for next job)
```

## 3.4 Cluster Managers

### Standalone Mode

Spark's built-in cluster manager. Easiest to set up, but limited in features.

```
spark://master:7077
    │
    ├── Worker 1 (8 cores, 64GB)
    ├── Worker 2 (8 cores, 64GB)
    └── Worker 3 (8 cores, 64GB)
```

Use standalone for development clusters or dedicated Spark clusters where you don't need multi-framework resource sharing.

### YARN (Yet Another Resource Negotiator)

The Hadoop ecosystem's resource manager. Dominates in on-premise deployments.

```
YARN Architecture:
┌──────────────────┐
│  ResourceManager │ ← Global resource authority
│  (Master)        │
└────────┬─────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌────────┐
│NodeMgr │ │NodeMgr │  ← Per-node resource management
│        │ │        │
│  Spark │ │  Spark │
│  Exec  │ │  Exec  │
└────────┘ └────────┘
```

Spark's ApplicationMaster runs inside a YARN container and negotiates resources from the ResourceManager.

### Kubernetes

The modern deployment target. Spark has native Kubernetes support since Spark 2.3, dramatically improved in 3.x.

```
┌────────────────────────────────────────────────┐
│              Kubernetes Cluster                  │
│                                                  │
│  ┌─────────────────┐   ┌────────────────────┐  │
│  │  Spark Driver   │   │  Executor Pod 1    │  │
│  │  Pod            │──▶│  (2 cores, 8GB)    │  │
│  │                 │   └────────────────────┘  │
│  │  spark-driver   │   ┌────────────────────┐  │
│  │  -5d7f8c        │──▶│  Executor Pod 2    │  │
│  └─────────────────┘   │  (2 cores, 8GB)    │  │
│                         └────────────────────┘  │
└────────────────────────────────────────────────┘
```

Key advantages of Kubernetes: elastic scaling, container isolation, integrated with cloud-native tooling (Helm, ArgoCD, Prometheus).

---

# PART 4 — SPARK EXECUTION ENGINE

## 4.1 From Code to Tasks: The Full Pipeline

Understanding this pipeline is the most important thing a Spark engineer can know. Every optimization decision, every performance problem, every debugging effort leads back to understanding exactly what happens between "you write code" and "tasks run on executors."

```
User Code (Python/Scala/SQL)
         │
         ▼
   SparkContext / SparkSession
         │
         ▼
   Unresolved Logical Plan
   (Abstract Syntax Tree)
         │
         ▼ (Catalog lookup: resolve table names, column types)
   Resolved Logical Plan
         │
         ▼ (Catalyst Optimizer: rule-based + cost-based)
   Optimized Logical Plan
         │
         ▼ (Planner: select physical operators)
   Physical Plan(s)
         │
         ▼ (Cost Model: pick best physical plan)
   Selected Physical Plan
         │
         ▼ (Tungsten: whole-stage codegen)
   Generated JVM Bytecode
         │
         ▼
   DAG of Stages
         │
         ▼
   Tasks (one per partition per stage)
         │
         ▼
   Executor Threads
```

## 4.2 DAG Scheduler

The DAG (Directed Acyclic Graph) Scheduler translates your logical computation into a graph of stages:

```
Example: Read → Filter → GroupBy → Sort → Write

DAG:
[Read CSV]──▶[Filter]──▶[Map]──▶ // Stage 1 (no shuffle)
                                  //   SHUFFLE BOUNDARY
                         [GroupBy]──▶[Sort]──▶[Write] // Stage 2
```

**Stage boundaries are always at shuffle operations.** A shuffle is any operation that requires data movement between executors (groupBy, join, repartition, distinct, etc.).

Within a stage, all transformations are "pipelined" — data flows from one operator to the next without being materialized. This is why Spark is fast.

The DAG Scheduler is responsible for:

1. Identifying stage boundaries (shuffle dependencies)
2. Determining which stages can run in parallel
3. Identifying preferred data locations (data locality)
4. Submitting stage task sets to the Task Scheduler
5. Re-submitting failed stages

## 4.3 Task Scheduler

The Task Scheduler receives TaskSets from the DAG Scheduler and assigns individual tasks to available executor slots:

```
TaskSet for Stage 2:
  Task(partition=0) → Executor 1, Core 2
  Task(partition=1) → Executor 3, Core 1
  Task(partition=2) → Executor 2, Core 0
  Task(partition=3) → Executor 1, Core 3
  ...
```

The Task Scheduler applies **data locality preferences** in this order:

| Locality Level | Meaning | Performance |
|---|---|---|
| PROCESS_LOCAL | Data in executor's own memory | Best |
| NODE_LOCAL | Data on same physical node | Very Good |
| RACK_LOCAL | Data on same network rack | Good |
| ANY | Data must travel across network | Acceptable |

The scheduler will wait a configurable amount of time (default 3 seconds) for a better locality slot before accepting ANY.

## 4.4 Stage Generation and Shuffle Boundaries

Consider this Spark code:

```python
# PySpark example
df = spark.read.parquet("s3://bucket/events/")
result = (df
  .filter(col("event_type") == "purchase")        # Narrow
  .withColumn("revenue", col("price") * col("qty")) # Narrow
  .groupBy("user_id")                               # SHUFFLE
  .agg(sum("revenue").alias("total_revenue"))       # Reduction
  .orderBy(desc("total_revenue"))                   # SHUFFLE
  .limit(100)
)
result.write.parquet("s3://bucket/top_users/")
```

This creates 3 stages:

```
Stage 0: Read Parquet + Filter + Map (narrow transformations)
  │ ← SHUFFLE (groupBy causes data redistribution by user_id)
Stage 1: Hash aggregate (reduce per partition)
  │ ← SHUFFLE (orderBy requires global sort)
Stage 2: Sort + Limit + Write
```

Understanding where stages begin and end is the first step in every performance investigation.

---

# PART 5 — RESILIENT DISTRIBUTED DATASETS (RDD)

## 5.1 The RDD Abstraction

An RDD (Resilient Distributed Dataset) is the foundational abstraction of Spark. It represents an immutable, partitioned collection of elements that can be operated on in parallel.

The key properties of an RDD:

| Property | Description |
|---|---|
| **List of partitions** | The dataset is split into N partitions across the cluster |
| **Compute function** | A function to compute the data for each partition |
| **Dependencies** | References to parent RDDs (the lineage) |
| **Partitioner** | Optional: how keys are hashed to partitions (for key-value RDDs) |
| **Preferred locations** | Where to run each partition's task (data locality) |

## 5.2 Fault Tolerance Through Lineage

This is Spark's core innovation. Instead of replicating data (expensive), Spark records the *lineage graph* — the sequence of transformations that produced each RDD.

```
RDD[A] ──(filter)──▶ RDD[B] ──(map)──▶ RDD[C] ──(groupBy)──▶ RDD[D]
  │                    │                  │                      │
  └─── stored         lost            computed               stored
       on HDFS                        from B                in memory
```

If partition 3 of RDD[C] is lost (executor crashed), Spark simply re-runs the `map` transformation on the corresponding partition of RDD[B]. If RDD[B] is also lost, it re-runs `filter` on RDD[A] (which is stable, being on HDFS).

This is fundamentally more efficient than full data replication for compute-heavy transformations.

## 5.3 Narrow vs Wide Transformations

This distinction is critical for understanding shuffle and performance.

**Narrow Transformations**

Each input partition contributes to exactly one output partition. No data movement across the network.

```
Partition 0 ──▶ Partition 0
Partition 1 ──▶ Partition 1
Partition 2 ──▶ Partition 2

Examples: map, filter, flatMap, mapPartitions, union
```

**Wide Transformations (Shuffle)**

Each input partition may contribute to multiple output partitions. Requires network transfer.

```
Partition 0 ──▶ Partition 0 (partial)
            ──▶ Partition 1 (partial)
Partition 1 ──▶ Partition 0 (partial)
            ──▶ Partition 2 (partial)
Partition 2 ──▶ Partition 1 (partial)
            ──▶ Partition 2 (partial)

Examples: groupByKey, reduceByKey, join, repartition, distinct, sortBy
```

Wide transformations always create a new stage boundary. They are the primary cause of performance problems in Spark.

## 5.4 Partitioning Strategy

```python
# Scala: Create an RDD with custom partitioning
val rdd = sc.parallelize(data, numPartitions = 200)

# Check partition count
println(rdd.getNumPartitions)  // 200

# Repartition (causes shuffle — use sparingly)
val rdd2 = rdd.repartition(100)

# Coalesce (reduces partitions without full shuffle)
val rdd3 = rdd.coalesce(50)
```

**Partition sizing rule of thumb:**

- Target 128–256 MB of uncompressed data per partition
- Use 2–3x the number of CPU cores for good utilization
- For a 100GB dataset with 200 executor cores: 200–600 partitions

```
Too few partitions:                Too many partitions:
┌─────────────────────┐           ┌──┐ ┌──┐ ┌──┐ ...
│  Partition 0        │           │p0│ │p1│ │p2│
│  80 GB of data      │           └──┘ └──┘ └──┘
│  One executor       │           Each: 1 MB
│  overloaded         │           1000s of tasks
└─────────────────────┘           Scheduling overhead
```

---

# PART 6 — DATAFRAMES AND DATASETS

## 6.1 Beyond RDD: The Case for Structure

RDDs give you maximum flexibility but zero information about the structure of your data. When you have an `RDD[Row]`, Spark knows nothing about the schema — it cannot push down filters, cannot reorder operations, cannot optimize joins.

DataFrames (introduced in Spark 1.3) added schema to distributed data. When Spark knows that a column is a `Long`, it can:

- Store it in columnar format (Parquet-compatible)
- Generate tight bytecode instead of interpreting objects
- Push filters down to storage (skip reading irrelevant data)
- Choose optimal join strategies

```
RDD[Employee]  vs  DataFrame (employees)
─────────────     ─────────────────────────────────────
No schema         Schema: id: Long, name: String,
                          dept: String, salary: Double

No optimization   Catalyst optimizes the query plan
                  Tungsten generates native bytecode
                  Predicate pushdown to Parquet
                  Columnar in-memory storage
```

## 6.2 The Catalyst Optimizer

Catalyst is Spark's query optimizer — a sophisticated rule-based and cost-based optimization engine written in Scala using a functional tree transformation approach.

**Optimization Pipeline:**

```
SQL/DataFrame API
     │
     ▼
Unresolved Logical Plan
     │
     ▼ [Analysis: resolve names against catalog]
Resolved Logical Plan
     │
     ▼ [Logical Optimization: apply rules]
Optimized Logical Plan
     │
     ▼ [Physical Planning: enumerate strategies]
Physical Plans
     │
     ▼ [Cost Model: select best plan]
Best Physical Plan
     │
     ▼ [Code Generation: Tungsten]
Executable Plan
```

**Key Catalyst optimizations:**

| Rule | Description | Example |
|---|---|---|
| Predicate Pushdown | Move filters as early as possible | Filter before join |
| Column Pruning | Remove unused columns early | Drop before shuffle |
| Constant Folding | Evaluate constant expressions | `1+1` → `2` at plan time |
| Join Reordering | Smaller table first in joins | CBO with statistics |
| Partition Pruning | Skip partitions based on predicates | Date range → fewer files |

**PySpark example: see the plan**

```python
df = spark.read.parquet("s3://bucket/events/")
result = df.filter(col("date") == "2024-01-15") \
           .select("user_id", "event_type") \
           .groupBy("user_id") \
           .count()

# View optimized plan
result.explain(mode="extended")

# == Parsed Logical Plan ==
# Aggregate [user_id], [user_id, count(1)]
# +- Project [user_id, event_type]
#    +- Filter (date = 2024-01-15)
#       +- Relation[...] parquet

# == Optimized Logical Plan ==
# Aggregate [user_id], [user_id, count(1)]
# +- Project [user_id]          ← Column pruning: event_type removed
#    +- Filter (date = 2024-01-15)
#       +- Relation[user_id,date] parquet  ← Partition/column pruning
```

## 6.3 Tungsten Execution Engine

Tungsten (introduced in Spark 1.5) addresses JVM inefficiency. The JVM is a general-purpose runtime — it manages memory with garbage collection, stores objects with headers and pointers, and interprets bytecode through an interpreter. None of these are optimal for columnar data processing.

Tungsten bypasses the JVM for data storage using **unsafe (off-heap) memory management**:

```
Standard JVM Object (String "hello"):
┌─────────┬─────────┬─────────────┐
│  Mark   │  Class  │  char[] ref │  + array header + char data
│  word   │  ptr    │             │  = 48 bytes for "hello"
└─────────┴─────────┴─────────────┘

Tungsten Compact Representation:
┌──────────────┬──────────────┐
│ length (4B)  │  "hello" UTF │  = 9 bytes
└──────────────┴──────────────┘
```

**Whole-Stage Code Generation (WSCG)**

Instead of iterating through a chain of operator objects (each with virtual function calls), Tungsten generates a single optimized JVM method for an entire stage:

```scala
// Conceptual view of generated code for: Filter + Map + Aggregate

// Without WSCG (interpreted):
for (row <- rdd) {
  val filtered = filterOperator.process(row)
  if (filtered != null) {
    val mapped = mapOperator.process(filtered)
    aggregateOperator.process(mapped)
  }
}

// With WSCG (compiled):
// One tight loop, no virtual calls, CPU registers hold intermediate state
for (row <- rdd) {
  val value = row.getLong(0)
  if (value > 100) {          // inlined filter
    val newVal = value * 2    // inlined map
    sum += newVal             // inlined aggregate
  }
}
```

WSCG generates code that runs at near-native speed, leveraging CPU instruction pipelines, branch prediction, and L1/L2 cache effectively.

---

# PART 7 — SPARK SQL ENGINE

## 7.1 SQL Query Execution Pipeline

Spark SQL is more than a SQL interface — it is the primary execution path for all structured Spark operations. Whether you write SQL or use the DataFrame API, the execution path converges at the Catalyst optimizer.

```
┌─────────────────────────────────────────────────────────┐
│              SQL Query Execution Pipeline                 │
│                                                           │
│  "SELECT user_id, COUNT(*) FROM events                   │
│   WHERE date = '2024-01-15' GROUP BY user_id"            │
│                    │                                      │
│                    ▼                                      │
│         ┌──────────────────┐                             │
│         │   SQL Parser     │  ANTLR grammar              │
│         │  (Unresolved AST)│                             │
│         └────────┬─────────┘                             │
│                  │                                        │
│                  ▼                                        │
│         ┌──────────────────┐                             │
│         │    Analyzer      │  Catalog: resolve "events"  │
│         │ (Resolved Plan)  │  types: date=Date,          │
│         └────────┬─────────┘         user_id=String      │
│                  │                                        │
│                  ▼                                        │
│         ┌──────────────────┐                             │
│         │    Optimizer     │  Predicate pushdown         │
│         │ (Optimized Plan) │  Column pruning             │
│         └────────┬─────────┘  Partition elimination      │
│                  │                                        │
│                  ▼                                        │
│         ┌──────────────────┐                             │
│         │ Physical Planner │  HashAggregate vs           │
│         │ (Physical Plan)  │  SortAggregate              │
│         └────────┬─────────┘  BroadcastHashJoin vs       │
│                  │            SortMergeJoin               │
│                  ▼                                        │
│         ┌──────────────────┐                             │
│         │  Code Generator  │  Whole-Stage Codegen        │
│         │  (Bytecode)      │                             │
│         └────────┬─────────┘                             │
│                  │                                        │
│                  ▼                                        │
│             Execution on Executors                        │
└─────────────────────────────────────────────────────────┘
```

## 7.2 Reading Query Plans

Reading query plans is a non-negotiable skill for production Spark engineers. Here is how to interpret the output of `explain()`:

```python
df = spark.sql("""
    SELECT u.user_id, u.name, SUM(o.amount) as total
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    WHERE o.status = 'completed'
    GROUP BY u.user_id, u.name
""")

df.explain(True)
```

**Sample Physical Plan output:**

```
== Physical Plan ==
*(3) HashAggregate(keys=[user_id#12, name#13], functions=[sum(amount#45)])
+- Exchange hashpartitioning(user_id#12, name#13, 200), ENSURE_REQUIREMENTS
   +- *(2) HashAggregate(keys=[user_id#12, name#13], functions=[partial_sum(amount)])
      +- *(2) Project [user_id#12, name#13, amount#45]
         +- *(2) BroadcastHashJoin [user_id#12], [user_id#32], Inner, BuildLeft
            :- BroadcastExchange HashedRelationBroadcastMode(List(user_id#12))
            :  +- *(1) Filter isnotnull(user_id#12)
            :     +- *(1) FileScan parquet [user_id#12, name#13] [...]
            +- *(2) Filter ((status#41 = completed) AND isnotnull(user_id#32))
               +- *(2) FileScan parquet [user_id#32, amount#45, status#41] [...]
```

**How to read this:**
- Read from bottom to top (leaves are first operations)
- `*(N)` indicates whole-stage codegen group N
- `Exchange` is a shuffle (stage boundary)
- `BroadcastExchange` means one table is broadcast to all executors (no shuffle)
- `FileScan` shows which columns were read (column pruning in effect)
- `Filter` before `FileScan` = predicate pushdown

## 7.3 Adaptive Query Execution (AQE)

Introduced in Spark 3.0, AQE re-optimizes query plans at runtime based on statistics gathered during execution.

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

AQE provides three major optimizations:

**1. Dynamic Coalescing of Shuffle Partitions**

Before AQE: You guess `spark.sql.shuffle.partitions=200`. If your data is small, 200 tasks each reading 1 MB of shuffle data is wasteful.

After AQE: Spark reads actual shuffle statistics, sees 5 GB of shuffle data, and coalesces to 40 tasks of ~128 MB each.

**2. Dynamic Switch to Broadcast Join**

Before AQE: Spark plans a SortMergeJoin for a table that statistics suggest is large, but after filtering at runtime it's only 5 MB.

After AQE: Spark detects the runtime size, switches to BroadcastHashJoin. No code change required.

**3. Skew Join Optimization**

Before AQE: One partition has 50x more data than others. One task takes 10 minutes while others finish in 10 seconds.

After AQE: Spark detects the skewed partition, splits it into smaller sub-partitions, and processes them in parallel.

---

# PART 8 — SPARK INTERNALS (ADVANCED)

## 8.1 The Shuffle Manager

The Shuffle Manager coordinates the most expensive operation in Spark: data redistribution between stages.

**Sort-Based Shuffle (default since Spark 1.2):**

```
Stage 1 (Map side):
  Each task writes sorted shuffle data to a single file per executor
  A separate index file records partition byte offsets

  Executor 1:
  ┌──────────────────────────────┐
  │ shuffle_0_0_0.data           │
  │ [part0 data][part1 data]...  │
  ├──────────────────────────────┤
  │ shuffle_0_0_0.index          │
  │ [0, 2048, 5120, ...]         │
  └──────────────────────────────┘

Stage 2 (Reduce side):
  Each reduce task fetches its partition from every map task
  Executor doing partition 3 reads offset[3]→offset[4] from every map file
```

**Why sort-based shuffle:**
- One output file per map task (vs O(M×R) files in hash-based)
- At M=1000 map tasks and R=500 reduce partitions: 500 files vs 500,000 files
- File system handles large files more efficiently

## 8.2 The Block Manager

The Block Manager is Spark's distributed storage system — it manages how data is stored and retrieved across the cluster.

```
┌─────────────────────────────────────────────────────────┐
│                      Block Manager                       │
├─────────────────────────────────────────────────────────┤
│  DiskStore          │  MemoryStore                      │
│  (local disk)       │  (JVM heap + off-heap)            │
├─────────────────────────────────────────────────────────┤
│  BlockManagerMaster  (runs on Driver)                   │
│    → Directory: which blocks are on which executors     │
│  BlockManagerSlave   (runs on each Executor)            │
│    → Serves blocks to other executors                   │
└─────────────────────────────────────────────────────────┘
```

When a task needs data that isn't local:
1. Task asks Driver's BlockManagerMaster where the block lives
2. Driver responds with the executor location
3. Task fetches block directly from that executor (peer-to-peer)

This is how cached DataFrames work — they're stored in Block Manager memory, and tasks fetch them via direct executor-to-executor transfer.

## 8.3 Memory Manager

Since Spark 1.6, the **Unified Memory Manager** manages a single memory pool shared between execution and storage:

```
┌─────────────────────────────────────────────────────────┐
│                    Executor JVM Heap                     │
├──────────────────────────────┬──────────────────────────┤
│      Reserved Memory (300MB) │   User Memory            │
│      (Spark internals)       │   (user data structures) │
├──────────────────────────────┴──────────────────────────┤
│              Spark Memory (unified pool)                 │
│                                                          │
│  ┌────────────────────────┬───────────────────────────┐ │
│  │  Execution Memory      │   Storage Memory          │ │
│  │  (shuffle, sort, join) │   (cached data, broadcast)│ │
│  │                        │                           │ │
│  │  ◄──── Can expand ───► │ ◄── Can expand ──►       │ │
│  └────────────────────────┴───────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

The boundary between Execution and Storage memory is **dynamic**:
- Execution memory can evict cached storage blocks if it needs space
- Storage memory can borrow unused execution memory
- This prevents the old static division problem where execution was starved while storage sat idle

---

# PART 9 — SPARK MEMORY MANAGEMENT

## 9.1 Memory Regions Explained

```
spark.executor.memory = 8g   (total JVM heap)

┌──────────────────────────────────────────────────────┐
│  Total JVM Heap: 8 GB                                │
├──────────────────────────────────────────────────────┤
│  Reserved: 300 MB  (Spark internal objects)          │
├──────────────────────────────────────────────────────┤
│  User Memory: ~1.9 GB  (25% of remainder)            │
│  → Your UDFs, Python objects, non-Spark structures   │
├──────────────────────────────────────────────────────┤
│  Spark Unified Memory: ~5.8 GB  (75% of remainder)   │
│                                                       │
│  Storage:  2.9 GB  (cached DFs, broadcast vars)      │
│  Execution: 2.9 GB  (shuffle buffers, sort, join)    │
│  (boundary is fluid)                                  │
└──────────────────────────────────────────────────────┘
```

Additionally, `spark.executor.memoryOverhead` (default: max(10% of executor memory, 384MB)) accounts for JVM overhead, interned strings, off-heap overhead, Python worker memory (for PySpark), and container overhead.

**For PySpark, always add:**

```
spark.executor.memory = 8g
spark.executor.memoryOverhead = 2g   ← Python worker process memory
```

## 9.2 Spill to Disk

When execution memory is insufficient for an operation (sort, hash aggregate, shuffle), Spark "spills" intermediate data to disk:

```
Shuffle Read Buffer fills up:
  1. Sort data accumulated so far
  2. Write sorted run to disk (spill file)
  3. Free memory
  4. Continue accumulating
  5. At end: merge all spill files + remaining in-memory data
```

Spill is not fatal — Spark handles it automatically. But it is **slow** (2–10x slower than in-memory operations). You'll see it in Spark UI as "Spill (Memory)" and "Spill (Disk)" in task metrics.

**Debugging spill:**

```python
# Check for spill in your job
# Spark UI → Stages → Click stage → Look at task metrics:
#   Spill (Memory): bytes spilled from memory
#   Spill (Disk):   bytes written to disk

# Fix: Increase executor memory or reduce partition size
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.sql.shuffle.partitions", "400")  # smaller partitions
```

## 9.3 Garbage Collection Tuning

GC is one of the most common causes of "slow Spark jobs that have no obvious bottleneck." When GC pauses, executors stop responding to heartbeats, tasks appear hung, and the Driver may mark executors as lost.

**Diagnosing GC issues:**

```bash
# In spark-submit, enable GC logging:
--conf "spark.executor.extraJavaOptions=-verbose:gc 
  -XX:+PrintGCDetails 
  -XX:+PrintGCDateStamps 
  -XX:+PrintGCTimeStamps"
```

In Spark UI, look at Executor tab → GC Time. If GC time > 10% of task time, you have a GC problem.

**GC tuning strategies:**

```bash
# Use G1GC (recommended for Spark 3.x):
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC 
  -XX:InitiatingHeapOccupancyPercent=35
  -XX:ConcGCThreads=4"

# Reduce GC pressure:
# 1. Use off-heap memory for storage (Tungsten)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

# 2. Reduce object creation in UDFs (avoid Python → JVM round-trips)
# 3. Use primitive types instead of boxed types in DataFrames
```

## 9.4 Real-World OOM Scenarios

**Scenario 1: Broadcast join table too large**

```
ERROR: java.lang.OutOfMemoryError: Not enough memory to build 
and broadcast the table to all worker nodes.

Cause: spark.sql.autoBroadcastJoinThreshold = 10MB by default.
       A table is 8MB → Spark broadcasts it.
       All 200 executors load 8MB → 1.6 GB cluster-wide.
       If executors have small memory, this causes OOM.

Fix:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable
# Or increase executor memory
```

**Scenario 2: Data skew in groupBy**

```
Scenario: 1 billion rows, groupBy user_id
          User ID "bot_account_1" has 500 million rows (50% of data)

Symptom: 199 tasks finish in 30 seconds. 1 task runs for 2 hours, OOMs.

Fix: Salt the key
```

```python
# Salting to handle skew
from pyspark.sql.functions import col, concat, lit, floor, rand, broadcast

SALT_FACTOR = 100

# Add random salt to the skewed table
df_skewed = df_skewed.withColumn("salt", (rand() * SALT_FACTOR).cast("int"))
df_skewed = df_skewed.withColumn("salted_key", 
    concat(col("user_id"), lit("_"), col("salt")))

# Explode the small table with all salt values
from pyspark.sql.functions import array, explode
df_small = df_small.withColumn("salt_arr", 
    array([lit(i) for i in range(SALT_FACTOR)]))
df_small = df_small.withColumn("salt", explode("salt_arr"))
df_small = df_small.withColumn("salted_key", 
    concat(col("user_id"), lit("_"), col("salt")))

# Now join on salted key — data is evenly distributed
result = df_skewed.join(df_small, "salted_key")
```

---

# PART 10 — SHUFFLE AND NETWORK COMMUNICATION

## 10.1 Why Shuffle is Expensive

Shuffle is the most expensive operation in distributed computing. It involves:

1. **CPU**: Sorting and hashing records to determine destination partitions
2. **Memory**: Buffering records before writing
3. **Disk I/O**: Writing shuffle output files; reading them in next stage
4. **Network**: Transferring data between nodes

```
Cost breakdown of a shuffle:

Map Phase (write):
  [Sort/Hash records by key] 
  [Write sorted data + index to local disk]
  
  Disk write: O(data size)
  CPU: O(N log N) for sort

Network Transfer:
  Every reduce task connects to every map task
  [Fetch partition data] over network
  
  Network: O(data size)

Reduce Phase (read):
  [Receive data from all map tasks]
  [Merge and sort]
  [Aggregate]
  
  CPU + Memory: O(N log N) again
```

A 1 TB shuffle with 200 executors involves:
- 200 executors writing 5 GB each to local disk
- 200 reduce tasks each fetching 5 GB from 200 sources (40,000 fetch requests)
- 1 TB of network traffic

This is why avoiding unnecessary shuffles is the most impactful performance optimization you can make.

## 10.2 Shuffle Optimization Strategies

**Strategy 1: Reduce the amount of data before shuffle**

```python
# BAD: shuffle all columns, then filter
df.groupBy("user_id").agg(collect_list("event"))
  .filter(size("event") > 10)

# GOOD: filter first, then shuffle less data
df.filter(/* pre-filter to reduce rows */)
  .groupBy("user_id").agg(collect_list("event"))
  .filter(size("event") > 10)
```

**Strategy 2: Use reduceByKey instead of groupByKey (RDD API)**

```scala
// BAD: groupByKey shuffles ALL values across network
rdd.groupByKey().mapValues(_.sum)

// GOOD: reduceByKey does partial aggregation before shuffle
rdd.reduceByKey(_ + _)

// With groupByKey: 100GB data → 100GB shuffled
// With reduceByKey: 100GB data → 1GB shuffled (after partial agg)
```

**Strategy 3: Bucketing for repeated joins**

```python
# Write bucketed tables (one-time cost)
df_orders.write \
  .bucketBy(200, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("orders_bucketed")

df_users.write \
  .bucketBy(200, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("users_bucketed")

# Now join them — no shuffle!
spark.sql("""
  SELECT * FROM orders_bucketed o
  JOIN users_bucketed u ON o.user_id = u.user_id
""").explain()
# == Physical Plan ==
# SortMergeJoin (NO Exchange/shuffle operators!)
```

Bucketing eliminates the shuffle for all future joins on the bucketed column. The one-time write cost pays dividends for every subsequent join.

---

# PART 11 — SPARK PERFORMANCE OPTIMIZATION

## 11.1 The Performance Optimization Hierarchy

Not all optimizations are equal. Work top-down:

```
Tier 1: Data Architecture (10–1000x impact)
  → Right file format (Parquet > CSV > JSON)
  → Right partitioning scheme
  → Data pruning at source

Tier 2: Query Logic (2–100x impact)
  → Eliminate unnecessary shuffles
  → Fix data skew
  → Use broadcast joins for small tables

Tier 3: Configuration Tuning (1.1–3x impact)
  → Executor memory/cores
  → Shuffle partitions
  → GC settings

Tier 4: Code Style (1.1–1.5x impact)
  → Avoid UDFs where SQL functions exist
  → Prefer DataFrames over RDD
  → Cache wisely
```

## 11.2 File Format and Compression

```
File Format Comparison:

Format      Read Speed   Write Speed   Size      Predicate Pushdown
──────────────────────────────────────────────────────────────────
CSV         Slow         Fast          Large     No
JSON        Slow         Medium        Large     No
Avro        Medium       Fast          Medium    No (row-based)
Parquet     Fast         Medium        Small     Yes (column stats)
ORC         Fast         Medium        Small     Yes (column stats)
Delta Lake  Fast         Fast          Small     Yes + ACID

Winner for analytics: Parquet / Delta Lake
```

```python
# Always use Parquet with snappy compression
df.write \
  .format("parquet") \
  .option("compression", "snappy") \
  .partitionBy("date", "region") \
  .save("s3://bucket/events/")

# Parquet predicate pushdown example:
df = spark.read.parquet("s3://bucket/events/")
result = df.filter(col("date") == "2024-01-15")
# Spark reads ONLY the 2024-01-15 partition folder
# AND uses Parquet min/max statistics to skip row groups
```

## 11.3 Broadcast Joins

Broadcast joins eliminate shuffle for one side of a join by sending the smaller table to every executor:

```
Regular SortMergeJoin (shuffle both sides):
┌─────────┐   Shuffle   ┌─────────┐
│ Table A │────────────▶│ Merged  │
│  100 GB │             │  Join   │
│ Table B │────────────▶│         │
│   50 GB │   Shuffle   └─────────┘
2 shuffles, 150 GB of network traffic

BroadcastHashJoin (broadcast small table):
┌─────────┐   Broadcast  ┌─────────┐
│ Table A │─────────────▶│ Join    │
│  100 GB │  (no shuffle)│ in each │
│ Table B │─────────────▶│executor │
│   5 MB  │              └─────────┘
0 shuffles, 5 MB × num_executors of network traffic
```

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
result = large_df.join(
    broadcast(small_df),
    on="user_id",
    how="inner"
)

# Or configure auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
```

## 11.4 Partition Tuning

```python
# Rule: ~128MB per partition of uncompressed data
# For shuffle output: 200 partitions default is often wrong

# Too few partitions → OOM, slow
spark.conf.set("spark.sql.shuffle.partitions", "50")  # bad for 500GB

# Too many partitions → scheduling overhead
spark.conf.set("spark.sql.shuffle.partitions", "50000")  # bad for 5GB

# Right-size based on data:
# 500GB dataset → 500000MB / 128MB ≈ 4000 partitions
spark.conf.set("spark.sql.shuffle.partitions", "4000")

# With AQE enabled (Spark 3.2+), this is auto-tuned:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

## 11.5 Caching Strategy

```python
import pyspark.StorageLevel as SL

# Storage levels in order of performance:
# MEMORY_ONLY          → Fastest, OOM risk
# MEMORY_AND_DISK      → Safe, good balance
# MEMORY_ONLY_SER      → Compressed (Java serialization), slower CPU
# DISK_ONLY            → Slowest, lowest memory cost
# OFF_HEAP             → Bypass GC, requires off-heap configuration

# Cache a DataFrame used multiple times
dim_users = spark.read.parquet("s3://bucket/dim/users/")
dim_users.cache()  # Default: MEMORY_AND_DISK
dim_users.count()  # Trigger materialization

# Use in multiple jobs
result1 = events.join(dim_users, "user_id")
result2 = clicks.join(dim_users, "user_id")

# Release when done
dim_users.unpersist()
```

**When NOT to cache:**
- When a dataset is only read once
- When a dataset is larger than available memory (causes eviction thrashing)
- When data is on fast local storage (NVMe) — reading from disk is fast enough

---

# PART 12 — SPARK STRUCTURED STREAMING

## 12.1 Streaming as Unbounded Tables

Spark Structured Streaming treats a stream as an ever-growing table:

```
Time:   T1          T2          T3
        ┌────────┐  ┌────────┐  ┌────────┐
New     │ event1 │  │ event3 │  │ event5 │
Data:   │ event2 │  │ event4 │  │ event6 │
        └────────┘  └────────┘  └────────┘
             │           │           │
             ▼           ▼           ▼
         ┌─────────────────────────────────┐
         │         Input Table             │
         │ event1, event2, event3, event4, │
         │ event5, event6...               │
         └────────────────┬────────────────┘
                          │ Query runs continuously
                          ▼
         ┌────────────────────────────────┐
         │         Result Table           │
         │ Updated incrementally          │
         └───────────────────────────────┘
                          │
                          ▼
                     Output Sink
```

This mental model is powerful: you write the same SQL/DataFrame logic for both batch and stream, and Spark handles the incremental execution.

## 12.2 Micro-Batch Model

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count

spark = SparkSession.builder \
    .appName("EventCounter") \
    .getOrCreate()

# Read from Kafka (stream source)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .load()

# Parse and transform
from pyspark.sql.functions import from_json, schema_of_json
schema = "user_id STRING, event_type STRING, timestamp TIMESTAMP"

parsed = df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Windowed aggregation
agg = parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .agg(count("*").alias("event_count"))

# Write to output sink
query = agg \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/event_counter/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start("s3://bucket/event_counts/")

query.awaitTermination()
```

## 12.3 Watermarking and Late Data

In streaming, events arrive out of order. A click at 14:05:30 might reach your Kafka topic at 14:08:45. Watermarking tells Spark when it's safe to consider a time window "complete":

```
Events arriving at processing time 14:10:

Event A: event_time=14:05, processed_at=14:06  ← 1 min late, OK
Event B: event_time=14:03, processed_at=14:10  ← 7 min late, OK
Event C: event_time=13:55, processed_at=14:10  ← 15 min late, DROPPED

Watermark = max_event_time_seen - threshold
         = 14:05 - 10 minutes
         = 13:55

Any event with event_time < 13:55 is dropped (too late).
Windows that end before 13:55 are finalized and emitted.
```

```python
# With 10-minute watermark:
.withWatermark("timestamp", "10 minutes")

# Windows close and results are emitted only after:
# window_end + watermark_delay has passed in event time
```

## 12.4 Output Modes

| Mode | Description | Use Case |
|---|---|---|
| Append | Only new rows are written | Immutable events, write-once sinks |
| Complete | Entire result table is written | Aggregations, small result sets |
| Update | Only changed rows are written | Stateful aggregations with updates |

## 12.5 Exactly-Once Semantics

Spark Structured Streaming provides exactly-once end-to-end guarantees when:

1. **Source** is replayable (Kafka, files — not socket sources)
2. **Sink** supports idempotent writes or transactional writes (Delta Lake, JDBC with upsert)
3. **Checkpointing** is enabled

```
Checkpoint stores:
  - Current stream offsets (what has been read from Kafka)
  - State store snapshots (for aggregations)
  - Pending uncommitted writes

On restart after failure:
  1. Read checkpoint → know last committed offset
  2. Re-read from that offset (replay)
  3. Re-process and re-write (idempotent write handles duplicates)
```

---

# PART 13 — REAL-TIME DATA PIPELINES

## 13.1 Production Streaming Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Real-Time Data Pipeline                        │
│                                                                   │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                │
│  │  Mobile    │  │  Web       │  │  IoT       │  Event         │
│  │  Apps      │  │  Servers   │  │  Devices   │  Producers     │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘                │
│        └───────────────┴───────────────┘                        │
│                         │                                        │
│                         ▼                                        │
│              ┌─────────────────────┐                            │
│              │    Kafka Cluster    │  Durable Message Log       │
│              │    (3 brokers,      │  Retention: 7 days         │
│              │     replication=3)  │  Throughput: 1M msg/s      │
│              └──────────┬──────────┘                            │
│                         │                                        │
│               ┌─────────┼──────────┐                           │
│               │         │          │                            │
│               ▼         ▼          ▼                            │
│        ┌──────────┐  ┌──────┐  ┌──────────┐                   │
│        │  Spark   │  │Flink │  │ Spark    │  Stream           │
│        │Structured│  │(CEP) │  │Streaming │  Processors        │
│        │Streaming │  │      │  │(ML Serve)│                   │
│        └────┬─────┘  └──────┘  └──────────┘                   │
│             │                                                    │
│             ▼                                                    │
│    ┌──────────────────────────────────────┐                     │
│    │         Data Lake (S3/GCS/ADLS)      │                     │
│    │                                       │                     │
│    │  ┌────────────┐  ┌────────────────┐  │                     │
│    │  │  Raw Layer │  │ Curated Layer  │  │                     │
│    │  │  (Parquet) │  │  (Delta Lake)  │  │                     │
│    │  └────────────┘  └────────────────┘  │                     │
│    └───────────────────┬──────────────────┘                     │
│                        │                                         │
│               ┌────────┼────────┐                               │
│               │        │        │                               │
│               ▼        ▼        ▼                               │
│          ┌───────┐ ┌───────┐ ┌───────┐                         │
│          │Druid  │ │Click  │ │Trino  │  Analytics Engines      │
│          │(OLAP) │ │House  │ │(SQL)  │                         │
│          └───┬───┘ └───────┘ └───────┘                         │
│              │                                                   │
│              ▼                                                   │
│    ┌──────────────────┐                                         │
│    │   BI Dashboard   │  Superset / Grafana / Tableau          │
│    └──────────────────┘                                         │
└─────────────────────────────────────────────────────────────────┘
```

## 13.2 Kafka as the Data Backbone

Kafka is a distributed, append-only log. It decouples producers from consumers and enables replay:

```
Topic: user_events (6 partitions, 3 replicas)

Partition 0: [msg0][msg1][msg2][msg3]...  → offset
Partition 1: [msg0][msg1][msg2]...
...
Partition 5: [msg0][msg1]...

Consumer Group A (Spark Streaming):
  Reads all 6 partitions in parallel
  Commits offsets after processing
  
Consumer Group B (Flink - CEP):
  Reads all 6 partitions independently
  Different offsets from Group A
```

Spark reads from Kafka using the `kafka-clients` library and stores per-partition offsets in its checkpoint directory.

---

# PART 14 — SPARK WITH DATA LAKES

## 14.1 The Lakehouse Architecture

The data lake promised cheap, flexible storage. It delivered cheap storage but created quality and reliability problems: no ACID, no schema enforcement, no efficient updates. The Lakehouse architecture adds a transactional metadata layer on top of object storage:

```
Traditional Architecture:
┌──────────┐   ETL   ┌────────────┐
│Data Lake │────────▶│ Data Warehouse│
│  (cheap) │         │  (expensive)  │
└──────────┘         └──────────────┘
Two copies, expensive, stale data

Lakehouse Architecture:
┌──────────────────────────────────────────────────┐
│           Object Storage (S3/GCS/ADLS)           │
│                                                    │
│  ┌────────────────────────────────────────────┐  │
│  │  Table Format Layer (Delta/Iceberg/Hudi)   │  │
│  │  ● ACID transactions                       │  │
│  │  ● Schema evolution                        │  │
│  │  ● Time travel                             │  │
│  │  ● Efficient upserts                       │  │
│  └────────────────────────────────────────────┘  │
│                                                    │
│  Data Files (Parquet)                             │
│  _delta_log/ or metadata/ (transaction log)       │
└──────────────────────────────────────────────────┘
```

## 14.2 Delta Lake

Delta Lake (open source, created by Databricks) is the most widely deployed table format. Its core mechanism is the **transaction log** — a sequential log of every operation:

```
s3://bucket/my_table/
  ├── _delta_log/
  │   ├── 00000000000000000000.json  ← initial table creation
  │   ├── 00000000000000000001.json  ← first write (adds 10 files)
  │   ├── 00000000000000000002.json  ← second write (adds 5 files)
  │   ├── 00000000000000000010.json  ← checkpoint (snapshot of state)
  │   └── 00000000000000000015.json  ← current version
  ├── part-00001.parquet
  ├── part-00002.parquet
  └── ...
```

```python
# Write to Delta Lake
df.write.format("delta").save("s3://bucket/events/")

# ACID update/delete/merge
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/events/")

# Upsert (merge)
dt.alias("target").merge(
    updates_df.alias("source"),
    "target.user_id = source.user_id AND target.date = source.date"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Time travel
df_yesterday = spark.read \
    .format("delta") \
    .option("versionAsOf", 10) \
    .load("s3://bucket/events/")

# Optimize (compaction + Z-ordering)
dt.optimize().zOrderBy("user_id", "date").executeCompaction()

# Vacuum (remove old files)
dt.vacuum(168)  # delete files older than 168 hours
```

## 14.3 Apache Iceberg

Iceberg (open source, created at Netflix) is a competing table format with strong multi-engine support (Spark, Flink, Trino, Hive, etc.):

```python
# Read/write Iceberg tables
spark.conf.set("spark.sql.catalog.my_catalog", 
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.my_catalog.type", "hadoop")
spark.conf.set("spark.sql.catalog.my_catalog.warehouse", 
               "s3://bucket/warehouse/")

# Create table
spark.sql("""
  CREATE TABLE my_catalog.db.events (
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP,
    date DATE
  ) USING iceberg
  PARTITIONED BY (date)
""")

# Time travel
spark.read \
    .option("snapshot-id", "123456789") \
    .table("my_catalog.db.events")

# Schema evolution (add column — no rewrite needed)
spark.sql("ALTER TABLE my_catalog.db.events ADD COLUMN region STRING")
```

**Delta vs Iceberg comparison:**

| Feature | Delta Lake | Apache Iceberg |
|---|---|---|
| Multi-engine | Spark-primary | Excellent (Flink, Trino, Hive) |
| ACID | Yes | Yes |
| Time travel | Yes | Yes |
| Schema evolution | Yes | Yes (more flexible) |
| Hidden partitioning | No | Yes |
| Merge-on-read | Yes | Yes |
| Ecosystem | Databricks-led | Apache-led |

---

# PART 15 — DEBUGGING SPARK IN PRODUCTION

## 15.1 A Systematic Debugging Framework

When a Spark job fails or is slow, apply this framework:

```
Step 1: Identify the symptom
  ● Job failed    → look for ERROR in logs
  ● Job is slow   → look for long-running stages in Spark UI
  ● Job uses too much memory → look for OOM errors, GC time
  ● Job produces wrong data → data quality issue, not Spark issue

Step 2: Locate the problem
  ● Spark UI → Jobs → find the failing/slow job
  ● Spark UI → Stages → find the slow/failed stage
  ● Spark UI → Tasks → find the slow/failed task
  ● Executor logs → find stack traces

Step 3: Diagnose the root cause
  ● Is there data skew? (task duration variance)
  ● Is there shuffle spill? (Spill metrics in task view)
  ● Is there OOM? (GC time, executor lost)
  ● Is there a slow data source? (Input bytes vs task duration)

Step 4: Fix and verify
  ● Apply the appropriate fix
  ● Re-run and compare metrics
```

## 15.2 Debugging Data Skew

**Symptoms:**
- In Stage view, most tasks finish quickly but 1–5 tasks run for much longer
- Task duration chart shows a "long tail"

```
Task Duration Histogram (skew present):

     ▓
     ▓
     ▓
     ▓ ▓
     ▓ ▓ ▓
     ▓ ▓ ▓ ▓
     ▓ ▓ ▓ ▓ ▓         ▓ (this task: 100x longer)
     ──────────────────────
     0s       30s    3000s
```

**Diagnosis:**
```python
# Find the skewed key
df.groupBy("join_key").count().orderBy(desc("count")).show(20)

# Output:
# +──────────────┬────────────┐
# │ join_key     │ count      │
# +──────────────┼────────────┤
# │ "bot_user"   │ 500000000  │  ← 50% of all data!
# │ "user_12345" │ 1500       │
# │ "user_67890" │ 1200       │
```

**Fixes:**

1. **Filter out the hot key** (if it's noise/junk):
```python
df = df.filter(col("user_id") != "bot_user")
```

2. **Salt the key** (for aggregations):
```python
# See example in Part 9.4
```

3. **Use AQE skew join optimization** (Spark 3.0+):
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

## 15.3 Debugging Executor Crashes

**Symptoms:** `ERROR: Lost executor N on host M: Executor heartbeat timed out after X ms`

**Common causes:**

| Cause | Indicator | Fix |
|---|---|---|
| OOM (heap) | `java.lang.OutOfMemoryError` in executor log | Increase `spark.executor.memory` |
| OOM (overhead) | Container killed by YARN/K8s | Increase `memoryOverhead` |
| GC thrashing | High GC time in executor tab | G1GC tuning, reduce object creation |
| Disk full | `No space left on device` | Add local disk, tune temp dir |
| Network timeout | Long GC pause exceeds heartbeat | Increase `spark.network.timeout` |

```bash
# Retrieve executor logs (YARN):
yarn logs -applicationId application_XXX -containerId container_YYY

# Kubernetes:
kubectl logs spark-executor-pod-name --previous  # crashed pod logs
```

## 15.4 Long Shuffle Stages

**Symptoms:**
- Stage has high "Shuffle Read" or "Shuffle Write" size
- Shuffle stage takes many minutes while individual task computation is fast

**Diagnosis checklist:**

```
1. What is the shuffle size?
   Spark UI → Stages → Shuffle Read/Write column
   
   > 100 GB of shuffle → consider if it's avoidable
   
2. What operation caused the shuffle?
   df.explain() → look for Exchange operators
   
3. Can the join be broadcast?
   If smaller side < 200 MB → use broadcast()
   
4. Can the shuffle be avoided with bucketing?
   If this join runs daily → consider bucketing
   
5. Is the partition count correct?
   200 shuffle partitions for 500 GB → too few, tasks are huge
   200 shuffle partitions for 500 MB → too many, tasks are tiny
```

---

# PART 16 — SPARK UI DEEP DIVE

## 16.1 Navigating the Spark UI

The Spark UI (default port 4040, or History Server port 18080) is your primary debugging tool. A senior engineer can diagnose most problems in 5 minutes using only the Spark UI.

```
http://driver-host:4040/

Tab structure:
├── Jobs          → Overview of all jobs (one per action)
├── Stages        → Stages within jobs + task metrics
├── Storage       → Cached DataFrames and RDDs
├── Environment   → Spark config, JVM settings
├── Executors     → Per-executor metrics, GC, storage
└── SQL/DataFrame → Visual query plan + SQL metrics
```

## 16.2 The Jobs Tab

```
Jobs Tab:
┌──────────────────────────────────────────────────────────────┐
│ Job ID │ Description     │ Submitted │ Duration │ Stages    │
├────────┼─────────────────┼───────────┼──────────┼───────────┤
│   0    │ csv at MyJob:45 │  14:00:01 │  2.1 min │  2/2 ✓   │
│   1    │ parquet at...   │  14:02:05 │ 15.3 min │  3/4 ⚠   │ ← slow!
│   2    │ show at...      │  14:17:30 │   3 sec  │  1/1 ✓   │
└──────────────────────────────────────────────────────────────┘
```

Click on a slow job → see its stages. The "Event Timeline" view shows which stages ran in parallel and which were sequential.

## 16.3 The Stages Tab

The Stages tab is where you diagnose data skew, shuffle issues, and task slowness:

```
Stage 2 (groupBy + sort):
┌────────────────────────────────────────────────────────────────┐
│ Duration: 12 min  │ Tasks: 200  │ Input: 50 GB │ Shuffle: 80GB│
├────────────────────────────────────────────────────────────────┤
│ Task Metrics Summary:                                          │
│                   Min    25th    Median    75th    Max         │
│ Duration:         0.1s   1.2s    1.5s      1.8s    487s ←skew │
│ Shuffle Read:     10MB   85MB    120MB     150MB   42GB  ←skew │
│ GC Time:          0.1s   0.2s    0.3s      0.5s    45s  ←gc   │
└────────────────────────────────────────────────────────────────┘
```

Key things to look for:
- **Duration Max >> Duration 75th**: Data skew
- **Shuffle Read Max >> Shuffle Read 75th**: Partition skew
- **GC Time > 10% of Duration**: GC pressure
- **Spill (Memory) > 0**: Insufficient execution memory

## 16.4 The Executors Tab

```
Executors Tab:
┌──────────────────────────────────────────────────────────────────┐
│ ID │ Address     │ RDD Blocks │ Storage │ Disk Used │ GC Time   │
├────┼─────────────┼────────────┼─────────┼───────────┼───────────┤
│  0 │ worker1:    │   1,245    │ 8.2 GB  │  2.1 GB   │  2%       │
│  1 │ worker2:    │   1,249    │ 8.1 GB  │  2.0 GB   │  3%       │
│  2 │ worker3:    │     0      │   0 B   │    0 B    │ 78% ←BAD  │
│  3 │ worker4:    │   1,247    │ 8.3 GB  │  2.2 GB   │  2%       │
└──────────────────────────────────────────────────────────────────┘
```

Executor 2's high GC time with no cached data suggests it's processing large amounts of data with insufficient heap, causing constant GC cycles.

## 16.5 The SQL/DataFrame Tab

For structured queries, the SQL tab shows the visual query plan with actual runtime metrics:

```
PhotonScan → Filter → HashAggregate → Exchange → HashAggregate
  
Each operator shows:
  number of output rows
  time spent in operator
  bytes processed

This lets you identify:
  - WHERE the most time is spent (slow operators)
  - Which filters are effective (row counts before/after)
  - Whether statistics are accurate (estimated vs actual rows)
```

---

# PART 17 — REAL-WORLD CASE STUDIES

## 17.1 Netflix: Media Encoding Analytics Pipeline

Netflix processes billions of streaming events daily. Their Spark infrastructure handles:
- Viewing session data (start, pause, seek, buffering events)
- A/B test analysis across hundreds of simultaneous experiments
- Content recommendation feature generation

**Architecture pattern:**

```
User Devices (200M+)
       │
       ▼
   Kafka Topics (100+ topics, 50TB/day ingestion)
       │
       ├──▶ Spark Structured Streaming (real-time A/B metrics)
       │    └──▶ Druid (30-second dashboard refresh)
       │
       └──▶ S3 Raw Zone (Parquet, partitioned by hour)
                │
                ▼
            Spark Batch (daily ETL → Iceberg tables)
                │
                ├──▶ Feature Store (ML features)
                └──▶ Hive Metastore (SQL access via Spark SQL)
```

**Netflix's key Spark decisions:**
1. Iceberg as the table format for its strong multi-engine support (Spark + Trino + Flink)
2. Dynamic partition overwrite to avoid stale data issues
3. Custom Spark memory profiling using Java flight recorder
4. "Keystone" pipeline: all data flows through Kafka first, giving them replay capability

## 17.2 Uber: Real-Time Geospatial Aggregations

Uber's core challenge: aggregate 1M+ location pings per second by geospatial region for surge pricing, driver dispatch, and ETA prediction.

**Key technical challenges:**

1. **Geospatial partitioning**: Standard hash partitioning scatters nearby lat/lng coordinates across all partitions, destroying data locality for geo joins.

**Solution**: H3 hexagonal index (Uber open-source):

```python
from h3 import h3

# Convert lat/lng to H3 cell at resolution 7 (~1km2)
def lat_lng_to_h3(lat, lng, resolution=7):
    return h3.geo_to_h3(lat, lng, resolution)

lat_lng_to_h3_udf = udf(lat_lng_to_h3, StringType())

df = df.withColumn("h3_cell", lat_lng_to_h3_udf(col("lat"), col("lng")))
# Now group by h3_cell — spatially close events are in the same partition
result = df.groupBy("h3_cell").agg(
    count("*").alias("active_drivers"),
    avg("price").alias("surge_multiplier")
)
```

2. **Sub-minute latency for surge pricing**: Kafka → Flink (CEP for trip state machine) → Spark (micro-batch, 30s trigger) → Redis (serving layer).

## 17.3 Airbnb: Search Ranking Feature Pipeline

Airbnb's search ranking model uses hundreds of real-time and pre-computed features. Spark generates the pre-computed features (listing quality scores, host response rates, historical booking rates).

**Scale:** 10 million listings × 200 features × daily refresh = 2 billion feature values per day.

**Key optimization: Dimensional modeling with Spark:**

```python
# Instead of one massive join (OOM risk):
# listings × bookings × reviews × host_profiles

# Build intermediate aggregates first:
host_metrics = (
    bookings.groupBy("host_id")
    .agg(
        count("*").alias("total_bookings"),
        avg("rating").alias("avg_rating"),
        countDistinct("guest_id").alias("unique_guests")
    )
    .cache()  # Used in multiple downstream joins
)

listing_metrics = (
    bookings.groupBy("listing_id")
    .agg(...)
    .join(broadcast(listing_meta), "listing_id")
    .cache()
)

# Final feature assembly (smaller data, no OOM)
features = (
    listings
    .join(listing_metrics, "listing_id")
    .join(host_metrics, "host_id")
)
```

## 17.4 LinkedIn: Data Validation at Scale

LinkedIn runs Spark jobs across thousands of data pipelines. Their key contribution: **Great Expectations + Spark** for automated data quality.

```python
import great_expectations as ge

# Validate data quality inline in Spark pipelines
def validate_events(df):
    ge_df = ge.dataset.SparkDFDataset(df)
    
    results = ge_df.expect_column_values_to_not_be_null("user_id")
    assert results["success"], "user_id has nulls!"
    
    results = ge_df.expect_column_values_to_be_between(
        "age", min_value=13, max_value=120)
    assert results["success"], "age out of range!"
    
    results = ge_df.expect_column_unique_value_count_to_be_between(
        "event_type", min_value=5, max_value=50)
    assert results["success"], "Unexpected event type count!"
    
    return df

# Apply validation before writing
clean_df = validate_events(raw_df)
clean_df.write.format("delta").save(output_path)
```

---

# PART 18 — BUILDING A PRODUCTION DATA PIPELINE

## 18.1 Full Pipeline Architecture

We will build a complete real-time + batch analytics pipeline for an e-commerce platform:

```
E-Commerce Events
(add_to_cart, purchase, search, view)
       │
       ▼
┌─────────────────┐
│  Kafka Cluster  │ 6 partitions, 7-day retention
│  Topic: events  │
└───────┬─────────┘
        │
        ├──────────────────────────────────────┐
        │                                      │
        ▼                                      ▼
┌──────────────────┐                  ┌────────────────────┐
│ Spark Structured │                  │   Spark Batch       │
│ Streaming        │                  │   (hourly/daily)    │
│                  │                  │                      │
│ - 1 min trigger  │                  │ - Full aggregates   │
│ - Window metrics │                  │ - ML features       │
│ - Anomaly detect │                  │ - Data quality      │
└────────┬─────────┘                  └──────────┬──────────┘
         │                                       │
         ▼                                       ▼
┌────────────────────────────────────────────────────────────┐
│              Iceberg Data Lake (S3)                         │
│                                                             │
│  raw/events/           → Bronze: raw Kafka events          │
│  silver/events_clean/  → Silver: deduped, validated        │
│  gold/user_metrics/    → Gold: aggregated user KPIs        │
│  gold/product_metrics/ → Gold: product performance         │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────┐
│      ClickHouse (OLAP)       │ Real-time query < 100ms
└──────────────────────────────┘
         │
         ▼
┌──────────────────────────────┐
│   Apache Superset Dashboard  │ Business metrics
└──────────────────────────────┘
```

## 18.2 Bronze Layer: Raw Ingestion

```python
# bronze_ingestion.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.prod.type", "hadoop") \
    .config("spark.sql.catalog.prod.warehouse", "s3://data-lake/iceberg/") \
    .getOrCreate()

# Read raw events from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

# Add ingestion metadata
enriched = raw_stream \
    .select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_payload"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at")
    )

# Write to Bronze Iceberg table
query = enriched.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "s3://checkpoints/bronze_events/") \
    .toTable("prod.bronze.events")

query.awaitTermination()
```

## 18.3 Silver Layer: Cleaning and Validation

```python
# silver_processing.py — runs hourly via Airflow
from pyspark.sql.functions import *
from pyspark.sql.types import *

EVENT_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("client_ip", StringType(), True),
    StructField("user_agent", StringType(), True)
])

# Read new bronze data (incremental)
bronze = spark.read.table("prod.bronze.events") \
    .filter(col("ingested_at") >= current_timestamp() - expr("INTERVAL 2 HOURS"))

# Parse JSON
parsed = bronze \
    .withColumn("data", from_json(col("raw_payload"), EVENT_SCHEMA)) \
    .select("event_key", "kafka_partition", "kafka_offset", 
            "kafka_timestamp", "ingested_at", "data.*")

# Data quality filters
clean = parsed \
    .filter(col("user_id").isNotNull()) \
    .filter(col("event_timestamp").isNotNull()) \
    .filter(col("event_type").isin(
        "view", "search", "add_to_cart", "purchase", "checkout")) \
    .filter(col("event_timestamp") >= lit("2020-01-01").cast("timestamp")) \
    .dropDuplicates(["event_key"])  # Kafka exactly-once dedup

# Write to Silver with merge (upsert)
from delta.tables import DeltaTable  # or Iceberg MERGE

spark.sql("""
MERGE INTO prod.silver.events AS target
USING clean_events AS source
ON target.event_key = source.event_key
WHEN NOT MATCHED THEN INSERT *
""")
```

## 18.4 Gold Layer: Aggregated Metrics

```python
# gold_user_metrics.py — runs daily
user_metrics = spark.sql("""
WITH daily_activity AS (
    SELECT
        user_id,
        DATE(event_timestamp) AS activity_date,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS sessions,
        SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_adds
    FROM prod.silver.events
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL 1 DAY
      AND event_timestamp < CURRENT_DATE
    GROUP BY user_id, DATE(event_timestamp)
),
user_30d_summary AS (
    SELECT
        user_id,
        SUM(revenue) AS revenue_30d,
        SUM(purchases) AS purchases_30d,
        COUNT(DISTINCT activity_date) AS active_days_30d,
        MAX(activity_date) AS last_active_date
    FROM prod.silver.events
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY user_id
)
SELECT
    d.user_id,
    d.activity_date,
    d.total_events,
    d.sessions,
    d.revenue,
    d.purchases,
    s.revenue_30d,
    s.purchases_30d,
    s.active_days_30d,
    s.last_active_date,
    CASE
        WHEN s.purchases_30d >= 3 THEN 'high_value'
        WHEN s.purchases_30d >= 1 THEN 'active'
        ELSE 'low_engagement'
    END AS user_segment
FROM daily_activity d
JOIN user_30d_summary s USING (user_id)
""")

user_metrics.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("overwrite-mode", "dynamic") \
    .save("prod.gold.user_daily_metrics")
```

---

# PART 19 — BEST PRACTICES FOR SPARK ENGINEERS

## 19.1 Code Structure

**Use configuration as code:**

```python
# conf/spark_config.py
SPARK_CONFIG = {
    # Base configuration
    "spark.app.name": "MyProductionApp",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "auto",
    
    # Memory
    "spark.executor.memory": "16g",
    "spark.executor.memoryOverhead": "4g",
    "spark.driver.memory": "8g",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "8g",
    
    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired": "false",
    
    # Network
    "spark.network.timeout": "800s",
    "spark.executor.heartbeatInterval": "60s",
    
    # Data
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128 MB
}

def create_spark_session(app_name, extra_config=None):
    builder = SparkSession.builder.appName(app_name)
    for k, v in SPARK_CONFIG.items():
        builder = builder.config(k, v)
    if extra_config:
        for k, v in extra_config.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()
```

**Modular pipeline structure:**

```python
# pipelines/user_metrics.py
class UserMetricsPipeline:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def extract(self):
        return self.spark.read.table(self.config.source_table)
    
    def transform(self, df):
        return df \
            .filter(col("date") == self.config.run_date) \
            .groupBy("user_id") \
            .agg(sum("amount").alias("revenue"))
    
    def load(self, df):
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.config.output_path)
    
    def run(self):
        raw = self.extract()
        transformed = self.transform(raw)
        self.load(transformed)
```

## 19.2 Monitoring and Alerting

**Production monitoring stack:**

```
Spark Metrics → Prometheus → Grafana Dashboards
                     │
                     ▼
              AlertManager → PagerDuty/Slack

Key metrics to track:
  - Job duration (alert if > 2x historical average)
  - Stage failure rate
  - Shuffle data volume
  - GC time %
  - Executor memory utilization
  - Task input rows vs expected rows (data freshness)
```

```python
# Custom metrics in application code
from pyspark.accumulators import AccumulatorParam

class MetricsAccumulator(AccumulatorParam):
    def zero(self, v): return {}
    def addInPlace(self, a, b):
        for k, v in b.items():
            a[k] = a.get(k, 0) + v
        return a

metrics = sc.accumulator({}, MetricsAccumulator())

def process_row(row):
    metrics.add({"rows_processed": 1})
    if row.amount < 0:
        metrics.add({"negative_amounts": 1})
    return row

# After job:
print(f"Rows processed: {metrics.value['rows_processed']}")
print(f"Data quality issues: {metrics.value.get('negative_amounts', 0)}")
```

## 19.3 Cost Optimization

**The biggest cost levers in Spark:**

| Optimization | Typical Savings | Effort |
|---|---|---|
| Right-size executor memory | 20–40% | Low |
| Use Spot/Preemptible instances | 60–80% on compute | Medium |
| Auto-scaling (K8s/YARN) | 30–50% on idle capacity | Medium |
| Parquet + column pruning | 40–70% on I/O costs | Low |
| Cache wisely (don't over-cache) | 10–20% | Low |
| Avoid small file problem | 20–30% | Medium |

**Spot instance strategy for Spark on Kubernetes:**

```yaml
# spark-submit with spot instances
spark.kubernetes.executor.annotations.cluster-autoscaler.kubernetes.io/safe-to-evict: "true"
spark.kubernetes.node.selector.cloud.google.com/gke-spot: "true"
spark.task.maxFailures: 8          # Allow more retries for spot eviction
spark.stage.maxConsecutiveAttempts: 8
```

**Handle spot eviction gracefully** by enabling speculative execution:

```python
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")
spark.conf.set("spark.speculation.quantile", "0.9")
```

## 19.4 Testing Spark Pipelines

```python
# tests/test_user_metrics.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()

def test_user_metrics_aggregation(spark):
    # Arrange: Create test data
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("event_type", StringType()),
        StructField("date", DateType())
    ])
    
    test_data = [
        ("user1", 100.0, "purchase", date(2024,1,15)),
        ("user1", 50.0,  "purchase", date(2024,1,15)),
        ("user2", 200.0, "purchase", date(2024,1,15)),
        ("user1", 30.0,  "view",     date(2024,1,15)),
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Act
    pipeline = UserMetricsPipeline(spark, config)
    result = pipeline.transform(df)
    
    # Assert
    rows = {r.user_id: r for r in result.collect()}
    assert rows["user1"].revenue == 150.0
    assert rows["user2"].revenue == 200.0
    assert len(rows) == 2  # No row for non-purchase user3
```

---

# PART 20 — THE FUTURE OF SPARK

## 20.1 Spark on Kubernetes: The New Standard

Kubernetes has become the dominant deployment target for Spark. The benefits are compelling:

- **Elastic scaling**: Executors scale from 0 to thousands based on workload
- **Multi-tenancy**: Namespace isolation, resource quotas per team
- **Portability**: Same YAML/Helm charts work on AWS EKS, GKE, AKS
- **Ecosystem integration**: Argo Workflows, Prometheus, Istio

```yaml
# spark-submit on Kubernetes
./bin/spark-submit \
  --master k8s://https://k8s-api-server:443 \
  --deploy-mode cluster \
  --name my-spark-job \
  --conf spark.executor.instances=50 \
  --conf spark.kubernetes.container.image=myrepo/spark:3.5.0 \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.mount.path=/tmp \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.options.claimName=spark-pvc \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  local:///opt/spark/jars/my-job.jar
```

**Dynamic allocation on Kubernetes (Spark 3.1+):**

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "200")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

No need for an external shuffle service — shuffle tracking keeps executors alive as long as their shuffle data is needed.

## 20.2 The Lakehouse Architecture Matures

The convergence of data lake and data warehouse continues to accelerate. By 2026, the dominant pattern is:

```
┌─────────────────────────────────────────────────────────────┐
│                    Lakehouse Platform                        │
│                                                              │
│  Compute Engines:                                            │
│    Spark   → ETL, ML feature engineering, heavy transforms  │
│    Flink   → Real-time stream processing                     │
│    Trino   → Interactive SQL at scale                        │
│    DuckDB  → Single-node fast analytics (emerging)          │
│                                                              │
│  Table Format:                                               │
│    Apache Iceberg (most multi-engine support)                │
│    Delta Lake (Databricks ecosystem)                         │
│    Apache Hudi (upsert-optimized, streaming)                 │
│                                                              │
│  Storage:                                                    │
│    S3 / GCS / ADLS (object storage)                         │
│    Apache Parquet / ORC (data files)                         │
│                                                              │
│  Catalog:                                                    │
│    Apache Polaris / Gravitino / Unity Catalog                │
│    (unified metadata across all engines)                     │
└─────────────────────────────────────────────────────────────┘
```

## 20.3 Spark + AI Pipelines

As machine learning becomes operationalized, Spark occupies the feature engineering and data preparation layer of the AI stack:

```
Data Sources (S3, Kafka, DBs)
        │
        ▼
    Spark ETL
    (Feature Engineering)
    → user embeddings
    → behavioral features
    → graph features
        │
        ▼
    Feature Store (Feast / Tecton / Hopsworks)
    → Online store (Redis): real-time serving
    → Offline store (Iceberg): training data
        │
        ├──▶  ML Training (PyTorch / TF on Ray)
        │         │
        │         ▼
        │     Model Registry (MLflow)
        │
        └──▶  Batch Inference (Spark + MLlib / pandas UDFs)
                  │
                  ▼
              Predictions written back to Feature Store
```

**Spark with PyTorch using Petastorm:**

```python
from petastorm.spark import SparkDatasetConverter, make_spark_converter

# Convert Spark DataFrame to PyTorch Dataset
converter = make_spark_converter(df.select("features", "label"))

with converter.make_torch_dataloader(batch_size=256) as dataloader:
    for batch in dataloader:
        features = batch["features"]
        labels = batch["label"]
        # Standard PyTorch training loop
        optimizer.zero_grad()
        output = model(features)
        loss = criterion(output, labels)
        loss.backward()
        optimizer.step()
```

## 20.4 The Emergence of Spark Connect

Spark Connect (Spark 3.4+) introduces a decoupled client-server architecture:

```
Before Spark Connect:
  User Code (Python) ← py4j bridge → Spark Driver (JVM) → Executors
  ● py4j is slow and fragile
  ● Driver must be co-located with user code
  ● Hard to use from notebooks, IDEs, microservices

After Spark Connect:
  User Code (Python/Go/Rust/JS)
        │ gRPC (protobuf)
        ▼
  Spark Connect Server (JVM)
        │
        ▼
  Spark Driver → Executors

  ● Language-agnostic
  ● Remote connection (laptop → production cluster)
  ● Enables thin clients, serverless Spark
  ● Better debuggability
```

## 20.5 Serverless Spark

The direction of cloud providers (AWS Glue, Google Dataproc Serverless, Azure Synapse) is zero-management serverless Spark:

```
Old model:
  Provision cluster → wait 5 min → run job → idle cluster costs money → terminate

Serverless model:
  Submit job → cloud provisions exactly what's needed → run job → auto-terminate
  Pay only for actual compute seconds

Trade-offs:
  ✓ No cluster management
  ✓ Auto-scaling
  ✓ Cost efficiency for intermittent workloads
  ✗ Cold start latency (30–90 seconds)
  ✗ Less control over tuning
  ✗ Vendor lock-in
```

## 20.6 Closing Thoughts: What Makes a Great Spark Engineer

After 15 years building distributed data systems, the engineers who consistently deliver reliable, performant Spark pipelines share certain traits:

They **read execution plans** before accepting that a job "works." A working job that takes 3 hours when it should take 20 minutes is not working — it is failing slowly.

They **measure before optimizing**. The Spark UI tells you exactly what is slow. Tuning shuffle partitions on a job that is bottlenecked on disk I/O is wasted effort.

They **design for failure**. In a cluster of 100 machines, hardware failures are daily events. Every pipeline needs checkpointing, idempotent writes, and dead man's switch alerting.

They **understand the data**. A skewed key distribution is a data problem, not a Spark problem. The best optimization is often a better data model — bucketing, pre-aggregation, denormalization.

They **keep it simple**. The most robust production pipelines are the ones that are easiest to understand, debug, and modify at 3 AM when the on-call alert fires. Clever code is the enemy of reliable systems.

---

## Appendix A: Configuration Reference

| Configuration | Default | Recommendation |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | `auto` with AQE, or 2–4x cores |
| `spark.sql.adaptive.enabled` | true (3.2+) | Always true in production |
| `spark.executor.memory` | 1g | 8–32g depending on workload |
| `spark.executor.cores` | 1 | 4–5 for CPU balance |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | 50–200MB |
| `spark.network.timeout` | 120s | 600–800s for large shuffles |
| `spark.serializer` | Java | KryoSerializer |
| `spark.sql.parquet.compression.codec` | snappy | snappy or zstd |
| `spark.memory.offHeap.enabled` | false | true for memory-intensive jobs |
| `spark.speculation` | false | true on spot instances |

## Appendix B: Spark Version History

| Version | Year | Key Feature |
|---|---|---|
| 0.5 | 2012 | Initial release, RDD API |
| 1.0 | 2014 | Production-ready, Spark SQL preview |
| 1.3 | 2015 | DataFrame API |
| 1.6 | 2016 | Dataset API, Unified Memory Manager |
| 2.0 | 2016 | Structured Streaming, Tungsten Phase 2 |
| 2.3 | 2018 | Kubernetes support |
| 3.0 | 2020 | Adaptive Query Execution |
| 3.2 | 2021 | AQE on by default, Spark Connect preview |
| 3.4 | 2023 | Spark Connect stable, Python DataSource API |
| 3.5 | 2023 | Spark Connect enhancements, improved Python support |

## Appendix C: Essential Reading

- *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing* — Zaharia et al., NSDI 2012
- *Spark SQL: Relational Data Processing in Spark* — Armbrust et al., SIGMOD 2015
- *Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark* — Armbrust et al., SIGMOD 2018
- *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores* — Armbrust et al., VLDB 2020
- *Apache Iceberg: An Open Table Format for Huge Analytic Datasets* — Netflix / Apple, SIGMOD 2023

---

*End of Apache Spark Engineering Handbook*

*This document covers Spark through version 3.5. The distributed systems landscape evolves rapidly — validate version-specific behavior against official Apache Spark documentation at spark.apache.org.*
