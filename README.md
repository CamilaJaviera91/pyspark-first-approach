# Pyspark (First Approach)

## 📝 Description

This code demonstrates how to integrate PySpark with datasets and perform simple data transformations. It loads a sample dataset using PySpark's built-in functionalities or reads data from external sources and converts it into a PySpark DataFrame for distributed processing and manipulation.

# 🔥 What's pyspark?

- It's the Python API for Apache Spark, enabling the use of Spark with Python.

## 🔑 Key Features:

1. **Distributed Computing:** Processes large datasets across a cluster of computers for scalability.
2. **In-Memory Processing:** Speeds up computation by reducing disk I/O.
3. **Lazy Evaluation:** Operations are only executed when an action is triggered, optimizing performance.
4. **Rich Libraries:**
    - **Spark SQL:** Structured data processing (like SQL operations).
    - **MLlib:** Machine learning library for scalable algorithms.
    - **GraphX:** Graph processing (via RDD API).
    - **Spark Streaming:** Real-time stream processing.
5. **Compatibility:** Works with Hadoop, HDFS, Hive, Cassandra, etc.
6. **Resilient Distributed Datasets (RDDs):** Low-level API for distributed data handling.
7. **DataFrames & Datasets:** High-level APIs for structured data with SQL-like operations.

## ✅ Pros

- Handles massive datasets efficiently.
- Compatible with many tools (Hadoop, Cassandra, etc.).
- Built-in libraries for SQL, Machine Learning, Streaming, Graph Processing.

## ❌ Cons

- Can be memory-intensive.
- Complex configuration for cluster environments.

## 🔧 Install pyspark

1. Install via pip

```
pip install pyspark
```

2. Verify installation

```
python3 -c "import pyspark; print(pyspark.__version__)"
```