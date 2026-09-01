# azure-databricks-ecommerce-lakehouse
# 🛒 Enterprise E-Commerce Medallion Lakehouse Pipeline

[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Community-FF3621?logo=databricks&logoColor=white)](https://community.cloud.databricks.com)
[![Apache Spark](https://img.shields.io/badge/Compute-Apache%20Spark%203.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%20ACID-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Language-Python%20%2F%20PySpark-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Power BI](https://img.shields.io/badge/Serving-Power%20BI%20%2F%20DAX-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

An end-to-end cloud data engineering project implementing a production-grade **Medallion Lakehouse Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** on the **Olist Brazilian E-Commerce dataset**. The pipeline demonstrates metadata-driven batch ingestion, schema enforcement, deduplication, surrogate key generation, Kimball Star Schema modeling, Delta Lake performance optimization (`OPTIMIZE`, `Z-ORDER`, `VACUUM`), and analytical serving.

---

## 🏗️ Architecture Overview

![Medallion Lakehouse Architecture](diagrams/medallion_architecture_diagram.png)
