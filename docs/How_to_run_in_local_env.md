**Steps to run in local environment:**

**Step 1: Install Prerequisites**
- Java JDK → Spark requires Java. Install JDK 8 or 11 (or 17 if you’re aligning with your Docker setup).(I am using this C:\Program Files\Java\jdk-11.0.26- found by running cmd echo %JAVA_HOME%)
- Python → Ensure Python 3.x is installed.( I am using Python 3.10)
- Apache Spark → Download from Spark official site (spark.apache.org) and extract locally.( I am using Spark 3.4.4)
- Hadoop Winutils (Windows only) → If you’re on Windows, you can download zip file from here(https://github.com/sunboyqing/winutils3.3.0), then unzip it in location like D:\Demo\.(I am using this D:\Demo\hadoop-3.3.0- echo %HADOOP_HOME%)

**🛠 Step 2: Environment Variables**

Set these in your system or via .env:
- JAVA_HOME → Path to your JDK (e.g., C:\Program Files\Java\jdk-11.0.26)- (set JAVA_HOME=C:\Program Files\Java\jdk-11.0.26)
- SPARK_HOME → Path to Spark folder (e.g., D:\Demo\spark-3.4.4)- (set SPARK_HOME=D:\Demo\spark-3.4.4)
- HADOOP_HOME (Windows only) → Path to Hadoop folder with bin/winutils.exe- (set HADOOP_HOME=D:\Demo\hadoop-3.3.0)
- Add %SPARK_HOME%\bin and %HADOOP_HOME%\bin to your PATH.
- PYTHONPATH → Path to Pythonpath (Standard environment variable in Python that tells the interpreter where to look for modules/packages when you import something) - (e.g, D:\Demo\spark-3.4.4\python and D:\Demo\spark-3.4.4\python\lib\py4j-0.10.9.7-src.zip)
- PYSPARK_PYTHON → Path to Pyspark_python (Environment variable used by Spark to specify which Python interpreter executors should use.)( e.g, D:\Demo\Sales_Analytics\.venv\Scripts\python.exe)

**🛠 Step 3: Install Python Packages**

In your PyCharm project’s virtual environment:
pip install pyspark

**🛠 Step 4: Configure PyCharm Project**
- Open PyCharm → Settings → Project Interpreter → select your virtual environment.
- Ensure pyspark is installed.
- (Optional) Add .env support if you’re using python-dotenv.

**🛠 Step 5: Run the jobs sequentially as mentioned**
- First run the opt/airflow/utils/DataGenerator/SalesDataFaker.py to generate the source data files
(which will be used to insert the raw source tables).
- Run this script(sql/SourceTableDDL,DML.sql) in your local sql server database( After creating Sales_Analytics Database).
This would create the source tables and fill the data in those tables using the files we generated earlier.
- Run opt/airflow/jobs/bronze_ingest.py, to load data from SQL server source tables to parquet files.(as is)
- Run opt/airflow/jobs/silver_transform.py to transform/cleanse data from bronze parquet files to silver parquet files.
- Run opt/airflow/jobs/gold_star.py to load the transformed silver parquet files to final facts and dims.
- Run this script(sql/TargetTableDDL.sql) in your local sql server database( Sales_Analytics Database-Already created earlier).
This would create the target dimension and fact tables.
- Run opt/airflow/jobs/publish_sqlserver.py to load the gold layer dim and fact parquet files data
to the target dw sql server tables(which were created in earlier step).
- Finally run these to validate data in the sql server tables:
select * from dw.FactSales;
select * from dw.DimCustomer;
select * from dw.DimDate;
select * from dw.DimProduct;
select * from dw.DimStore;

**⚡ Quick Checklist**
- ✅ JDK installed and JAVA_HOME set
- ✅ Spark downloaded and SPARK_HOME set
- ✅ pyspark installed in PyCharm venv
- ✅ Simple test script runs successfully

**Notes:**
-----------------------------------------------------------------------------------------------------------------------------------------------
**🐍 PYTHONPATH**
- Definition: Standard environment variable in Python that tells the interpreter where to look for modules/packages when you import something.
- Usage in PySpark:
- Spark workers need to know where your Python code and dependencies live.
- By setting PYTHONPATH, you ensure that both the driver and executors can import your custom modules.
- Example:
export PYTHONPATH=/home/kaustav/Sales_Analytics/utils:/home/kaustav/Sales_Analytics/common

- Now, inside PySpark you can do:
import my_custom_module
- without errors, because Spark executors know where to find it.

**🔥 PYSPARK_PYTHON**

- Definition: Environment variable used by Spark to specify which Python interpreter executors should use.
- Why it matters:
- In a cluster, your driver might run Python 3.11 locally, but executors (workers) need to use the same version to avoid compatibility issues.
- Setting PYSPARK_PYTHON ensures consistency across driver and worker nodes.
- Example:
export PYSPARK_PYTHON=/usr/bin/python3
- or if you’re using a virtual environment:
export PYSPARK_PYTHON=/home/kaustav/.venv/bin/python
⚡ How They Work Together- PYSPARK_PYTHON → tells Spark which Python binary to run on executors.
- PYTHONPATH → tells that Python binary where to find your modules.
So in practice:export PYSPARK_PYTHON=/home/kaustav/.venv/bin/python
export PYTHONPATH=/home/kaustav/Sales_Analytics
This ensures:- Spark executors use your virtualenv Python.
- They can import your project modules (Sales_Analytics, utils, etc.).
🛠 Best Practice for Local vs Cloud- Local dev (PyCharm, Docker):
- PYSPARK_PYTHON → point to your venv Python.
- PYTHONPATH → point to your project root.
- Cloud/YARN/K8s:
- PYSPARK_PYTHON → point to system Python or packaged venv.
- PYTHONPATH → distribute your code via --py-files or .zip packages instead of relying on env vars.
👉 Since you’re orchestrating Airflow + PySpark pipelines, the cleanest approach is:- Keep PYSPARK_PYTHON in your .env (switch between local venv and cloud system Python).
- Use PYTHONPATH only for local dev; in cloud, package dependencies with spark-submit --py-files.
-----------------------------------------------------------------------------------------------------------------------------------------------


