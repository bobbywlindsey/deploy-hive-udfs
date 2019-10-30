# deploy-hive-udfs

The Python script `deploy.py` will do the following:

1. Compile your Java Maven project
2. Upload the resulting jar file to HDFS
3. Run the queries in `udf_queries.txt` against Hive in order to create the Hive UDFs

### Usage

First put the `deploy.py` script in the root folder of your Java Maven project.

Then assign values to the following "constant" variables in `deploy.py`:

* `HDFS_BASE_URL`
* `JDBC_URL`
* `ABSOLUTE_JAR_FILE_PATH`

Example values might look like the following:

```python
HDFS_BASE_URL = 'https://hdfs-host.amazonaws.com:8443/gateway/default/webhdfs/v1/parentfolder/childfolder'
JDBC_URL = 'jdbc:hive2://hdfs-host.amazonaws.com:8443/;ssl=true'
ABSOLUTE_JAR_FILE_PATH = '/Users/username/hive-jdbc.jar'
```

Next, add the required SQL commands for your new udf to `udf_queries.txt`. Examples have been provided in `udf_queries.txt`.

Finally, execute `python deploy.py [schema-name]` and enter your Hive user credentials.
