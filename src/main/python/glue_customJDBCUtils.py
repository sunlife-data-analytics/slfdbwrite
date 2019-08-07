from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame


def write(database, table, df, jdbc_conf, cut_off_percentage, repartition=False, s3path='', format='csv'):
    """ Python wrapper function for Scala class CustomJDBCUtils
    @params:  
              database              - string name of the database to write to
              table                 - string name of the table being written to (format = 'schema.table')
              df                    - pyspark.sql.DataFrame to be written
              jdbc_conf             - jdbc_conf for the connection, extracted from glueContext.extract_jdbc_conf()
              cut_off_percentage    - integer representing the bad records threshhold (ex. 50 = 50%)
              s3path                - path in s3 where the bad records should be written to (optional)
              format                - file format to write out the bad records as (default = 'csv')
    """
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    jvm = sc._jvm
    jsc = sc._jsc
    jdf = df._jdf

    if s3path is not "":
        df = DataFrame(jvm.com.slf.CustomJDBCUtils.write(
            database, table, jdf, jdbc_conf, cut_off_percentage), jsc)
        try:
            df.repartition(1).write.format(format).save(s3path)
            return df
        except:
            # Spark Writing Failed. Reverting to GlueContext
            glueContext.write_dynamic_frame_from_options(
                frame=DynamicFrame.fromDF(df, glueContext, 'dynamic_frame'),
                connection_type='s3',
                connection_options={'path': s3path},
                format=format)
    else:
        return DataFrame(jvm.com.slf.CustomJDBCUtils.write(database, table, jdf, jdbc_conf, cut_off_percentage), jsc)
