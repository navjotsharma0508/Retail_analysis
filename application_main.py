import sys
from lib import DataManipulation,DataReader,Utils
from lib.logger import Log4j
from pyspark.sql.functions import *

if __name__=='__main__':
    if len(sys.argv)<2:
        print("please specify the environment" )
        sys.exit(-1)
    job_run_env=sys.argv[1]
    print("creating spark session")

    spark=Utils.get_spark_session(job_run_env)
    logger=Log4j(spark)
    logger.warn("spark session created")
    print("created spark Session")
    orders_df=DataReader.read_orders(spark,job_run_env)
    orders_filter=DataManipulation.filter_closed_orders(orders_df)

    customers_df=DataReader.read_customers(spark,job_run_env)
    joined_df=DataManipulation.join_orders(orders_df,customers_df)

    aggregated_results=DataManipulation.count_order(joined_df)

    aggregated_results.show()
    print("main end")

    logger.warn("this is the end of main")