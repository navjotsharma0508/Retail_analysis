import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_orders,read_customers
from lib.DataManipulation import filter_closed_orders,join_orders,count_order
from lib.configReader import get_app_config

def test_read_orders():
    spark=get_spark_session("LOCAL")
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count==68884

@pytest.mark.skip('work in progress')
def test_with_fixture(spark):
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count==68884

def test_read_customer_df(spark):
    customer_count=read_customers(spark,"LOCAL").count()
    assert customer_count==12435

