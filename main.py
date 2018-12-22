from os.path import dirname, join
from concurrent.futures import ThreadPoolExecutor
import threading

# IMPORT HELPERS
from bokeh.document import without_document_lock
from tornado import gen
from bokeh.server.server import Server


import config
from scripts.streaming.kafka_sink_spark_source import KafkaConnectPyspark

# GET THE DASHBOARDS
from scripts.utils.mylogger import mylogger


from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.context import SparkConf, SparkContext

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)


# set up stream runners
conf = SparkConf() \
    .set("spark.streaming.kafka.backpressure.initialRate", 150) \
    .set("spark.streaming.kafka.backpressure.enabled", 'true') \
    .set('spark.streaming.kafka.maxRatePerPartition', 250) \
    .set('spark.streaming.receiver.writeAheadLog.enable', 'true') \
    .set("spark.streaming.concurrentJobs", 2)
spark_context = SparkContext(appName='aion_analytics',conf=conf)
ssc = StreamingContext(spark_context,1)

def tx_runner(spark_context,conf,ssc):
    kcp_tx = KafkaConnectPyspark('transaction',spark_context,conf,ssc)
    kcp_tx.run()


def block_runner(spark_context,conf,ssc):
    kcp_block = KafkaConnectPyspark('block',spark_context,conf,ssc)
    kcp_block.run()

# thread the runners
t1 = threading.Thread(target=tx_runner, args=(spark_context,conf,ssc,))
t1.daemon=True
t2 = threading.Thread(target=block_runner, args=(spark_context,conf,ssc,))
t2.daemon=True

# interleave the threads with the dashboard
t1.start()
t2.start()


@gen.coroutine
def kafka_spark_streamer(doc):
    try:
        doc.add_root()
    except Exception:
        logger.error("TABS:", exc_info=True)


# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        server = Server({'/': kafka_spark_streamer}, num_procs=1, port=5007)
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()

    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5007/')
    launch_server()

