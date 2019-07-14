from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
from datetime import datetime, timedelta

packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0"

# Versiones de kafka 0-8
# Version de scala: 2.11
# Version de spark: 2.4.0

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages)
)

sc = SparkContext(appName="PracticaPythonStreamingKafka")

ssc = StreamingContext(sc, 5)

kafkaParams = {"metadata.broker.list": "127.0.0.1:9092"}
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["test"], kafkaParams) #Conexion de kafka con spark
ssc.checkpoint("checkpoint")  # create dir for checkpoint archives

# Con un solo directStream se puede escuchar varios brokers.

def sum_since_start(new, current):
    new = new[0]
    current = current or (0, 0)
    new_count = new[1] + current[1]
    new_sum = new[0] + current[0]
    return new_sum, new_count

def update_sum(current, new):
    new_count = new[1] + current[1]
    new_avg = new[0] + current[0]
    return (new_avg, new_count)

def get_average(tuple):
    key = tuple[0]
    sum = tuple[1][0]
    count = tuple[1][1]
    return key, sum/count

def update_substr(current, old):
    new_count = current[1] - old[1]
    new_avg = current[0] - old[0]
    return new_avg, new_count

def count_couples(current, new):
    FMT = '%Y-%m-%d %H:%M:%S'
    tdelta = datetime.strptime(new[0], FMT) - datetime.strptime(current[0], FMT)
    new_count = current[1] + (1 if tdelta != timedelta(minutes=1) else 0)
    new_date = new[0]
    return (new_date, new_count)

# Configura el endpoint para localizar el broker de Kafka
kafkaBrokerIPPort = "127.0.0.1:9092"

# Productor simple (Singleton!)

import kafka
class KafkaProducerWrapper(object):
  producer = None
  @staticmethod
  def getProducer(brokerList):
    if KafkaProducerWrapper.producer != None:
      return KafkaProducerWrapper.producer
    else:
      KafkaProducerWrapper.producer = kafka.KafkaProducer(bootstrap_servers=brokerList,
                                                          key_serializer=str.encode,
                                                          value_serializer=str.encode)
      return KafkaProducerWrapper.producer

# Envía métricas a Kafka! (salida)
def sendMetrics(itr):
  prod = KafkaProducerWrapper.getProducer([kafkaBrokerIPPort])
  for m in itr:
    prod.send("metrics", key=m[0], value=m[0]+","+str(m[1]))
  prod.flush()

lines = directKafkaStream.map(lambda x: x[1]).map(lambda x: str.replace(x, '"', '')).map(lambda x: x.split(","))

# variables = lines.map(lambda x: [('temp', (float(x[2]), 1)), ('humidity', (float(x[3]), 1)), ('cO2', (float(x[5]), 1))])\
#                  .flatMap(lambda v: (v[0], v[1], v[2]))
#
# values_since_start = variables.reduceByKey(lambda x, y: tuple(map(lambda l, m: l + m, x, y)))\
#                               .updateStateByKey(sum_since_start)\
#                               .map(get_average)
#
# values_by_batch = variables.reduceByKey(update_sum)\
#                            .map(get_average)

# light_over_window = lines.map(lambda x: ('light', (float(x[4]), 1)))\
#                        .reduceByKeyAndWindow(update_sum, update_substr, 5, 5).map(get_average)


tim_sep = lines.map(lambda x: ("time", (x[1], 0)))\
               .reduceByKey(count_couples)\
               .map(lambda x: ('parejas de muestras separadas por 1 minuto', x[1][1]))

# values_since_start.pprint()
# values_by_batch.pprint()
# light_over_window.pprint()

tim_sep.pprint()
lines.map(lambda x: ("time", x[1])).pprint()
ssc.start()
ssc.awaitTermination()
