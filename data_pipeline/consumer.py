



from util import load_config
from pyspark.sql import SparkSession







class Consumer:
    def __init__(self, yaml_path=str):
        # load config
        self.cfg = load_config(yaml_path=yaml_path)


    def init_mongodb_spark_session(self,
                                   mongodb_ip=None, 
                                   table=None, 
                                   collection=None, 
                                   appName='myApp'):

        # build mongo collection path
        colleciton_path = "mongodb://{}/{}.{}".format(mongodb_ip,
                                                      table,
                                                      collection)
        
        # build session config
        read_config = ["spark.mongodb.read.connection.uri", colleciton_path]
        write_config = ["spark.mongodb.write.connection.uri", colleciton_path]
        
        # up session
        mongodb_spark_session = SparkSession \
                                .builder \
                                .appName(appName) \
                                .config(read_config[0], read_config[1]) \
                                .config(write_config[0], write_config[1]) \
                                .getOrCreate()
        
        return mongodb_spark_session

    def write_to_db(self, data_info=dict, table=str, collection=str, stop_spark_session=False):

        '''
        data_info = {'data' : [(d1, d2, ...), (...), ....], 'columns' : [....]}
        '''
        # init spark session
        mongodb_spark_session = self.init_mongodb_spark_session(self.cfg['mongodb_ip'], 
                                                                table, 
                                                                collection)

        # build spark-dataframe
        spark_dat = mongodb_spark_session.createDataFrame(data_info['data'], data_info['columns'])

        
        # submit spark-dataframe to mongoDB
        spark_dat.write.format("mongodb").mode("append").save()

        # stop spark session
        if stop_spark_session is True:
            mongodb_spark_session.stop()
            







