


from pyspark.sql import SparkSession




class SparkSessionDB:
    def __init__(self, ip, database, collect, session_mode):

        if session_mode == 'mongodb':
            self.db_spk_session = self.build_mongodb_spark_session(ip, 
                                                                   database, 
                                                                   collect, 
                                                                   appName='mongodb')
        else:
            print('non session...')


    def build_mongodb_spark_session(self, ip, database, collect, appName='mongodb'):
        # build mongo path
        mongo_path = f"mongodb://{ip}/{database}.{collect}"

        # build session config
        read_url = ["spark.mongodb.read.connection.uri", mongo_path]
        write_url = ["spark.mongodb.write.connection.uri", mongo_path]

        # build session
        mongodb_spark_session = SparkSession \
                                .builder \
                                .appName(appName) \
                                .config(read_url[0], read_url[1]) \
                                .config(write_url[0], write_url[1]) \
                                .getOrCreate()
        return mongodb_spark_session


    def build_spark_dataframe(self, 
                              data_generator, 
                              columns):
        # build data_info
        data_info = {'data' : [], 'columns' : columns}

        for record in data_generator:
            record_ = []
            for cl in columns:
                if cl in record:
                    record_.append(record[cl])
                else:
                    record_.append(None)
            data_info['data'].append(tuple(record_))
        
        # build spark_dat
        spark_dat = self.db_spk_session \
                    .createDataFrame(data_info['data'], data_info['columns'])
        return spark_dat


    def write_to_db_spark(self, spk_dat, stop_spk_session=False):
        spk_dat.write.format("mongodb").mode("append").save()
        if stop_spk_session is True:
            self.db_spk_session.stop()






