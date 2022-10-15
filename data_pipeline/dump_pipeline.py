




from util import load_config
from producer import Producer
from pyspark.sql import SparkSession







class Consumer:
    def __init__(self, yaml_path=str):
        # load config
        self.cfg = load_config(yaml_path=yaml_path)

        # Producer
        self.producer_ = Producer(yaml_path=yaml_path)

        self.available_data_paths = self.producer_.available_data_paths


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

    def write_to_db(self, data_info=dict, table=str, collection=str):

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







# yaml_path
yaml_path = 'pipeline.yaml'

# Producer
producer = Producer(yaml_path)

# Consumer
consumer = Consumer(yaml_path)

# database
table = 'test1'
batch_size = 10000


for data_path in producer.available_data_paths:
    # init
    collection = data_path.split('/')[-1].replace('.json', '')
    data_info = {'data' : [], 'columns' : None}

    # build generator
    generator = producer.build_generator(data_path)

    # build columns
    columns = producer.show_all_columns(data_path)

    # build data
    for i, data_org in enumerate(generator):
        # build data_
        data_ = []
        for c in columns:
            if c in  data_org:
                data_.append(data_org[c])
            else:
                data_.append(None)
        data_ = tuple(data_)
        data_info['data'].append(data_)
        data_info['columns'] = columns
        if (i+1) % batch_size == 0:
            # write to mongoDB
            try:
                consumer.write_to_db(data_info, table, collection)
            except Exception as error_message:
                print(data_info)
                print('=============================')
                print(error_message)
                print(len(data_info['data']))
                for ed in data_info['data']:
                    print(len(ed))
                quit()
            # update data_info
            data_info = {'data' : [], 'columns' : None}
            print('--------------{}-th-runnnnnn-----------------'.format(str(i+1)))
            
    
    # check data_info
    if data_info['columns'] is not None:
        consumer.write_to_db(data_info, table, collection)
        continue









