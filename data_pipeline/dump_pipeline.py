


from producer import Producer
from consumer import Consumer





# database
table = 'test1'
batch_size =  5000


# yaml_path
yaml_path = 'pipeline.yaml'


# Producer
producer = Producer(yaml_path)


# Consumer
consumer = Consumer(yaml_path)




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
                #print(data_info)
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
        consumer.write_to_db(data_info, table, collection, True)
    continue









