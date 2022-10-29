'''data pipeline(spark)'''
import argparse
from util import all_columns
from util import load_config
from util import read_json_data
from sparksession import SparkSessionDB
from transform import SparkTransform




def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dpl_yaml_path',
                        help='data pipeline yaml path.',
                        type=str,
                        default='./pipeline.yaml')

    parser.add_argument('--db_yaml_path',
                        help='database config setting path.',
                        type=str,
                        default='./database.yaml')

    parser.add_argument('--dataprocess',
                        help='the module of data process.',
                        type=str,
                        default='PersonalizeProcessor')
    
    return parser.parse_args()



# load config
args = arg_parse()
db_cfg = load_config(args.db_yaml_path)
dpl_cfg = load_config(args.dpl_yaml_path)



# read from json and save by generator (history, meta)
history_generator = read_json_data(dpl_cfg['histroy_data_path'])
meta_generator_list = [read_json_data(p) for p in dpl_cfg['meta_data_paths']]



# build columns (history, meta)
history_columns = all_columns(dpl_cfg['histroy_data_path'])
meta_columns_list = [all_columns(p) for p in dpl_cfg['meta_data_paths']]



# build session_info
session_info = db_cfg['mongodb_connect']
session_info['session_mode'] = 'mongodb'



# build spark-dat (meta) [ALL]
spk_session = SparkSessionDB(**session_info)

spk_meta_dat_list = [spk_session.build_spark_dataframe(m_g, m_c) \
                     for m_g, m_c in zip(meta_generator_list, meta_columns_list)]



# build spark-dat (history) [ALL]
spk_history_dat = spk_session \
                  .build_spark_dataframe(history_generator, history_columns)



# transform - left_join (history, meta)
spk_trf = SparkTransform()


# collect name (main, meta)
history_collect = dpl_cfg['history_collect']
merge_info = dpl_cfg['metadata']['merge_info'][history_collect]


# merge
for i, spk_meta_dat in enumerate(spk_meta_dat_list):
    # meta collect name
    meta_collect = dpl_cfg['meta_data_collects'][i]

    # left right key
    left_key = merge_info[meta_collect]['left_key']
    right_key = merge_info[meta_collect]['right_key']
    
    # left join with meta_dat
    spk_history_dat = spk_trf.left_join(spk_history_dat, spk_meta_dat, left_key, right_key)



# write to db (spark)
spk_session.write_to_db_spark(spk_history_dat, stop_spk_session=True)


quit()

