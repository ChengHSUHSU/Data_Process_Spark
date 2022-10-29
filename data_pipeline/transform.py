



class SparkTransform:
    def __init__(self):
        print()

    def left_join(self, 
                  spk_main_dat, 
                  spk_meta_dat, 
                  left_key, 
                  right_key):

        #if meta_info['join_method'] == 'PYdataframe':
        self.left_key = left_key
        self.right_key = right_key
        self.meta_dat = spk_meta_dat

        joinType = "left_outer"
        joinExpression = spk_main_dat[left_key] == spk_meta_dat[right_key]
        spk_main_dat = spk_main_dat.join(spk_meta_dat, joinExpression, joinType)

        #spk_main_dat.mapInPandas(self.merge_func_pydf, spk_main_dat.schema)
        
        # elif meta_info['join_method'] == 'SPKdataframe':
        #     pass

        # spk_main_dat.join(spk_meta_dat, 
        #                   df.name == spk_meta_dat.name, 
        #                   'left')

        return spk_main_dat
        

    def merge_func_pydf(self, iterator):
        for m_dat in iterator:
            dat = m_dat.merge(self.meta_dat, 
                              how='left', 
                              left_on=self.left_key, 
                              right_on=self.right_key)
            yield dat

 



