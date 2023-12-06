# this is a utility module

import pandas as pd
from configparser import ConfigParser

def merge_dataframes(view_level_df, dashboard_level_df,common_column = 'used_fields',join_type='inner'):
    """
    function merges based on common column it is more towards merging the view level data with its
    corresponding dashboard data
    """
    exploded_df = dashboard_level_df.explode(common_column)
    merged_df = pd.merge(view_level_df,exploded_df,left_on=common_column,right_on=common_column,how=join_type)
    merged_df.to_csv("test.csv",index=False)
    return merged_df
    
def parse_config_file(path):
    """
    loads the config file into memory
    """
    config = ConfigParser()
    config.read(path)
    return config
