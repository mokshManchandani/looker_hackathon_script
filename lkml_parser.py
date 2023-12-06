# this module works for parsing the view files and model file parsed from github
# NOTE: For now just a single model file is being considered in the script
from lkml import load
import pandas as pd
from copy import deepcopy

class LKMLParser:
    def __init__(self,view_file_glob,model_file_glob):
        # list of view.lkml files
        self.view_file_glob = view_file_glob
        # the values that are fetched from the lkml files (views)
        self.view_level_values = ['dimensions','measures','name','sql_table_name','derived_table']
        self.fields = ['dimensions','measures']
        self.content = []
        self.model_name = ''
        # to fetch the model file
        self.model_file_glob = model_file_glob
        # to store view level df and metadata
        self.all_content = []
        self.metadata_content = []
    
    def read_view_files(self):
        """
        Utility function to read view file lookml code
        """
        content = []
        for path in self.view_file_glob:
            with open(path, 'r') as f:
                content.append(load(f)['views'][0])
        self.content = content
    
    def get_model_name(self):
        """
        utility function to read model name
        """
        for path in self.model_file_glob:
            self.model_name = path.stem.split('.model')[0]
    
    def sanitize_content(self):
        """
        from all the content just keep the field level values
        this is for simplicity sake
        """
        sanitized_content = []
        for content in self.content:
            new_content = deepcopy(content)
            for key in content.keys():
                if key not in self.view_level_values: del new_content[key]
            sanitized_content.append(new_content)

        self.content = sanitized_content
    
    def __create_structure(self,content_obj):
        """
        This function puts the parsed code into a format which then can be converted into a tabular format
        """
        view_name = content_obj.get('name','NO_NAME_DEFINED')
        sql_table_name = content_obj.get('sql_table_name','NO_SQL_TABLE_NAME_PROVIDED')

        is_derived = 'VIEW_IS_DERIVED_TABLE' if sql_table_name == 'NO_SQL_TABLE_NAME_PROVIDED' else 'VIEW_IS_NOT_DERIVED_TABLE'
        derived_table_form = 'NOT_APPLICABLE'
        if is_derived == 'VIEW_IS_DERIVED_TABLE' and 'derived_table' in content_obj:
            derived_table_form = 'DERIVED_TABLE_IS_NATIVE' if 'explore_source' in content_obj['derived_table'] else 'DERIVED_TABLE_IS_SQL_BASED'
        
        # structure for a row in metadata table
        metadata_obj = {
            'view_name' : view_name,
            'sql_table_name' : sql_table_name,
            'is_derived': is_derived,
            'derived_table_form': derived_table_form
        }
        self.metadata_content.append(metadata_obj)

        for key, value in content_obj.items():
            if key in self.fields:
                for value_obj in value:
                    # structure for a row in the view level table
                    data = {
                        'view_name': view_name,
                        'sql_table_name' : sql_table_name,
                        'field_type': key,
                        'description':value_obj.get('description','NO_DESCRIPTION_PROVIDED'),
                        'field_name': value_obj.get('name','NO_NAME_PROVIDED'),
                        'sql' : value_obj.get('sql','NO_SQL_PROVIDED'),
                        'field_content_type': value_obj.get('type','NO_CONTENT_TYPE_PROVIDED'),
                        'view_is_derived_table' : is_derived,
                        'is_derived_table_native': derived_table_form,
                        'used_fields':f"{view_name}.{value_obj['name']}"
                    }
                    self.all_content.append(data)

    def __preprocess_str_value(self, str, ignore_value = 'NO_SQL_TABLE_NAME_PROVIDED',replace_with = '`',replace_value=''):
        """
        internal function to remove the `` quotes from the string
        """
        if str != ignore_value:
            return str.replace(replace_with,replace_value)
        return str
    
    def __add_in_table_name(self):
        """
        internal function to replace ${TABLE} with the sql_table_name provided to provide more fine grained insigths
        """
        for index,row in self.view_level_df.iterrows():
            if row['sql_table_name'] != 'NO_SQL_TABLE_NAME_PROVIDED' and row['sql'] != 'NO_SQL_PROVIDED':
                self.view_level_df.at[index,'sql'] = row['sql'].replace("${TABLE}",row['sql_table_name'])

    def __preprocess_view_level_df(self):
        """
        Function to preprocess the created data frame (table structure)
        """
        self.view_level_df['sql_table_name'] = self.view_level_df['sql_table_name'].apply(lambda name:self.__preprocess_str_value(name))
        self.__add_in_table_name()

    def create_df(self):
        """
        Loop through each parsed block in the view file and put them in the respective
        metadata table and view level data
        """
        for content in self.content:
            self.__create_structure(content)
        self.view_metadata_df = pd.DataFrame(self.metadata_content)
        self.view_level_df = pd.DataFrame(self.all_content)
        self.__preprocess_view_level_df()
