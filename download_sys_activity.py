# this module takes care of downloading the data from the system activity explore on our instance
import looker_sdk
from looker_sdk.sdk.api40 import  models
import os
import pandas as pd
import json


class SysActivityDownloader:
    # was kept for testing purposes
    # dashboard_title = 'CEO Dashboard'
    def __init__(self, looker_config):
        self.__instance_url = looker_config.get('base_url',None)
        self.__client_secret = looker_config.get('client_secret',None)
        self.__client_id=looker_config.get('client_id',None)
        self.__ssl_verify = looker_config.get('verify_ssl',None)
        self.file_save_path = 'temp.csv'
        self.dashboards = []
        if(
            not self.__instance_url or
            not self.__client_id or
            not self.__client_secret
        ): print("Auth parameters missing in config")
        else:
            # add into the environment variables for lookersdk auth
            os.environ['LOOKERSDK_BASE_URL'] = self.__instance_url
            os.environ['LOOKERSDL_CLIENT_ID'] = self.__client_id
            os.environ['LOOKERSDK_CLIENT_SECRET'] = self.__client_secret
        try:
            # perform auth
            self.sdk = looker_sdk.init40()
        except Exception:
            print("Login failed to looker")

    def __extract_vis_type(self,vis_obj):
        """
        internal function to extract out the type of visualization present on the tile
        """
        return json.loads(vis_obj).get('type','NO_VIS_TYPE_PRESENT')
    def __preprocess(self):
        """
        extract out the required fileds and also convert certain types into internal python types
        """
        self.sys_activity_df.fillna('no',inplace=True)
        self.sys_activity_df['vis_name'] = self.sys_activity_df['query_vis_config'].apply(lambda vis_obj: self.__extract_vis_type(vis_obj))
        self.sys_activity_df['used_fields'] = self.sys_activity_df['query_fields_used'].apply(lambda fields: json.loads(fields))
        self.sys_activity_df.drop(['query_vis_config','query_fields_used'], axis=1,inplace=True)
    
    def create_df(self):
        """
        convert the dashboard level data into a tabular structure
        """
        self.sys_activity_df = pd.read_csv(self.file_save_path)
        # renaming for ease of use
        self.sys_activity_df.rename(
            columns={
                "Dashboard Title":"dashboard_title",
                "Dashboard Element Title":"element_title",
                "Dashboard Element Type":"element_type",
                "Query Explore":"query_view",
                "Query Fields Used":"query_fields_used",
                "Query Vis Config":"query_vis_config"
            },
            inplace=True
        )
        self.__preprocess()
    
    def fetch_dashboard_list(self,model_name):
        """
        this operation allows us to fetch all the dashboards linked to our project from the system activity
        each project has a model which is linked to explores which are then saved into dashboards hence a relation of model -> dashboard
        """
        resp = self.sdk.run_inline_query(
            result_format="json",
            body = models.WriteQuery(
                model = 'system__activity',
                view = 'dashboard',
                fields = [
                    "dashboard.title",
                    "query.model"
                ],
                filters = {
                    "query.model":f"{model_name}",
                    "dashboard.moved_to_trash":"No"
                }
            )
        )
        resp = json.loads(resp)
        for obj in resp:
            self.dashboards.append(obj['dashboard.title'])


    def fetch_data(self):
        """
        this function queries the system activity explore for dashboard level values
        """
        resp = self.sdk.run_inline_query(
            result_format='csv',
            body=models.WriteQuery(
                model = 'system__activity',
                view = 'dashboard',
                fields = [
                    "dashboard.title",
                    "dashboard_element.title",
                    "dashboard_element.type",
                    "query.view",
                    "query.formatted_fields",
                    "query.vis_config"                                                                                     
                ],
                filters={
                    "dashboard.title":",".join(self.dashboards),
                    "dashboard_element.type":"-button"
                }
            )
        )
        with open(self.file_save_path,'w') as f:
            f.write(resp)
        

        
