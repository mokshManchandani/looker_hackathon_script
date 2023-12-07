import looker_sdk
from looker_sdk.sdk.api40 import models
import os
import pandas as pd
from pprint import pprint

class UserAttributeDownloader:
    user_attribute_id = 103
    user_ids = [10,763,242,1951]

    def __init__(self,looker_config):
        self.__instance_url = looker_config.get('base_url',None)
        self.__client_secret = looker_config.get('client_secret',None)
        self.__client_id=looker_config.get('client_id',None)
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
    def dump_users(self):
        # first get a dump of all users
        user_dump = []
        for user_id in UserAttributeDownloader.user_ids:
            user_dump.append(self.sdk.search_users(fields="id, first_name, last_name,email,is_iam_admin",id=user_id)[0])
        # pprint(user_dump)
                        
        user_content = [
            {
                "user_id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "email": user.email,
                "is_iam_admin":user.is_iam_admin
            }
            for user in user_dump
        ]
        for user_obj in user_content:
            user_id = user_obj['user_id']
            resp = self.sdk.user_attribute_user_values(
                user_id=f"{user_id}",
                fields="name,value,user_attribute_id",
                user_attribute_ids=models.DelimSequence([UserAttributeDownloader.user_attribute_id]))[0]
            user_obj['user_attribute_name'],user_obj['user_attribute_id'],user_obj['user_attribute_value'] = resp.name,UserAttributeDownloader.user_attribute_id,resp.value
        pprint(user_content)
        return pd.DataFrame(user_content)

