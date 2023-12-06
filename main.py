# this is a testing cli tool

import argparse
import sys
from github_downloader import GitDownloader
from download_sys_activity import SysActivityDownloader
from lkml_parser import LKMLParser
from utils import merge_dataframes,parse_config_file
from bigquery_service import BigqueryService

def main():
    parser = argparse.ArgumentParser(description='Hackathon Tool')
    
    # take in the repo link (for the looker project)
    # location where to store it (it will clone to the location data/<your_prefered_location>)
    parser.add_argument('-repo', type=str, help='The remote git link to clone the repo')
    parser.add_argument('-dir',type=str, help='The local directory where the data is stored')
    # a config file that was used for different services => looker sdk, google cloud sdk for bigquery and github access tokens for pulling
    # default file it looks for is config.ini
    parser.add_argument('-config_file',type=str,default='config.ini', help='Local .ini file which stores config values')


    args = parser.parse_args()

    if not args.repo and not args.dir and not args.config_file:
        print("Missing arguments use the --help flag for further arguments")
        sys.exit()
    
    # pull in the configuration values from the specified configuration file
    config_values = parse_config_file(args.config_file)

    # fetch view files from the github repository
    g_downloader = GitDownloader(repo_link=args.repo,local_dir=args.dir,github_config=config_values['Github'])
    g_downloader.download_content()
    
    # parse the fetched files
    l_parser = LKMLParser(view_file_glob=g_downloader._view_file_glob,model_file_glob=g_downloader._model_file_glob)
    l_parser.read_view_files()
    l_parser.get_model_name()
    l_parser.sanitize_content()
    l_parser.create_df()

    # fetch sys activity data from looker instance for a particular dashboard
    s_downloader = SysActivityDownloader(config_values['Looker'])
    s_downloader.fetch_dashboard_list(l_parser.model_name)
    s_downloader.fetch_data()
    s_downloader.create_df()

    merged_df = merge_dataframes(l_parser.view_level_df,s_downloader.sys_activity_df)

    merged_df.to_parquet("merged_data.parquet",index=False)
    l_parser.view_metadata_df.to_parquet('metadata.parquet',index=False)
    
    mapping = {
        "metadata":"metadata.parquet",
        "merged":"merged_data.parquet"
    }
    bq_service = BigqueryService(config_values['GCP'],name_mapping=mapping)
    bq_service.create_tables()


if __name__ == "__main__":
    main()