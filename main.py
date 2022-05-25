from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from datetime import datetime, timedelta
from google.cloud import bigquery
import json
import io
##########################INPUT VARIABLES HERE################################

START_DATE = str(datetime.now().date() - timedelta(days=1))
END_DATE = START_DATE

AD_ACCOUNTS = ['act_xxxxxxxxxxxxxxx',
               'act_yyyyyyyyyyyyyyy'] #replace with your Facebook Ads account numbers

BQ_DATASET_NAME = 'Facebook' #Needs to be created ahead of the script running, in GCP.
BQ_TABLE_NAME = 'FACEBOOK_ADS_DATA'
SERVICE_ACCOUNT_FILE = 'xxxxx.json' #replace with your GCP service account file for credentials.

my_app_id = 'xxxx' #replace with your Facebook Ads API details
my_app_secret = 'yyyy'
my_access_token = 'zzzz'

##########################END OF VARIABLES####################################

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

def getInsights(AD_ACCOUNT):
    
    params = {
        'time_range': {
            'since':  START_DATE, 
            'until': END_DATE
            }, 
        'fields':
                ','.join(
                    ['account_currency',
                      'account_id',
                      'ad_id',
                      'ad_name',
                      'adset_id',
                      'adset_name',
                      'account_name',
                      'actions',
                      'campaign_name',
                      'clicks',
                      'cost_per_action_type',
                      'cost_per_inline_link_click',
                      'cost_per_inline_post_engagement',
                      'cost_per_outbound_click',
                      'cost_per_unique_action_type',
                      'cost_per_unique_click',
                      'cost_per_unique_inline_link_click',
                      'cost_per_unique_outbound_click',
                      'cpc',
                      'cpm',
                      'cpp',
                      'ctr',
                      'date_start',
                      'date_stop',
                      'frequency',
                      'impressions',
                      'inline_link_clicks',
                      'inline_link_click_ctr',
                      'inline_post_engagement',
                      'objective',
                      'outbound_clicks',
                      'outbound_clicks_ctr',
                      'reach',
                      'social_spend',
                      'spend',
                      'unique_actions',
                      'unique_clicks',
                      'unique_ctr',
                      'unique_inline_link_click_ctr',
                      'unique_inline_link_clicks',
                      'unique_link_clicks_ctr',
                      'unique_outbound_clicks',
                      'unique_outbound_clicks_ctr',
                      'video_play_curve_actions',
                      'website_ctr',
                      'video_thruplay_watched_actions',                  
                      'video_30_sec_watched_actions',
                      'video_avg_time_watched_actions',
                      'video_p100_watched_actions',
                      'video_p25_watched_actions',
                      'video_p50_watched_actions',
                      'video_p75_watched_actions',
                      'video_p95_watched_actions',
                      'video_play_actions'
                    ]
                ),
        'level': 'ad',
        'time_increment': 1}
    
    insights = AdAccount(AD_ACCOUNT).get_insights(params=params)

    return (insights)
    
def insightsToJSON(insights):
    
    FRAMES = []
    
    for item in insights:  
        data = dict(item)
        FRAMES.append(data) 
        data = dict(item)
        FRAMES.append(data)

    #Turns list of dictionaries into ND-JSON format
    ND_JSON = "\n".join([json.dumps(x) for x in FRAMES])

    return ND_JSON


def loadJSONToBigQuery(ND_JSON, SERVICE_ACCOUNT_FILE ,BQ_DATASET_NAME, BQ_TABLE_NAME):
        # establish a BigQuery client
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        
        #turns JSON data into a file type object
        string_data = io.StringIO(ND_JSON)
        
        job_config = bigquery.LoadJobConfig(autodetect=True)
        
        # Set the destination table
        table_ref = client.dataset(BQ_DATASET_NAME).table(BQ_TABLE_NAME)
    
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON #TEST
        job_config.write_disposition = 'WRITE_APPEND'
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    
        load_job = client.load_table_from_file(string_data, table_ref, job_config=job_config)
        load_job.result()


def main():
    for AD_ACCOUNT in AD_ACCOUNTS:
        insights = getInsights(AD_ACCOUNT)
        try:
            ND_JSON = insightsToJSON(insights)
            #TOTAL_DF.to_csv('test4.csv')
            loadJSONToBigQuery(ND_JSON, SERVICE_ACCOUNT_FILE, BQ_DATASET_NAME, BQ_TABLE_NAME)
            print(AD_ACCOUNT + " done")
        except:
            print ("No data in " + AD_ACCOUNT)
        
if __name__ == '__main__':
    main()
