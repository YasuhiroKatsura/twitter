import logging, os, datetime
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from modules import stream as st # Stream APIを利用するモジュール

ADLS_ACCOUNT_NAME = os.getenv('ADLS_ACCOUNT_NAME')
ADLS_ACCESS_KEY = os.getenv('ADLS_ACCESS_KEY')
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
BEARER_TOKEN = os.getenv('BEARER_TOKEN')


def main(req):
    logging.info("=====Function triggered=====")

    df = get_stream_data()
    send_to_adls(df.to_parquet())

    return func.HttpResponse(f"HTTP triggered function executed successfully.")


# Stream APIを利用
def get_stream_data():
    logging.info("=====Connecting to endpoint=====")

    query = "くまちゃん OR ネコちゃん OR ワンちゃん -is:retweet" # フィルター条件
    max_tweets = 200 # max_tweets 件のツイートを取得したら処理を終了

    return st.run(BEARER_TOKEN, query, max_tweets)

# Search APIを利用
def get_search_data():
    pass


def get_filename():
    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    now = datetime.datetime.now(JST)

    return "{:%Y%m%d%H%M%S}".format(now) + ".parquet"


# ADLS Gen2と連携
def send_to_adls(data):
    logging.info("=====Saving to ADLS=====")

    connect_str = "DefaultEndpointsProtocol=https;AccountName=" + ADLS_ACCOUNT_NAME \
                + ";AccountKey=" + ADLS_ACCESS_KEY \
                + ";EndpointSuffix=core.windows.net"

    service_client = DataLakeServiceClient.from_connection_string(connect_str)
    file_system_client = service_client.get_file_system_client(file_system="twitterfilesystem")
    directory_client = file_system_client.get_directory_client("data")
    file_client = directory_client.create_file(get_filename())

    file_client.upload_data(data=data, overwrite=True)
    file_client.flush_data(len(data))