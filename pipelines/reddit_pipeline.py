from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # connecting to reddit instance -> ETL to CSV
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')

    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)

    post_df = transform_data(post_df)

    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path
