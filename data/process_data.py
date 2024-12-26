"""Disaster Response Pipeline - Preprocessing script

Arguments:
  messages - Path to the messages csv file
  categories - Path to the categories csv file
  database - Path to the database file for saving cleaned data

Example:
  $ python process_data.py messages.csv categories.csv disaster_response.db

"""
import logging
from pathlib import Path
from argparse import ArgumentParser

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def load_data(
    messages_filepath: Path,
    categories_filepath: Path
) -> pd.DataFrame:
  messages_df = pd.read_csv(messages_filepath)
  categories_df = pd.read_csv(categories_filepath)
  data_df = messages_df.merge(categories_df, on=['id'])
  return data_df


def clean_data(data_df: pd.DataFrame) -> pd.DataFrame:
  expanded_categories_df = data_df.categories.str.split(';', expand=True)
  expanded_categories_first_row = expanded_categories_df.iloc[0]
  expanded_categories_df.columns = expanded_categories_first_row.apply(lambda column: column.split('-')[0])
  expanded_categories_df = expanded_categories_df.map(lambda row: row.split('-')[1]).astype(int)

  data_df.drop(columns=['categories'], inplace=True)
  data_df = pd.concat([data_df, expanded_categories_df], axis=1)

  num_duplicated_messages = data_df[data_df.duplicated()].shape[0]
  num_all_messages = data_df.shape[0]
  logger.info(f'Percentage of duplicates in dataset: {num_duplicated_messages / num_all_messages * 100}')
  logger.info(f'Removing duplicated messages...')
  data_df.drop_duplicates(keep='first', inplace=True)

  logger.info(f'Number of messages in dataset: {len(data_df)}')
  return data_df


def save_data(df: pd.DataFrame, database_filename: Path) -> None:
  engine = create_engine(f'sqlite:///{database_filename.name}')
  df.to_sql('messages', engine, index=False)


def main():
  parser = ArgumentParser(__doc__)
  parser.add_argument('messages', help='Path to messages file', type=Path)
  parser.add_argument('categories', help='Path to categories file', type=Path)
  parser.add_argument('database', help='Path to database file', type=Path)
  args = parser.parse_args()

  messages_filepath = args.messages
  categories_filepath = args.categories
  database_filepath = args.database

  logger.info(f'Loading data...\n    MESSAGES: {messages_filepath}\n    CATEGORIES: {categories_filepath}')
  data_df = load_data(messages_filepath, categories_filepath)

  logger.info('Cleaning data...')
  cleaned_data_df = clean_data(data_df)

  logger.info(f'Saving data...\n    DATABASE: {database_filepath}')
  save_data(cleaned_data_df, database_filepath)

  logger.info('Cleaned data saved to database!')


if __name__ == '__main__':
  main()
