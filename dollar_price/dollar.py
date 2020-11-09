import configparser
from telethon import TelegramClient, connection
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import PeerChannel
import telethon.sync
from telethon.tl.functions.messages import GetHistoryRequest
import csv
from datetime import date
import datetime
from time import time
import pandas as pd
from sqlalchemy import types
import sqlalchemy as sal
import re


def date_handle(date):
    # remove " marks
    date += datetime.timedelta(hours=3, minutes=30)
    date = date.replace(tzinfo=None)
    return date


def handle_types(df):
    dtypeditct = {}
    for column_name, column_type in zip(df.columns, df.dtypes):
        if str(column_type) == 'int64':
            dtypeditct.update({column_name: types.BIGINT})
        elif str(column_type) == 'float64':
            dtypeditct.update({column_name: types.FLOAT})
        elif re.search('date', str(column_type)):
            dtypeditct.update({column_name: types.DATETIME})
        elif str(column_type) == 'bool':
            dtypeditct.update({column_name: types.BOOLEAN})
        else:
            dtypeditct.update({column_name: types.NVARCHAR})
    return dtypeditct


# --------------------------------- START initializing client
config = configparser.ConfigParser()
config.read("config.ini")
api_id = config['Telegram']['api_id']
api_hash = str(config['Telegram']['api_hash'])
phone = config['Telegram']['phone']
username = config['Telegram']['username']
# Setting up Client
# Proxy details below

client = TelegramClient(username, api_id, api_hash, connection=connection.ConnectionTcpMTProxyRandomizedIntermediate,
                        proxy=('DynU.cOm.dYNu-CoM.gA.', 8080, 'dd00000000000000000000000000000000'))
client.start()
print('client Started')
if not client.is_user_authorized():
    client.send_code_request(phone)
    try:
        client.sign_in(phone, input('Enter the code: '))
    except SessionPasswordNeededError:
        client.sign_in(password=input('Password: '))
print("Authentication Successful")
# --------------------------------- END initializing client
# --------------------------------- START Setting Channel Entity
input_channel = 'https://t.me/dollar_tehran3bze'
if input_channel.isdigit():
    entity = PeerChannel(int(input_channel))
else:
    entity = input_channel
channel_ent = client.get_entity(entity)
# --------------------------------- END Setting Channel Entity
# --------------------------------- START Getting channel messages
all_messages = []
history = client(GetHistoryRequest(
    peer=channel_ent,
    offset_id=0,
    offset_date=None,
    add_offset=0,
    limit=10,
    max_id=0,
    min_id=0,
    hash=0
))
messages = history.messages
for message in messages:
    all_messages.append(message.to_dict())
# -------------------------------- END Getting Channel messages
# -------------------------------- START Processing and adding to dataframe
dollar_df = pd.DataFrame(columns=['message', 'message_date'])
columns = dollar_df.columns
for message in all_messages:
    text = message['message']
    message_date = date_handle(message['date'])
    values = [text, message_date]
    dollar_df = dollar_df.append(dict(zip(columns, values)), ignore_index=True)
# -------------------------------- END Processing and adding to dataframe
# -------------------------------- START SQL Transfer
dollar_df['Update'] = pd.to_datetime('today')
engine = sal.create_engine(
    '')
conn = engine.connect()
data_types = handle_types(dollar_df)
dollar_df.to_sql('Temp_telegram_dollar', conn, if_exists='append', dtype=data_types, index=False)
conn.close()
print('Telegram dollar Data Successfully transferred to Sql Server')
# -------------------------------- END SQL Transfer
