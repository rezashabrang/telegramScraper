import configparser
from telethon import TelegramClient, connection
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.types import PeerChannel
import telethon.sync
from telethon.tl.functions.messages import (GetHistoryRequest)
import csv
import datetime
from time import time
import pandas as pd
from sqlalchemy import types
import sqlalchemy as sal
import re


# Formatting Date
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


# Reading infos from config file
s = time()
config = configparser.ConfigParser()
config.read("config.ini")
api_id = config['Telegram']['api_id']
api_hash = str(config['Telegram']['api_hash'])
phone = config['Telegram']['phone']
username = config['Telegram']['username']
# Setting up Client
# Proxy details below
# connection = connection.ConnectionTcpMTProxyRandomizedIntermediate,
# proxy = ('168.119.97.180', 88, 'dd00000000000000000000000000000000')
# noinspection PyTypeChecker
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

input_channel = 'https://t.me/ahan_online'
# -----------------------------------------START of Getting Channel Messages
offset_id = 0
limit = 100
all_messages = []
total_messages = 0

if input_channel.isdigit():
    entity = PeerChannel(int(input_channel))
else:
    entity = input_channel

my_channel = client.get_entity(entity)
while True:
    print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
    # noinspection PyTypeChecker
    history = client(GetHistoryRequest(
        peer=my_channel,
        offset_id=offset_id,
        offset_date=None,
        add_offset=0,
        limit=limit,
        max_id=0,
        min_id=0,
        hash=0
    ))
    if not history.messages:
        break
    messages = history.messages
    for message in messages:
        all_messages.append(message.to_dict())
    offset_id = messages[len(messages) - 1].id
    total_messages = len(all_messages)
count_failed = 1
result_posts = pd.DataFrame(columns=['message_id', 'post_views', 'post_date', 'post_text'])
result_posts = result_posts.iloc[1:]
for telmessage in all_messages:
    # noinspection PyBroadException
    try:
        message_id = telmessage['id']
        views = telmessage['views']
        message_text = telmessage['message']
        date_time = date_handle(telmessage['date'])
        data = [message_id, views, date_time, message_text]
        # Posts Dataframe
        df_append = pd.DataFrame([data], columns=['message_id', 'post_views', 'post_date', 'post_text'])
        result_posts = result_posts.append(df_append)
        # Writing Post Infos
        with open('tel-post-analytics.csv', 'a+', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow((message_id, views, date_time, message_text))
    except:
        print("Post Failed " + str(count_failed))
        count_failed += 1
# -------------------------------------------END of Getting channel messages
# -----------------------------------------START of Getting channel members

if input_channel.isdigit():
    entity = PeerChannel(int(input_channel))
else:
    entity = input_channel

my_channel = client.get_entity(entity)
offset = 0
limit = 100
all_users = []
all_participants = []

while True:
    # noinspection PyTypeChecker
    participants = client(GetParticipantsRequest(
        my_channel, ChannelParticipantsSearch(''), offset, limit,
        hash=0
    ))
    if not participants.users:
        break
    all_participants.extend(participants.participants)
    all_users.extend(participants.users)
    offset += len(participants.users)
print(participants)

count_failed = 1
count = 1
result_users = pd.DataFrame(
    columns=['user_id', 'bot_status', 'first_name', 'last_name', 'user_name', 'phone_num', 'join_date', 'total_users'])
result_users = result_users.iloc[1:]
total_users = (client.get_participants(my_channel)).total
print(total_users)
# For Getting Participants
for participant, user in zip(all_participants, all_users):
    # noinspection PyBroadException
    try:
        id = user.id
        # bot status
        if user.bot:
            bot_status = 'True'
        else:
            bot_status = 'False'
        first_name = user.first_name
        last_name = user.last_name
        user_name = user.username
        phone_num = user.phone
        join_date = ''
        # noinspection PyBroadException
        try:
            join_date = date_handle(participant.date)
        except:
            print(user_name + " Failed")
        # Users Dataframe
        df_append = pd.DataFrame(
            [[id, bot_status, first_name, last_name, user_name, phone_num, join_date, total_users]],
            columns=['user_id', 'bot_status', 'first_name', 'last_name', 'user_name', 'phone_num',
                     'join_date', 'total_users'])
        result_users = result_users.append(df_append)
        # Writing Users
        with open('tel-users-analytics.csv', 'a+', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow((id, bot_status, first_name, last_name, user_name, phone_num, join_date, total_users))
        print("Added {} users".format(count))
        count += 1
    except:
        print('User Failed: ' + str(count_failed))
        count_failed += 1

# ----------------------------------------- END of Getting channel members

e = time()
print('TOTAL FETCH TIME: ' + str(e - s))

# ------------------------------------------ START of SQL Transfer
s = time()
result_posts['Update'] = pd.to_datetime('today')
result_users['Update'] = pd.to_datetime('today')
print(result_posts)
print(result_users)
engine = sal.create_engine(
    '')
conn = engine.connect()
data_types = handle_types(result_users)
result_users.to_sql('Temp_telegram_members', conn, if_exists='append', dtype=data_types, index=False)
data_types = handle_types(result_posts)
result_posts.to_sql('Temp_telegram_posts', conn, if_exists='append', dtype=data_types, index=False)
conn.close()
print('Telegram Data Successfully transferred to Sql Server')
e = time()
print('TOTAL TRANSFER TIME: ' + str(e - s))
# ------------------------------------------ END of SQL Transfer
