from telethon import TelegramClient, sync
import pandas as pd
import csv

# load api secrets and wished groups
# format of secret file:
# api_id,api_hash
# group0,group1,...
with open("../secrets.csv") as secrets:
    csv_reader = csv.reader(secrets)
    api_id, api_hash = next(csv_reader)
    groups = next(csv_reader)

# check if secrets are read correctly (watch out for wild whitespaces
print(api_id, api_hash, groups)

# start telethon
client = TelegramClient("session1", api_id, api_hash).start()

# aggregate data
data = {}
for group in groups:
    print(f"working on {group}")
    # download last n messages from group
    last_n = 100_000
    chat = client.get_messages(group, last_n)
    print("Download finished. Managing data.")
    # define data strucutre for data
    message_id = []
    message = []
    sender = []
    reply_to = []
    time = []
    
    for msg in chat:
        message_id.append(msg.id)
        message.append(msg.message)
        sender.append(msg.from_id)
        reply_to.append(msg.reply_to_msg_id)
        time.append(msg.date)
    
    this_data = {"message_id":message_id, "message": message, "sender":sender, "reply_to":reply_to, "time":time}
    data[group] = pd.DataFrame(this_data)

df = pd.concat(data, keys=groups)
df.to_pickle("../data/data_medium.pkl")
    
        
