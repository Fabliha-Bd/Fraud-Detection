#!/usr/bin/env python
# coding: utf-8

# In[1]:


import base64
import message_pb2
import pandas as pd
import json
from read_protobuf import read_protobuf
from google.protobuf.json_format import MessageToJson

def b64_decoder(msg):
    message_bytes = base64.b64decode(msg)
    return message_bytes
def protobuf_deserializer(msg):
    location = message_pb2.Location()
    location.ParseFromString(msg)
    #print(location)
    return location
    


# In[2]:


def base64_converter(coordinates):
    df = coordinates["EnqueuedTimeUtc"]
    df_body = coordinates['Body']
    df2= df_body.apply(lambda x: b64_decoder(x))
    df2= df2.apply(lambda x: protobuf_deserializer(x))
    df2 = df2.apply(lambda x: MessageToJson(x))
    df2 = df2.apply(lambda x: json.loads(x))
    df_final = df2.apply(pd.Series)
    df_final['datetime'] = df
    df_final.head(10)
    return df_final


# In[ ]:




