import json
import os
import requests

from SingletonStorage.LLMroom import ChatRoom, Speaker
from SingletonStorage.LLMroomGradio import Configs, build_gui
from SingletonStorage.LLMstore import AbstractContent, LLMstore

sss = LLMstore()
try:
    sss.load('visionRoom.json')
except Exception as e:
    print(e)

cr = ChatRoom(sss)

def get_speaker(name,role,metadata={}):
    if name in cr.speakers:
        return cr.speakers[name]
    return Speaker(cr.store.add_new_author(name=name, role=role, metadata=metadata))

get_speaker('RoomDataSaver','assistant').entery_room(cr).add_new_message_callback(lambda s,m:sss.dump('visionRoom.json'))

configs = Configs()
model=configs.new_config('model','gpt-4o')
num_passages=configs.new_config('num passages','1')
stream=configs.new_config('stream','no')
OPENAI_API_KEY=configs.new_config('openai api key',os.environ['OPENAI_API_KEY'])

def openai_streaming_request(url="https://api.openai.com/v1/chat/completions", data=r"{}",
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY.value}","Content-Type": "application/json"}):
    try:
        with requests.post(url=url, data=data, headers=headers, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    decoded_chunks = chunk.decode('utf-8').replace('\n','').split('data: ')
                    for decoded_chunk in decoded_chunks:
                        if len(decoded_chunk)==0:continue
                        if '[DONE]' in decoded_chunk:return
                        chunk_dict = json.loads(decoded_chunk, strict=False)
                        yield chunk_dict
    except Exception as e:
        yield {'error': f'{e}'}

def openai_request(url="https://api.openai.com/v1/chat/completions",data=r"{}",
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY.value}","Content-Type": "application/json"}):

    response = requests.post(url=url, data=data, headers=headers)
    try:
        response = json.loads(response.text,strict=False)
    except Exception as e:
        if type(response) is not dict:
            return {'error':f'{e}'}
        else:
            return {'error':f'{response}({e})'}
    return response

def openai_msg():
    msgd = cr.msgsDict(True,todict = lambda c:c.controller.get_data_raw())
    def mergelist(l):
        name,role = l[0]['name'],l[0]['role']        
        return dict(name=name,role=role,content=[c['content'] for c in l])
    messages = [(d if type(d) is not list else mergelist(d)) for d in msgd]
    return messages

def pygpt(speaker:Speaker, msg:AbstractContent):
    is_stream = any([i == stream.value.lower() for i in ['y','yes','true']])
    data = {"model": model.value, "stream":is_stream,
            "messages": [{"role":"system",
                        "content":"Your name is VisionMaster and good at making description of image."}
                        ] + openai_msg()[-int(num_passages.value):]}
    if is_stream:
        def genmsg():
            for response in openai_streaming_request(url="https://api.openai.com/v1/chat/completions",data=json.dumps(data,ensure_ascii=False)):
                yield response['choices'][0]['delta'].get('content','')            
        speaker.speak_stream(genmsg())
    else:
        response = openai_request(url="https://api.openai.com/v1/chat/completions",data=json.dumps(data,ensure_ascii=False))
        speaker.speak(str(response['error']) if 'error' in response else response['choices'][0]['message']['content'])

get_speaker('VisionMaster','assistant',{'model':model.value}).entery_room(cr).add_mention_callback(pygpt)
get_speaker('User','user').entery_room(cr)

# u.speak('hi')
# gid = u.new_group()
# u.speak_img(r"D:\Download\2975157083_4567dde5d5_z.jpg",gid)
# u.speak('@VisionMaster hi, please tell me the details in the image.',gid)

try:    
    build_gui(cr,configs.tolist(),'visionRoom.json').launch()
except Exception as e:
    print(e)