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

cr.speakers.get('RoomDataSaver',Speaker(cr.store.add_new_author(name="RoomDataSaver", role="assistant"))
                ).entery_room(cr).add_new_message_callback(lambda s,m:[sss.dump('visionRoom.json')])

configs = Configs()
model=configs.new_config('model','gpt-4o')
num_passages=configs.new_config('num passages','1')
OPENAI_API_KEY=configs.new_config('openai api key',os.environ['OPENAI_API_KEY'])

def openairequest(url,jsonstr):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY.value}","Content-Type": "application/json"}
    data = jsonstr
    response = requests.post(url=url, data=data, headers=headers)
    try:
        response = json.loads(response.text,strict=False)
    except Exception as e:
        if type(response) is not dict:
            return {'error':f'{e}'}
        else:
            return {'error':f'{response}({e})'}
    return response

def openaimsg():
    msgd = cr.msgsDict(True,todict = lambda c:c.controller.get_data_raw())
    def mergelist(l):
        name,role = l[0]['name'],l[0]['role']        
        return dict(name=name,role=role,content=[c['content'] for c in l])
    messages = [(d if type(d) is not list else mergelist(d)) for d in msgd]
    return messages

def pygpt(speaker:Speaker, msg:AbstractContent):
    data = {"model": model.value,
            "messages": [{"role":"system",
                        "content":"Your name is VisionMaster and good at making description of image."}] + openaimsg()[-int(num_passages.value):]}
    response = openairequest(url="https://api.openai.com/v1/chat/completions",jsonstr=json.dumps(data,ensure_ascii=False))
    speaker.speak(str(response['error']) if 'error' in response else response['choices'][0]['message']['content'])

cr.speakers.get('VisionMaster',Speaker(cr.store.add_new_author(name="VisionMaster",role="assistant",metadata={'model':model.value}))
                                ).entery_room(cr).add_mention_callback(pygpt)

u = cr.speakers.get('User',Speaker(cr.store.add_new_author(name="User", role="user"))).entery_room(cr)

# u.speak('hi')
# gid = u.new_group()
# u.speak_img(r"D:\Download\2975157083_4567dde5d5_z.jpg",gid)
# u.speak('@VisionMaster hi, please tell me the details in the image.',gid)

try:    
    build_gui(cr,configs.tolist(),'visionRoom.json').launch()
except Exception as e:
    print(e)