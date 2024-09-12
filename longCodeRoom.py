import json
import os
import re
import threading
import time
import requests

from SingletonStorage.LLMroom import ChatRoom, Speaker
from SingletonStorage.LLMroomGradio import Configs, build_gui
from SingletonStorage.LLMstore import Model4LLM, LLMstore

sss = LLMstore()
ROOM_NAME = 'longCodeRoom'
try:
    sss.load(f'{ROOM_NAME}.json')
except Exception as e:
    print(e)

cr = ChatRoom(sss)

def get_speaker(name,role,metadata={}):
    if name in cr.speakers:
        return cr.speakers[name]
    return Speaker(cr.store.add_new_author(name=name, role=role, metadata=metadata))

get_speaker('RoomDataSaver','assistant').entery_room(cr).add_new_message_callback(lambda s,m:sss.dump(f'{ROOM_NAME}.json'))

configs = Configs()
OPENAI_API_KEY=configs.new_config('openai api key',os.environ['OPENAI_API_KEY'])
model=configs.new_config('model','gpt-4o')
num_passages=configs.new_config('num passages','1')
stream=configs.new_config('stream','no')
sysp=configs.new_config('sysp','You are an expert in code explanation, familiar with Electron, and Python.')

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
    msgd = cr.msgsDict(True,todict = lambda c:c.get_controller().get_data_raw())
    def mergelist(l):
        name,role = l[0]['name'],l[0]['role']        
        return dict(name=name,role=role,content=[c['content'] for c in l])
    messages = [(d if type(d) is not list else mergelist(d)) for d in msgd]
    return messages

########################################################################
splitter_lines=configs.new_config('splitter lines','100')
splitter_overlap=configs.new_config('splitter overlap','0.3')
TextSplitterQueue = []
def TextSplitter_callback(speaker:Speaker, msg:Model4LLM.AbstractContent):
    pass

CodeExplainer_limit_words=configs.new_config('CodeExplainer limit words','1000')
def CodeExplainer_callback(speaker:Speaker, msg:Model4LLM.AbstractContent):    
    is_stream = any([i == stream.value.lower() for i in ['y','yes','true']])
    data = {"model": model.value, "stream":is_stream,
            "messages": [{"role":"system", "content":sysp.value}
                        ] + openai_msg()[-int(num_passages.value):]}
    if is_stream:
        def genmsg():
            for response in openai_streaming_request(url="https://api.openai.com/v1/chat/completions",data=json.dumps(data,ensure_ascii=False)):
                yield response['choices'][0]['delta'].get('content','')            
        speaker.speak_stream(genmsg())
    else:
        response = openai_request(url="https://api.openai.com/v1/chat/completions",data=json.dumps(data,ensure_ascii=False))
        speaker.speak(str(response['error']) if 'error' in response else response['choices'][0]['message']['content'])


reminderp=configs.new_config('reminderp','''
@CodeExplainer
I have an app built with Electron and Python.
I will provide pieces of the project code along with prior explanations.
Your task is to read each new code snippet and add new explanations accordingly.  
You should reply in Japanese with explanations only, without any additional information.

## Your Reply Format Example (should not over {limit_words} words)
@QueueReminder my explanation is following.
```explanation
- This code shows ...
```
                                                       
## Code Snippet
code path : {code_path}
```javascript
{code}
```
                             
## Previous Explanations
```explanation
{pre_explanation}
```''')
reminder_results=[]
def QueueReminder_callback(speaker:Speaker, msg:Model4LLM.AbstractContent):    
    def extract_explanation_block(text):
        matches = re.findall(r"```explanation\s*(\{.*?\})\s*```", text, re.DOTALL)
        return matches if matches else []
    msgc = msg.get_controller()
    try:
        pass
    except Exception as e:
        speaker.speak(f'{e}')

reminder_loop=configs.new_config('reminder_loop','True')
def QueueReminder_loop():
    while True:
        time.sleep(10)
        if len(reminder_loop.value)==0:continue
        if len(TextSplitterQueue)==0:continue
        try:
            cr.speakers['QueueReminder'].speak(reminderp.value.format(
                limit_words=CodeExplainer_limit_words.value,
                code_path='',
                code=TextSplitterQueue.pop(0),
                pre_explanation=reminder_results[-1] if len(reminder_results)>0 else '',
            ))
        except Exception as e:
            cr.speakers['QueueReminder'].speak(f'{e}')
            reminder_loop.value = ''
threading.Thread(target=QueueReminder_loop).start()

get_speaker('CodeExplainer','assistant',metadata={'type':'llm','model':model.value}
            ).entery_room(cr).add_mention_callback(CodeExplainer_callback)
            
get_speaker('QueueReminder','assistant',metadata={'type':'function'}
            ).entery_room(cr).add_mention_callback(QueueReminder_callback)
            
get_speaker('TextSplitter','assistant',metadata={'type':'function'}
            ).entery_room(cr).add_mention_callback(TextSplitter_callback)
            
get_speaker('User','user').entery_room(cr)


try:    
    build_gui(cr,configs.tolist(),f'{ROOM_NAME}.json').launch()
except Exception as e:
    print(e)