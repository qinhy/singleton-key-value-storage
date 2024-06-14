import os
import re
import requests
import numpy as np
from typing import Any, Callable, List, Dict
import json
import threading
import time
import json
import threading
from LLMstore import AbstractContent, AbstractContentController, Author, ContentGroup, LLMstore

class Speaker:
    def __init__(self,author) -> None:
        self.author:Author = author
        self.id = self.author.id
        self.name = self.author.name#self.id[-8:]
        self.room:ChatRoom = None
        self.is_speaking = 0
        self.new_message_callbacks=[]
        self.mention_callbacks=[]

    def add_new_message_callback(self, cb):
        self.new_message_callbacks.append(cb)
        return self

    def add_mention_callback(self, cb):
        self.mention_callbacks.append(cb)
        return self
        
    def on_new_message(self, message:AbstractContent):
        for cb in self.new_message_callbacks:
            cb(self,message)

    def on_mention(self, message:AbstractContent):
        for cb in self.mention_callbacks:
            cb(self,message)

    def entery_room(self,room):
        self.room:ChatRoom = room
        self.room.add_speaker(self)
        rooms = self.author.metadata.get('groups','')
        if self.room.chatroom.model.id not in rooms:
            rooms += self.room.chatroom.model.id
            self.author.controller.update_metadata('groups',rooms)
        return self
    
    def new_group(self):
        if self.room is None:
            raise ValueError('please enter room at first')
        groupid = self.room.add_content_group_to_chatroom()        
        return groupid

    def speak_img(self,imgpath:str,group_id:str=None,new_group=False):
        self.is_speaking += 1
        if self.room is not None:
            self.room.speak_img(self.id,imgpath,group_id,new_group)
        self.is_speaking -= 1
        return self

    def speak(self,msg:str,group_id:str=None,new_group=False):
        self.is_speaking += 1
        if self.room is not None:
            self.room.speak(self.id,msg,group_id,new_group)
        self.is_speaking -= 1
        return self
    
    def speak_stream(self,stream,group_id:str=None,new_group=False):
        self.is_speaking += 1
        def callback():
            self.is_speaking -= 1
        if self.room is not None:
            worker_thread = threading.Thread(target=self.room.speak_stream,args=(self.id,stream,callback,group_id,new_group))
            worker_thread.start()
        return self
    
class ChatRoom:
    def __init__(self, store:LLMstore, chatroom_id:str=None, speakers:Dict[str,Speaker]={}) -> None:
        self.store=store
        self.speakers=speakers
        self.chatroom:ContentGroup = None
        roots:List[ContentGroup] = self.store.find_all('ContentGroup:*')
        if chatroom_id is None:
            roots = [g for g in roots if len(g.parent_id)==0]
            if len(roots)==0:
                raise ValueError('no valid root group in store')
        else:
            roots = [g for g in roots if g.id==chatroom_id]
            if len(roots)==0:
                raise ValueError(f'no group({chatroom_id}) in store')        
        self.chatroom = roots[0]
        self.msgs = []
    
    def _on_message_change(self):
        self.msgs = self.get_messages_in_group()

    def add_message_to_chatroom(self, message_content: AbstractContent, group:ContentGroup=None):
        
        if group is not None and self.chatroom.model.id != group.id:
            self.add_content_group_to_chatroom(group)
        else:
            group = self.chatroom.model
        ContentGroupController(group).add_content(AbstractContentController(message_content))
        self._on_message_change()
        return message_content.id
    
    def add_content_group_to_chatroom(self, groupc: ContentGroup=None):
        group = self.chatroom.add_new_group(groupc)
        self._on_message_change()
        return group.model.id
    
    def get_messages_in_group(self,id=None):
        if id is None:
            return self.chatroom.get_children_content()
        else:
            return ContentGroupController(ContentGroup(id=id)).load().get_children_content()
    
    def get_messages_recursive_in_chatroom(self):
        return self.chatroom.get_children_content_recursive()
    
    def traverse_nested_messages(self, nested_content_list=None):
        if nested_content_list is None:nested_content_list=self.get_messages_recursive_in_chatroom()
        for element in nested_content_list:
            if isinstance(element, list):
                for e in self.traverse_nested_messages(element):
                    yield e
            else:
                yield element
    
    ##################################################################

    def add_speaker(self,speaker:Speaker):
        self.speakers[speaker.id] = speaker
        self.speakers[speaker.name] = speaker

    def get_speaker(self,speaker_id) -> Speaker:
        if speaker_id not in self.speakers:
            raise ValueError(f'no such user {speaker_id}')
        return self.speakers[speaker_id]

    def get_mentions(self, message:AbstractContent, speaker_ids=[]):
        msg_auther = self.speakers[message.author_id].name
        controller:AbstractContentController = message.controller
        mentions = re.findall(r'@([a-zA-Z0-9]+)', controller.get_data_raw())
        targets = []
        
        for mention in mentions:
            for speaker in {self.get_speaker(s) for s in speaker_ids}:                
                if speaker.name == mention and msg_auther!=mention:
                    targets.append(speaker.id)
        return targets
    
    def notify_new_message(self, message:AbstractContent, speaker_ids=[]):
        speaker_ids = set(speaker_ids)-set(self.get_mentions(message,speaker_ids))-set(self.speakers[message.author_id].id)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_new_message(message)
    
    def notify_mention(self, message:AbstractContent, speaker_ids=[]):
        speaker_ids = self.get_mentions(message, speaker_ids)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_mention(message)
    
    def _prepare_speak(self,speaker_id,group_id:str=None,new_group=False,type='Text',msg=''):
        speaker = self.get_speaker(speaker_id)

        def new_content(obj,type=type):
            if 'Text' in type:
                return getattr(obj,f'add_new_text_content')
            elif 'Image' in type:
                return getattr(obj,f'add_new_image_content')
            else:
                raise ValueError(f'Unknown type of {type}')

        if (group_id is None and not new_group) or (group_id == self.chatroom.model.id):
            tc:AbstractContentController = new_content(self.chatroom)(speaker.author.id, msg)#self.chatroom.add_new_text_content(speaker.author.id,'')
            message_id = self.add_message_to_chatroom(tc.model)

        elif group_id is not None and not new_group:
            if group_id not in self.chatroom.model.children_id:
                raise ValueError(f'no such group {group_id}')
            group:ContentGroupController = ContentGroupController(ContentGroup(id=group_id)).load()
            tc:AbstractContentController = new_content(group)(speaker.author.id, msg)#group.add_new_text_content(speaker.author.id,'')
            # message_id = self.add_message_to_chatroom(tc.model,group.model)
            self._on_message_change()

        elif group_id is None and new_group:            
            group:ContentGroupController = self.chatroom.add_new_group()
            tc:AbstractContentController = new_content(group)(speaker.author.id, msg)#group.add_new_text_content(speaker.author.id,'')
            message_id = self.add_message_to_chatroom(tc.model,group.model)
        return tc

    def speak_stream(self,speaker_id,stream,callback,group_id:str=None,new_group=False):
        content:TextContentController = None#self._prepare_speak(speaker_id,group_id,new_group)
        msg = ''
        for i,r in enumerate(stream):
            if r and i==0:
                content = self._prepare_speak(speaker_id,group_id,new_group)
            assert r is not None, f'can not prepare string reply in speak_stream! {r}'
            content.append_data_raw(r)
            msg += r
        callback()
        self.notify_new_message(content.model, self.speakers.keys())
        self.notify_mention(content.model, self.speakers.keys())

    def speak(self,speaker_id,msg:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group)
        content.update_data_raw(msg)
        self.notify_new_message(content.model, self.speakers.keys())
        self.notify_mention(content.model, self.speakers.keys())
        return content
    
    def speak_img(self,speaker_id,imagpath:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group,type='Image',msg=imagpath)
        self.notify_new_message(content.model, self.speakers.keys())
        self.notify_mention(content.model, self.speakers.keys())
        return content
    #######################################################################

    def msgsDict(self,refresh=False,msgs=None,todict=None):        
        if todict is None:
            def todict(c:AbstractContentController):
                n = c.__class__.__name__
                if 'Text' in n:
                    return {"type": "text","text": c.get_data_raw()}
                
                if 'Image' in n:
                    return {"type": "image_url","image_url": {
                                "url": f"data:image/jpeg;base64,{c.get_data_raw()}"}}
                
            # todict = lambda c:c.load().get_data_raw()
            
        if refresh:
            self.msgs:List[AbstractContentController] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs
        res = []
        for m in msgs:
            if 'ContentGroup' not in m.__class__.__name__:
                name = m.get_author().model.name
                role = m.get_author().model.role
                if 'EmbeddingContent' in m.__class__.__name__:
                    m:EmbeddingContentController = m
                    t = m.get_target().get_data_raw()[:10]
                    # print(f'{intents}{self.speakers[m.model.author_id].name}: "{t}"=>{m.load().get_data_raw()[:5]}...')
                elif 'TextContent' in m.__class__.__name__:
                    res.append(dict(name=name,role=role,content=todict(m)))
                else:
                    res.append(dict(name=name,role=role,content=todict(m)))
            else:
                res.append(self.msgsDict(False,self.get_messages_in_group(m.model.id)))
        return res


    def printMsgs(self,refresh=False,intent=0,msgs=None):
        if refresh:
            self.msgs:List[AbstractContentController] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs
        intents = "".join([' ']*intent)
        print("", flush=True)
        print(f'{intents}#############################################################')
        
        for m in msgs:
            print(f'{intents}-------------------------------------------------------------')
            if 'ContentGroup' not in m.__class__.__name__:
                if 'EmbeddingContent' in m.__class__.__name__:
                    m:EmbeddingContentController = m
                    t = m.get_target().get_data_raw()[:10]
                    print(f'{intents}{self.speakers[m.model.author_id].name}: "{t}"=>{m.load().get_data_raw()[:5]}...')
                elif 'ImageContent' in m.__class__.__name__:
                    m:ImageContentController = m
                    im = m.load().get_image()
                    print(f'{intents}{self.speakers[m.model.author_id].name}: Image{im.size} of {im.info}')
                else:
                    print(f'{intents}{self.speakers[m.model.author_id].name}: {m.load().get_data_raw()}')
            else:
                self.printMsgs(False,intent+4,self.get_messages_in_group(m.model.id))
        print(f'{intents}-------------------------------------------------------------')
        print(f'{intents}#############################################################')
