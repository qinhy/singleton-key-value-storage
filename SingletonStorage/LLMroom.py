

import threading
from .LLMstore import Model4LLM, LLMstore

import re
from typing import List, Dict
class Speaker:
    def __init__(self,author:Model4LLM.Author) -> None:
        self.author = author
        self.id = self.author.get_id()
        self.name = self.author.name#self.get_id()[-8:]
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
        
    def on_new_message(self, message:Model4LLM.AbstractContent):
        for cb in self.new_message_callbacks:
            cb(self,message)

    def on_mention(self, message:Model4LLM.AbstractContent):
        for cb in self.mention_callbacks:
            cb(self,message)

    def entery_room(self,room):
        self.room:ChatRoom = room
        self.room.add_speaker(self)
        rooms = self.author.metadata.get('groups','')
        if self.room.chatroom().get_id() not in rooms:
            rooms += self.room.chatroom().get_id()
            self.author.get_controller().update_metadata('groups',rooms)
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
        chatroom:Model4LLM.ContentGroup = None
        roots:List[Model4LLM.ContentGroup] = self.store.find_all('ContentGroup:*')
        if chatroom_id is None:
            roots = [g for g in roots if len(g.parent_id)==0]
            if len(roots)==0:
                print(f'no group({chatroom_id}) in store, make a new one')
                roots = [self.store.add_new_root_group()]
        else:
            roots = [g for g in roots if g.get_id()==chatroom_id]
            if len(roots)==0:
                raise ValueError(f'no group({chatroom_id}) in store')        
        chatroom = roots[0]

        self.chatroom_id = chatroom.get_id()
        self.msgs = []
        
        for a in self.store.find_all_authors():
            if self.chatroom_id in a.metadata.get('groups',''):
                print(a)
                self.speakers[a.get_id()] = Speaker(a).entery_room(self)
                self.speakers[a.name] = self.speakers[a.get_id()]
        print(self.speakers)
    
    def chatroom(self):
        return self.store.find_group(self.chatroom_id)

    def _on_message_change(self):
        self.msgs = self.get_messages_in_group()
    
    def add_content_group_to_chatroom(self):
        child = self.chatroom().get_controller().add_new_child_group()
        self._on_message_change()
        return child.get_id()
    
    def get_messages_in_group(self,id=None)->List[Model4LLM.AbstractContent]:
        if id is None:
            return self.chatroom().get_controller().get_children_content()
        else:
            return self.store.find_group(id).get_controller().get_children_content()
    
    def get_messages_recursive_in_chatroom(self):
        return self.chatroom().get_controller().get_children_content_recursive()
    
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

    def get_mentions(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        msg_auther = self.speakers[message.author_id].name
        mentions = re.findall(r'@([a-zA-Z0-9]+)', message.get_controller().get_data_raw())
        targets = []
        
        for mention in mentions:
            for speaker in {self.get_speaker(s) for s in speaker_ids}:                
                if speaker.name == mention and msg_auther!=mention:
                    targets.append(speaker.id)
        return targets
    
    def notify_new_message(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        speaker_ids = set(speaker_ids)-set(self.get_mentions(message,speaker_ids))-set(self.speakers[message.author_id].id)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_new_message(message)
    
    def notify_mention(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        speaker_ids = self.get_mentions(message, speaker_ids)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_mention(message)
    
    def _prepare_speak(self,speaker_id,group_id:str=None,new_group=False,type='Text',msg=''):
        speaker = self.get_speaker(speaker_id)

        def add_content(obj:Model4LLM.ContentGroup,type=type):
            if 'Text' in type:
                return obj.get_controller().add_new_text_content
            elif 'Image' in type:
                return obj.get_controller().add_new_image_content
            else:
                raise ValueError(f'Unknown type of {type}')

        if (group_id is None and not new_group) or (group_id == self.chatroom_id):
            tc = add_content(self.chatroom())

        elif group_id is not None and not new_group:
            if group_id not in self.chatroom().children_id:
                raise ValueError(f'no such group {group_id}')
            group:Model4LLM.ContentGroup = self.store.find(group_id)
            tc = add_content(group)
            self._on_message_change()

        elif group_id is None and new_group:            
            group = self.chatroom().get_controller().add_new_child_group()
            tc = add_content(group)
        
        return tc(speaker.id, msg)

    def speak_stream(self,speaker_id,stream,callback,group_id:str=None,new_group=False):
        content:Model4LLM.AbstractContent = None
        msg = ''
        for i,r in enumerate(stream):
            assert r is not None, f'can not prepare string reply in speak_stream! {r}'
            if i==0:
                content = self._prepare_speak(speaker_id,group_id,new_group)
            content.get_controller().append_data_raw(r)
            msg += r
        callback()
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())

    def speak(self,speaker_id,msg:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group,msg=msg)
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())
        return content
    
    def speak_img(self,speaker_id,imagpath:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group,type='Image',msg=imagpath)
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())
        return content
    #######################################################################

    def msgsDict(self,refresh=False,msgs=None,todict=None):        
        if todict is None:
            def todict(v:Model4LLM.AbstractContent):
                c = v.get_controller()
                n = v.__class__.__name__
                if 'Text' in n:
                    return {"type": "text","text": c.get_data_raw()}
                if 'Image' in n:
                    return {"type": "image_url","image_url": {
                                "url": f"data:image/jpeg;base64,{c.get_data_raw()}"}}                
            # todict = lambda c:c.load().get_data_raw()
            
        if refresh:
            self.msgs:List[Model4LLM.AbstractContent] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs

        res = []
        for v in msgs:
            if 'ContentGroup' not in v.__class__.__name__:
                mc = v.get_controller()
                name = mc.get_author().name
                role = mc.get_author().role
                if 'EmbeddingContent' in v.__class__.__name__:
                    v:Model4LLM.EmbeddingContent = v
                    t = v.get_controller().get_target(
                        ).get_controller().get_data_raw()[:10]
                    # print(f'{intents}{self.speakers[m.model.author_id].name}: "{t}"=>{m.load().get_data_raw()[:5]}...')
                elif 'TextContent' in v.__class__.__name__:
                    res.append(dict(name=name,role=role,content=todict(v)))
                else:
                    res.append(dict(name=name,role=role,content=todict(v)))
            else:
                res.append(self.msgsDict(False,self.get_messages_in_group(v.get_id())))
        return res


    def printMsgs(self,refresh=False,intent=0,msgs:List[Model4LLM.AbstractContent]=None):
        if refresh:
            self.msgs:List[Model4LLM.AbstractContent] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs
        intents = "".join([' ']*intent)
        print("", flush=True)
        print(f'{intents}#############################################################')
        
        for i,v in enumerate(msgs):
            print(f'{intents}-------------------------------------------------------------')
            if 'ContentGroup' not in v.__class__.__name__:
                v:Model4LLM.AbstractContent = v
                if 'EmbeddingContent' in v.__class__.__name__:
                    v:Model4LLM.EmbeddingContent = v
                    econtroller = v.get_controller()
                    t = econtroller.get_target().get_controller().get_data_raw()[:10]
                    print(f'{intents}{self.speakers[econtroller.get_target().author_id].name}: "{t}"=>{econtroller.get_data_raw()[:5]}...')
                elif 'ImageContent' in v.__class__.__name__:
                    v:Model4LLM.ImageContent = v
                    im = v.get_controller().get_image()
                    print(f'{intents}{self.speakers[v.author_id].name}: Image{im.size} of {im.info}')
                else:
                    print(f'{intents}{self.speakers[v.author_id].name}: {v.get_controller().get_data_raw()}')
            else:
                self.printMsgs(False,intent+4,self.get_messages_in_group(v.get_id()))
        print(f'{intents}-------------------------------------------------------------')
        print(f'{intents}#############################################################')