
from SingletonStorage.LLMroom import ChatRoom
from SingletonStorage.LLMstore import AbstractContent, AbstractContentController, ContentGroupController, EmbeddingContentController, ImageContent, ImageContentController, TextContent

import threading
from typing import List
class Configs:
    class Config:
        def __init__(self, name, value) -> None:
            self.name = name
            self.value = value
            self.lock = threading.Lock()

        def update(self, value):
            with self.lock:
                self.value = value
                return value
        
        def get(self):
            with self.lock:
                return self.value
        
    def __init__(self) -> None:
        self._configs = {}
        self.configs_lock = threading.Lock()

    def tolist(self):
        with self.configs_lock:
            return list(self._configs.values())
    
    def new_config(self, name, value):
        with self.configs_lock:
            if name in self._configs:
                raise Exception("Config with this name already exists.")
            tmp = Configs.Config(name, value)
            self._configs[name] = tmp
            return tmp

    def get(self, name):
        with self.configs_lock:
            return self._configs[name]
    
    def delete(self, name):
        with self.configs_lock:
            if name in self._configs:
                del self._configs[name]
            else:
                raise KeyError(f"No configuration with the name '{name}' exists.")

try:
    import gradio as gr     
    def build_gui(cr:ChatRoom,configs:List[Configs.Config]=[],json=None):
        def roomdelmsg(msgid,cr:ChatRoom=cr):
            try:
                if str(msgid).isdecimal() and int(msgid)>0:
                    for m in cr.chatroom().get_controller().get_children_content()[-int(msgid):]:
                        cr.chatroom().get_controller().remove_child(m.id)
                    cr.store.dump(json)
                    return True,chat(None,'')
                
                for msgid in msgid.split('\n'):
                    cr.chatroom().get_controller().remove_child(msgid)
                cr.store.dump(json)
                
                return True,chat(None,'')
            except Exception as e:
                return f'{e}',chat(None,'')
        
        def clonemsg(msgid,cr:ChatRoom=cr):
            try:
                for msgid in msgid.split('\n'):
                    gc:ContentGroupController = cr.chatroom().get_controller()
                    c = gc.get_child_content(msgid)
                    if type(c) is TextContent:
                        return chat(c.get_controller().get_author().id,c.get_controller().get_data_raw())
            except Exception as e:
                return f'{e}'
            
        def chat(author_id:str=None, message:str=None,filepath:str=None, cr:ChatRoom=cr,configs:List[Configs.Config]=configs):
            try:                
                if message is None:
                    return f'# Welcome to {json}, show messages by "Reflesh"'
                if len(message)>0 and author_id in cr.speakers and filepath:
                    s = cr.speakers[author_id]
                    gid = s.new_group()
                    s.speak_img(filepath,gid)
                    s.speak(message,gid)
                    
                elif len(message)>0 and author_id in cr.speakers:
                    cr.speakers[author_id].speak(message)

                show_passages = [c for c in configs if c.name=='show_passages']
                show_passages = int(show_passages[0].value) if len(show_passages)>0 else 10
                
                def get_msgs_md(msgs:List[AbstractContent],cr:ChatRoom=cr,depth=0):
                    res = []
                    for i,v in enumerate(msgs):
                        if 'ContentGroup' not in v.__class__.__name__:
                            if 'EmbeddingContent' in v.__class__.__name__:
                                v:ImageContent = v
                                econtroller:EmbeddingContentController = v.get_controller()
                                t = econtroller.get_target().get_controller().get_data_raw()[:10]

                                name = cr.speakers[econtroller.get_target().author_id].name
                                msg = f'"{t}"=>{econtroller.get_data_raw()[:5]}...'

                            elif 'ImageContent' in v.__class__.__name__:
                                v:ImageContent = v
                                im = v.get_controller().get_image()
                                imid = v.id.split(':')[1]
                                b64 = v.get_controller().get_data_raw()
                                im = v.get_controller().get_image()
                                
                                name = cr.speakers[v.author_id].name
                                msg = f'![{imid}](data:image/{im.format};base64,{b64})'
                            else:                                
                                name = cr.speakers[v.author_id].name
                                msg = v.get_controller().get_data_raw().replace("\n","\n>\n>")

                            m = f'\n\n##### {name}:\n>{msg}\n>\n>_{v.create_time} {v.id}_'
                            if depth>0:
                                lvl = ''.join(['>']*(depth+1))
                                m = m.replace('>',lvl)
                                lvl = ''.join(['>']*(depth))
                                m = m.replace('#####',f'{lvl}#####')
                            res.append(m)
                        else:
                            res.append("\n".join(get_msgs_md(cr.get_messages_in_group(v.id),depth=depth+1)))
                    return res
                return "\n".join(get_msgs_md(cr.get_messages_in_group()[-show_passages:]))
            except Exception as e:
                return f'{e}'

        def read_json(file):
            cr.store.load(file)
            return cr.chatroom().get_controller().prints()
        
        with gr.Blocks() as demo:
            with gr.Tab("Chat"):
                with gr.Column():

                    history = gr.Markdown(label="History",value=chat(None,None))
                    author = gr.Dropdown(value='User',choices=[n for n in cr.speakers.keys() if ':' not in n], label="Author")
                    
                    with gr.Tab("Home"):
                        message = gr.Textbox(label="Message")
                        send = gr.Button("Send")
                        reflesh = gr.Button("Reflesh")
                    with gr.Tab("Image"):
                        image_file = gr.Image(type='filepath')
                    with gr.Tab("Room"):                    
                        msgid = gr.Textbox(label="Message Id")
                        delmsg =gr.Button("Delete")
                        clmsg =gr.Button("Clone")
                        

                    clmsg.click(fn=clonemsg, inputs=[msgid], outputs=history)
                    reflesh.click(fn=lambda :chat(None,''), inputs=[], outputs=history)
                    send.click(fn=chat, inputs=[author,message,image_file], outputs=history)
                    delmsg.click(fn=roomdelmsg, inputs=[msgid], outputs=[msgid,history])

                    # image_button_filepath.click(save_image_filepath, inputs=image_input_filepath)

            with gr.Tab("Config"):
                with gr.Column():
                    for i,c in enumerate(configs):                    
                        with gr.Row():
                            config_value = gr.Textbox(label=c.name, value=c.value)
                            getv = gr.Button("Get")
                            update = gr.Button("Update")
                            update.click(fn=c.update, inputs=[config_value], outputs=[config_value])
                            getv.click(fn=c.get, inputs=[], outputs=[config_value])

                    gr.Interface(read_json, "file", "text")

                # demo.launch()
                # demo.launch(auth=("admin", "admin"))
        return demo
except Exception as e:
    print(e)

