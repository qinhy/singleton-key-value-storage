import threading
from typing import List
from SingletonStorage.LLMroom import ChatRoom
from SingletonStorage.LLMstore import AbstractContent, AbstractContentController, ContentGroupController, EmbeddingContentController, ImageContentController

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
                for msgid in msgid.split('\n'):
                    gc:ContentGroupController = cr.chatroom().controller
                    gc.remove_child(msgid)
                cr.store.dump(json)
                return True
            except Exception as e:
                return f'{e}'
        
        def chat(author=None, message:str=None,filepath:str=None, cr:ChatRoom=cr,configs:List[Configs.Config]=configs):
            try:                
                if len(message)>0 and author in cr.speakers and filepath:
                    s = cr.speakers[author]
                    gid = s.new_group()
                    s.speak_img(filepath,gid)
                    s.speak(message,gid)
                    
                elif len(message)>0 and author in cr.speakers:
                    cr.speakers[author].speak(message)

                show_passages = [c for c in configs if c.name=='show_passages']
                show_passages = int(show_passages[0].value) if len(show_passages)>0 else 10
                
                def get_msgs_md(msgs:List[AbstractContent],cr:ChatRoom=cr):
                    res = []
                    for i,v in enumerate(msgs):
                        if 'ContentGroup' not in v.__class__.__name__:
                            if 'EmbeddingContent' in v.__class__.__name__:
                                econtroller:EmbeddingContentController = v.controller
                                tcontroller:AbstractContentController = econtroller.get_target().controller
                                t = tcontroller.get_data_raw()[:10]
                                m = f'## {cr.speakers[econtroller.get_target().author_id].name}: "{t}"=>{econtroller.get_data_raw()[:5]}...\n\n_[{v.create_time}][{v.id}]_'                                
                            elif 'ImageContent' in v.__class__.__name__:
                                vcontroller:ImageContentController = v.controller
                                im = vcontroller.get_image()
                                imid = v.id.split(':')[1]
                                b64 = vcontroller.get_data_raw()
                                im = vcontroller.get_image()
                                m = f'## {cr.speakers[v.author_id].name}:\n\n_[{v.create_time}][{v.id}]_'
                                m += f'![{imid}](data:image/{im.format};base64,{b64})'
                            else:
                                vcontroller:AbstractContentController = v.controller
                                m = f'## {cr.speakers[v.author_id].name}:\n{vcontroller.get_data_raw()}\n\n_[{v.create_time}][{v.id}]_'
                            res.append(m)
                        else:
                            res.append("\n\n---\n\n".join(get_msgs_md(cr.get_messages_in_group(v.id))))
                    return res
                return "\n\n---\n\n".join(get_msgs_md(cr.get_messages_in_group()[-show_passages:]))
            except Exception as e:
                return f'{e}'

        with gr.Blocks() as demo:
            with gr.Tab("Chat"):
                with gr.Column():
                    with gr.Row():
                        msgid = gr.Textbox(label="Message Id")
                        delmsg =gr.Button("Delete")
                        reflesh = gr.Button("Reflesh")
                        delmsg.click(fn=roomdelmsg, inputs=[msgid], outputs=[msgid])

                    # history = gr.Textbox(label="History",value=chat(None,''))                
                    history = gr.Markdown(label="History",value=chat(None,''))
                    author = gr.Dropdown(value='User',choices=[n for n in cr.speakers.keys() if ':' not in n], label="Author")
                    
                    with gr.Tab("Text"):
                        message = gr.Textbox(label="Message")
                    with gr.Tab("Image"):
                        image_file = gr.Image(type='filepath')
                    send = gr.Button("Send")                    
                    

                    reflesh.click(fn=lambda :chat(None,''), inputs=[], outputs=history)
                    send.click(fn=chat, inputs=[author,message,image_file], outputs=history)

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

                # demo.launch()
                # demo.launch(auth=("admin", "admin"))
        return demo
except Exception as e:
    print(e)

