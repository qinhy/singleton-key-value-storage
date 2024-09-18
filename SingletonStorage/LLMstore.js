
class Controller4LLM {
    static AbstractObjController = class extends Controller4Basic.AbstractObjController {
        // Inherits functionality from Controller4Basic.AbstractObjController
    };

    static CommonDataController = class extends Controller4LLM.AbstractObjController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.CommonData
            this._store = store;  // LLMstore
        }
    };

    static AuthorController = class extends Controller4LLM.AbstractObjController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.Author
            this._store = store;  // LLMstore
        }
    };

    static AbstractContentController = class extends Controller4LLM.AbstractObjController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.AbstractContent
            this._store = store;  // LLMstore
        }

        delete() {
            this.get_data().get_controller().delete();
            this._store.delete(this.model.get_id());
            this.model._controller = null;
        }

        get_author() {
            return this._store.find(this.model.author_id);  // Model4LLM.Author
        }

        get_group() {
            return this._store.find(this.model.group_id);  // Model4LLM.ContentGroup
        }

        get_data() {
            return this._store.find(this.model.data_id());  // Model4LLM.CommonData
        }

        get_data_raw() {
            return this.get_data().raw;
        }

        update_data_raw(msg) {
            this.get_data().get_controller().update({ raw: msg });
            return this;
        }

        append_data_raw(msg) {
            const data = this.get_data();
            data.get_controller().update({ raw: data.raw + msg });
            return this;
        }
    };

    static AbstractGroupController = class extends Controller4LLM.AbstractObjController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.AbstractGroup
            this._store = store;  // LLMstore
        }

        *yield_children_content_recursive(depth = 0) {
            for (const child_id of this.model.children_id) {
                if (!this._store.exists(child_id)) continue;
                const content = this._store.find(child_id);  // Model4LLM.AbstractObj
                yield [content, depth];
                if (child_id.startsWith('ContentGroup')) {
                    const group = content.get_controller();  // Controller4LLM.AbstractGroupController
                    for (const [cc, d] of group.yield_children_content_recursive(depth + 1)) {
                        yield [cc, d];
                    }
                }
            }
        }

        delete_recursive_from_key_value_storage() {
            for (const [content] of this.yield_children_content_recursive()) {
                content.get_controller().delete();
            }
            this.delete();
        }

        get_children_content() {
            return this.model.children_id.map(child_id => this._store.find(child_id));  // Array of Model4LLM.AbstractObj
        }

        get_child_content(child_id) {
            return this._store.find(child_id);  // Model4LLM.AbstractContent
        }

        prints() {
            let result = '########################################################\n';
            for (const [content, depth] of this.yield_children_content_recursive()) {
                result += `${'    '.repeat(depth)}${content.get_id()}\n`;
            }
            result += '########################################################\n';
            console.log(result);
            return result;
        }
    };

    static ContentGroupController = class extends Controller4LLM.AbstractGroupController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.ContentGroup
            this._store = store;  // LLMstore
        }

        add_new_child_group(metadata = {}, rank = [0]) {
            const [, child] = this._store.add_new_group_to_group(this.model, metadata, rank);
            return child;
        }

        add_new_text_content(author_id, text) {
            const [, child] = this._store.add_new_text_to_group(this.model, author_id, text);
            return child;
        }

        add_new_embedding_content(author_id, content_id, vec) {
            const [, child] = this._store.add_new_embedding_to_group(this.model, author_id, content_id, vec);
            return child;
        }

        add_new_image_content(author_id, filepath) {
            const [, child] = this._store.add_new_image_to_group(this.model, author_id, filepath);
            return child;
        }

        remove_child(child_id) {
            this.model.children_id = this.model.children_id.filter(id => id !== child_id);
            for (const content of this.get_children_content()) {
                if (content.get_controller().model.get_id() === child_id) {
                    if (child_id.startsWith('ContentGroup')) {
                        content.get_controller().delete_recursive_from_key_value_storage();
                    }
                    content.get_controller().delete();
                    break;
                }
            }
            this.update({ children_id: this.model.children_id });
            return this;
        }

        get_children_content_recursive() {
            const results = [];
            for (const [content] of this.yield_children_content_recursive()) {
                results.push(content);  // Array of Model4LLM.AbstractContent
            }
            return results;
        }
    };

    static TextContentController = class extends Controller4LLM.AbstractContentController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.TextContent
            this._store = store;  // LLMstore
        }
    };

    static EmbeddingContentController = class extends Controller4LLM.AbstractContentController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.EmbeddingContent
            this._store = store;  // LLMstore
        }

        get_data_raw() {
            return super.get_data_raw().slice(1, -1).split(',').map(Number);
        }

        get_data_rLOD0() {
            return this.get_data_raw().filter((_, index) => index % 10 === 0);
        }

        get_data_rLOD1() {
            return this.get_data_raw().filter((_, index) => index % 100 === 0);
        }

        get_data_rLOD2() {
            return this.get_data_raw().filter((_, index) => index % 1000 === 0);
        }

        get_target() {
            return this._store.find(this.model.target_id);  // Model4LLM.AbstractContent
        }

        update_data_raw(embedding) {
            super.update_data_raw(JSON.stringify(embedding));
            return this;
        }
    };

    static FileLinkContentController = class extends Controller4LLM.AbstractContentController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.FileLinkContent
            this._store = store;  // LLMstore
        }
    };

    static BinaryFileContentController = class extends Controller4LLM.AbstractContentController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.BinaryFileContent
            this._store = store;  // LLMstore
        }

        read_bytes(filepath) {
            const fs = require('fs');
            return fs.readFileSync(filepath);
        }

        b64decode(file_base64) {
            return Buffer.from(file_base64, 'base64');
        }

        get_data_rLOD0() {
            throw new Error('Binary file has no LOD concept');
        }

        get_data_rLOD1() {
            throw new Error('Binary file has no LOD concept');
        }

        get_data_rLOD2() {
            throw new Error('Binary file has no LOD concept');
        }
    };

    static ImageContentController = class extends Controller4LLM.BinaryFileContentController {
        constructor(store, model) {
            super(store, model);
            this.model = model;  // Model4LLM.ImageContent
            this._store = store;  // LLMstore
        }

        decode_image(encoded_string) {
            const sharp = require('sharp');
            return sharp(Buffer.from(encoded_string, 'base64'));
        }

        get_image() {
            const encoded_image = this.get_data_raw();
            return encoded_image ? this.decode_image(encoded_image) : null;
        }

        get_image_format() {
            const image = this.get_image();
            return image ? image.metadata().format : null;
        }

        get_data_rLOD(lod = 0) {
            const image = this.get_image();
            const ratio = 10 ** (lod + 1);
            if (image.width / ratio <= 0 || image.height / ratio <= 0) {
                throw new Error(`Image size (${image.width}, ${image.height}) of LOD${lod} is too small`);
            }
            return image.resize(Math.floor(image.width / ratio), Math.floor(image.height / ratio));
        }

        get_data_rLOD0() {
            return this.get_data_rLOD(0);
        }

        get_data_rLOD1() {
            return this.get_data_rLOD(1);
        }

        get_data_rLOD2() {
            return this.get_data_rLOD(2);
        }
    };
}
