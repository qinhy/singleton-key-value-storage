// from https://github.com/qinhy/singleton-key-value-storage.git
import { SingletonKeyValueStorage, uuidv4 } from "./Storage.js"

function now_utc() {
  // Get the current time in milliseconds since the Unix epoch
  const now = new Date()
  // Get the current time with higher precision (including microseconds)
  const milliseconds = now.getMilliseconds() // Milliseconds part (0-999)
  const microseconds = Math.floor((performance.now() % 1) * 1000) // Simulate microseconds
  // Format the date to an ISO string
  let isoString = now.toISOString() // e.g., "2024-09-18T16:49:21.552Z"
  // Replace the milliseconds part with microsecond precision
  return isoString.replace(
    /\.\d{3}Z/,
    `.${String(milliseconds).padStart(3, "0")}${String(microseconds).padStart(
      3,
      "0"
    )}Z`
  )
}

export let Controller4Basic

;(function(_Controller4Basic) {
  class AbstractObjController {
    constructor(store, model) {
      this.model = model
      this._store = store
    }

    storage() {
      return this._store
    }

    update(kwargs) {
      if (!this.model) throw new Error("Controller has null model!")

      for (const key in kwargs) {
        if (kwargs.hasOwnProperty(key) && this.model.hasOwnProperty(key)) {
          this.model[key] = kwargs[key]
        }
      }
      this._update_timestamp()
      this.store()
      return this
    }

    _update_timestamp() {
      if (!this.model) throw new Error("Controller has null model!")
      this.model.update_time = now_utc()
    }

    store() {
      if (!this.model?._id) throw new Error("Model ID is not set!")
      this.storage().set(this.model._id, this.model.model_dump_json_dict())
      return this
    }

    delete() {
      if (!this.model) throw new Error("Controller has null model!")
      this.storage().delete(this.model.get_id())
      this.model._controller = null
    }

    update_metadata(key, value) {
      if (!this.model) throw new Error("Controller has null model!")
      const updated_metadata = {
        ...this.model.metadata,
        [key]: value
      }
      this.update({
        metadata: updated_metadata
      })
      return this
    }
  }

  _Controller4Basic.AbstractObjController = AbstractObjController

  class AbstractGroupController extends Controller4Basic.AbstractObjController {
    constructor(store, model) {
      super(store, model)
    }

    *yield_children_recursive(depth = 0) {
      const model = this.model
      if (!model) throw new Error("Controller has null model!")
      for (const child_id of model.children_id || []) {
        if (!this.storage().exists(child_id)) continue

        const child = this.storage().find(child_id)
        if (
          child &&
          child.hasOwnProperty("parent_id") &&
          child.hasOwnProperty("children_id")
        ) {
          const group = child.get_controller()
          yield* group.yield_children_recursive(depth + 1)
        }
        yield {
          child,
          depth
        }
      }
    }

    delete_recursive() {
      if (!this.model) throw new Error("Controller has null model!")
      for (const { child } of this.yield_children_recursive()) {
        child.get_controller().delete()
      }
      this.delete()
    }

    get_children_recursive() {
      const model = this.model
      if (!model) throw new Error("Controller has null model!")
      const children_list = []
      for (const child_id of model.children_id || []) {
        if (!this.storage().exists(child_id)) continue

        const child = this.storage().find(child_id)
        if (
          child &&
          child.hasOwnProperty("parent_id") &&
          child.hasOwnProperty("children_id")
        ) {
          const group = child.get_controller()
          children_list.push(group.get_children_recursive())
        } else {
          children_list.push(child)
        }
      }
      return children_list
    }

    get_children() {
      const model = this.model
      if (!model) throw new Error("Controller has a null model!")
      return (model.children_id || []).map(child_id =>
        this.storage().find(child_id)
      )
    }

    get_child(child_id) {
      return this.storage().find(child_id)
    }

    add_child(child_id) {
      const model = this.model
      if (!model) throw new Error("Controller has null model!")
      return this.update({
        children_id: [...(model.children_id || []), child_id]
      })
    }

    delete_child(child_id) {
      const model = this.model
      if (!model || !model.children_id?.includes(child_id)) return this

      const remaining_ids = (model.children_id || []).filter(
        cid => cid !== child_id
      )
      const child = this.storage().find(child_id)
      if (!child) return this

      const child_con = child.get_controller()

      if (child_con.delete_recursive) {
        child_con.delete_recursive()
      } else {
        child_con.delete()
      }

      this.update({
        children_id: remaining_ids
      })
      return this
    }
  }

  _Controller4Basic.AbstractGroupController = AbstractGroupController
})(Controller4Basic || (Controller4Basic = {}))

export let Model4Basic

;(function(_Model4Basic) {
  class AbstractObj {
    constructor(data) {
      this._id = data?._id || null
      this.rank = data?.rank || [0]
      this.create_time = data?.create_time || now_utc()
      this.update_time = data?.update_time || now_utc()
      this.status = data?.status || ""
      this.metadata = data?.metadata || {}
      this._controller = null
      this.auto_del = data?.auto_del || false
    }

    model_dump_json_dict() {
      const publicData = {}
      for (const key in this) {
        if (
          Object.prototype.hasOwnProperty.call(this, key) &&
          !key.startsWith("_")
        ) {
          publicData[key] = this[key]
        }
      }
      return publicData
    }

    class_name() {
      return this.constructor.name
    }

    set_id(id) {
      if (this._id) throw new Error("This object ID is already set!")
      this._id = id
      return this
    }

    gen_new_id() {
      return `${this.class_name()}:${uuidv4()}`
    }

    get_id() {
      if (!this._id) throw new Error("This object ID is not set!")
      return this._id
    }

    get_controller() {
      return this._controller
    }

    init_controller(store) {
      const controller_class = this._get_controller_class(Controller4Basic)
      this._controller = new controller_class(store, this)
    }

    _get_controller_class(model_class) {
      const class_type = `${this.constructor.name}Controller`
      const res = Object.values(model_class).find(c => c.name === class_type)
      if (!res) throw new Error(`No such class of ${class_type}`)
      return res
    }
  }

  _Model4Basic.AbstractObj = AbstractObj

  class AbstractGroup extends Model4Basic.AbstractObj {
    constructor(data) {
      super(data)
      this.author_id = data?.author_id || ""
      this.parent_id = data?.parent_id || ""
      this.children_id = data?.children_id || []
    }
  }

  _Model4Basic.AbstractGroup = AbstractGroup
})(Model4Basic || (Model4Basic = {}))

export class BasicStore extends SingletonKeyValueStorage {
  MODEL_CLASS_GROUP = Model4Basic

  constructor(version_control = false) {
    super(version_control)
    this.tempTsBackend()
  }

  _get_class(id) {
    const class_type = id.split(":")[0]
    const classes = {
      AbstractObj: Model4Basic.AbstractObj,
      AbstractGroup: Model4Basic.AbstractGroup
    }
    const res = classes[class_type]
    if (!res) throw new Error(`No such class of ${class_type}`)
    return res
  }

  _get_as_obj(id, data_dict) {
    const ClassConstructor = this._get_class(id)
    const obj = new ClassConstructor(data_dict)
    obj.set_id(id).init_controller(this)
    return obj
  }

  _auto_fix_id(obj, id = "None") {
    const class_type = id.split(":")[0]
    const obj_class_type = obj.class_name()
    if (class_type !== obj_class_type) {
      id = `${obj_class_type}:${id}`
    }
    return id
  }

  _add_new_obj(obj, id = null) {
    id = id === null ? obj.gen_new_id() : id
    id = this._auto_fix_id(obj, id)
    const data = obj.model_dump_json_dict()
    this.set(id, data)
    return this._get_as_obj(id, data)
  }

  add_new(obj_class_type = Model4Basic.AbstractObj, id = null) {
    return (...args) => {
      const obj = new obj_class_type(...args)
      if (obj._id !== null)
        throw new Error(`obj._id is "${obj._id}", must be none`)
      return this._add_new_obj(obj, id)
    }
  }

  add_new_obj(obj, id = null) {
    if (obj._id !== null) throw new Error(`obj._id is ${obj._id}, must be none`)
    return this._add_new_obj(obj, id)
  }

  add_new_group(obj, id = null) {
    if (obj._id !== null) throw new Error(`obj._id is ${obj._id}, must be none`)
    return this._add_new_obj(obj, id)
  }

  find(id) {
    const raw = this.get(id)
    if (raw === null) {
      const raws = this.find_all(`*:${id}`)
      if (raws.length === 1) {
        return raws[0]
      }
      return null
    }
    return this._get_as_obj(id, raw)
  }

  find_all(id = "AbstractObj:*") {
    return this.keys(id)
      .map(key => this.find(key))
      .filter(obj => obj !== null)
  }
}
