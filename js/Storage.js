// from https://github.com/qinhy/singleton-key-value-storage.git

import { PEMFileReader, SimpleRSAChunkEncryptor } from "./RSA.js"
import { Buffer } from "buffer"

export function uuidv4() {
  const b = new Uint8Array(16)
  crypto.getRandomValues(b) // 128 random bits

  b[6] = (b[6] & 0x0f) | 0x40 // version = 4
  b[8] = (b[8] & 0x3f) | 0x80 // variant = RFC 4122

  const hex = [...b].map(x => x.toString(16).padStart(2, "0")).join("")
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(
    12,
    16
  )}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
/**
 * Roughly estimate the deep size of a JS value, handling:
 * - primitives
 * - Array / Set / Map
 * - plain objects (own props, incl. Symbols)
 * - ArrayBuffer / TypedArray / Buffer
 * - Date / RegExp
 * Cycles are handled via a WeakSet.
 */
export function getDeepSize(obj, seen = null, shallow = false) {
  const _seen = seen ?? new WeakSet()

  const sizeOfPrimitive = value => {
    switch (typeof value) {
      case "string":
        return value.length * 2 // UTF-16, ~2 bytes/char
      case "number":
        return 8 // 64-bit float
      case "boolean":
        return 4
      case "bigint":
        // very rough: cost ~ length of decimal representation
        return value.toString().length * 2
      case "symbol":
      case "function":
      case "undefined":
        return 0
      default:
        return 0
    }
  }

  const sizeOfObject = value => {
    if (_seen.has(value)) return 0
    _seen.add(value)

    // Node Buffer
    if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
      return value.length
    }
    // ArrayBuffer / DataView / TypedArray
    if (value instanceof ArrayBuffer) return value.byteLength
    if (ArrayBuffer.isView(value)) return value.byteLength

    // Date ~ 8 bytes
    if (value instanceof Date) return 8

    // RegExp ~ source length
    if (value instanceof RegExp) return value.source.length * 2

    // Map: keys + values
    if (value instanceof Map) {
      if (shallow) return value.size * 8 // rough pointer-only estimate
      let s = 0
      for (const [k, v] of value.entries()) {
        s += getDeepSize(k, _seen, false)
        s += getDeepSize(v, _seen, false)
      }
      return s
    }

    // Set: items
    if (value instanceof Set) {
      if (shallow) return value.size * 8
      let s = 0
      for (const v of value.values()) {
        s += getDeepSize(v, _seen, false)
      }
      return s
    }

    // Array
    if (Array.isArray(value)) {
      if (shallow) return value.length * 8
      let s = 0
      for (const item of value) {
        s += getDeepSize(item, _seen, false)
      }
      return s
    }

    // Plain object and class instances: own props (including Symbols)
    const propNames = Object.getOwnPropertyNames(value)
    const propSymbols = Object.getOwnPropertySymbols(value)
    let s = 0

    // If shallow, only account for immediate (rough) pointer cost / primitive bytes
    const visit = (desc, val) => {
      if (!desc) return
      // Accessor getters might throw—guard.
      try {
        const cell = desc.get ? desc.get.call(value) : val
        if (shallow) {
          s +=
            typeof cell === "object" && cell !== null
              ? 8
              : sizeOfPrimitive(cell)
        } else {
          s += getDeepSize(cell, _seen, false)
        }
      } catch {
        /* ignore unreadable getter */
      }
    }

    for (const k of propNames) {
      const desc = Object.getOwnPropertyDescriptor(value, k)
      // Pass the current value if it's a data descriptor
      visit(desc, value[k])
    }
    for (const sym of propSymbols) {
      const desc = Object.getOwnPropertyDescriptor(value, sym)
      visit(desc, value[sym])
    }
    return s
  }

  // primitives/null
  if (obj === null) return 0
  const t = typeof obj
  if (t !== "object") return sizeOfPrimitive(obj)

  return sizeOfObject(obj)
}

/** Humanize byte counts like Python's humanize_bytes. */
export function humanizeBytes(n) {
  let size = Number(n)
  const units = ["B", "KB", "MB", "GB", "TB"]
  for (const u of units) {
    if (size < 1024) return `${size.toFixed(1)} ${u}`
    size /= 1024
  }
  return `${size.toFixed(1)} PB`
}

export class AbstractStorage {
  static _uuid = uuidv4()
  static _store = null
  static _is_singleton = true
  static _meta = {}

  constructor(id = null, store = null, isSingleton = null) {
    this.uuid = id ?? uuidv4()
    this.store = store ?? null
    this.isSingleton = isSingleton ?? false
  }

  getSingleton() {
    return new AbstractStorage(
      AbstractStorage._uuid,
      AbstractStorage._store,
      AbstractStorage._is_singleton
    )
  }

  /** Subclasses must implement memory usage (deep or shallow). */
  memoryUsage(deep, humanReadable) {
    throw new Error("Subclasses must implement memoryUsage method")
  }
}

export class TsDictStorage extends AbstractStorage {
  static _uuid = uuidv4()
  static _store = {}
  _filePath = null

  constructor(id = null, store = null, isSingleton = null) {
    super(id, store, isSingleton)
    this.store = store ?? {}

    // Handle file path if provided as first argument
    if (id && typeof id === "string" && !store && !isSingleton) {
      this._filePath = id
      this.load()
    }
  }

  memoryUsage(deep = true, humanReadable = true) {
    // deep -> full traversal; shallow -> top-level estimate only
    const size = getDeepSize(this, null, !deep)
    return humanReadable ? humanizeBytes(size) : size
  }

  dump() {
    if (!this._filePath) return false
    try {
      const fs = require("fs")
      fs.writeFileSync(this._filePath, JSON.stringify(this.store), "utf8")
      return true
    } catch (error) {
      console.error(`Error writing to file ${this._filePath}:`, error)
      return false
    }
  }

  load() {
    if (!this._filePath) return false
    try {
      const fs = require("fs")
      if (fs.existsSync(this._filePath)) {
        const data = fs.readFileSync(this._filePath, "utf8")
        this.store = JSON.parse(data)
        return true
      }
      return false
    } catch (error) {
      console.error(`Error reading from file ${this._filePath}:`, error)
      return false
    }
  }
}

class AbstractStorageController {
  constructor(model) {
    this.model = model
  }

  isSingleton() {
    return this.model.isSingleton
  }

  exists(key) {
    console.error(`[${this.constructor.name}]: not implemented`)
    return false
  }

  set(key, value) {
    console.error(`[${this.constructor.name}]: not implemented`)
  }

  get(key) {
    console.error(`[${this.constructor.name}]: not implemented`)
    return null
  }

  delete(key) {
    console.error(`[${this.constructor.name}]: not implemented`)
  }

  keys(pattern = "*") {
    console.error(`[${this.constructor.name}]: not implemented`)
    return []
  }

  clean() {
    this.keys("*").forEach(key => this.delete(key))
  }

  dumps() {
    const data = {}
    this.keys("*").forEach(key => {
      data[key] = this.get(key)
    })
    return JSON.stringify(data)
  }

  loads(jsonString = "{}") {
    const data = JSON.parse(jsonString)
    Object.entries(data).forEach(([key, value]) => {
      const val = value ? value : {}
      this.set(key, val)
    })
  }

  // dump(path: string): string {
  //     const data = this.dumps();
  //     fs.writeFileSync(path, data);
  //     return data;
  // }

  // load(path: string): void {
  //     const data = fs.readFileSync(path, 'utf8');
  //     this.loads(data);
  // }

  // Placeholder methods for RSA encryption and decryption
  dumpRSAs(publicKeyPath, compress = false) {
    const publicKey = new PEMFileReader(publicKeyPath).loadPublicPkcs8Key()
    const encryptor = new SimpleRSAChunkEncryptor(publicKey, null)
    return encryptor.encryptString(this.dumps(), compress)
  }

  loadRSAs(content, privateKeyPath) {
    const privateKey = new PEMFileReader(privateKeyPath).loadPrivatePkcs8Key()
    const encryptor = new SimpleRSAChunkEncryptor(null, privateKey)
    const decryptedText = encryptor.decryptString(content)
    this.loads(decryptedText)
  }
}

class TsDictStorageController extends AbstractStorageController {
  constructor(model) {
    super(model)
    this.store = model.store
  }

  exists(key) {
    return key in this.store
  }

  set(key, value) {
    this.store[key] = value
  }

  get(key) {
    return this.store[key] || null
  }

  delete(key) {
    delete this.store[key]
  }

  keys(pattern = "*") {
    const regex = new RegExp("^" + pattern.replace(/\*/g, ".*"))
    return Object.keys(this.model.store).filter(key => key.match(regex))
  }
}

class EventDispatcherController extends TsDictStorageController {
  static ROOT_KEY = "Event"

  _findEvent(uuid) {
    const keys = this.keys(`*:${uuid}`)
    return keys.length === 0 ? [] : keys
  }

  set(key, value) {
    this.store[key] = value
  }

  get(key) {
    return this.store[key] || null
  }

  events() {
    return this.keys("*").map(key => [key, this.get(key)])
  }

  getEvent(uuid) {
    return this._findEvent(uuid).map(key => (key ? this.get(key) : null))
  }

  deleteEvent(uuid) {
    this._findEvent(uuid).forEach(key => {
      if (key) this.delete(key)
    })
  }

  setEvent(eventName, callback, id) {
    if (!id) id = uuidv4()
    this.set(
      `${EventDispatcherController.ROOT_KEY}:${eventName}:${id}`,
      callback
    )
    return id
  }

  dispatchEvent(eventName, ...args) {
    this.keys(`${EventDispatcherController.ROOT_KEY}:${eventName}:*`).forEach(
      key => {
        const callback = this.get(key)
        if (callback) callback(...args)
      }
    )
  }

  clean() {
    super.clean()
  }
}

class MessageQueueController extends TsDictStorageController {
  static ROOT_KEY = "MessageQueue"
  counters = {}

  constructor(model) {
    super(model)
  }

  _getQueueKey(queueName, index) {
    return `${MessageQueueController.ROOT_KEY}:${queueName}:${index}`
  }

  _getQueueCounter(queueName) {
    if (!(queueName in this.counters)) {
      this.counters[queueName] = 0
    }
    return this.counters[queueName]
  }

  _incrementQueueCounter(queueName) {
    this.counters[queueName] = this._getQueueCounter(queueName) + 1
  }

  push(message, queueName = "default") {
    const counter = this._getQueueCounter(queueName)
    const key = this._getQueueKey(queueName, counter)
    this.set(key, message)
    this._incrementQueueCounter(queueName)
    return key
  }

  pop(queueName = "default") {
    const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`)
    if (!keys.length) return null // Queue is empty
    const earliestKey = keys[0]
    const message = this.get(earliestKey)
    this.delete(earliestKey)
    return message
  }

  peek(queueName = "default") {
    const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`)
    if (!keys.length) return null // Queue is empty
    return this.get(keys[0])
  }

  size(queueName = "default") {
    return this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).length
  }

  clear(queueName = "default") {
    this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).forEach(
      key => {
        this.delete(key)
      }
    )
    if (queueName in this.counters) {
      delete this.counters[queueName]
    }
  }
}

class LocalVersionController {
  static TABLENAME = "_Operation"
  static KEY = "ops"
  static FORWARD = "forward"
  static REVERT = "revert"

  _currentVersion = ""

  constructor(client = null, limitMemoryMB = 128) {
    this.limitMemoryMB = limitMemoryMB
    this.client = client || new TsDictStorageController(new TsDictStorage())

    // Ensure ops list exists without clobbering existing data
    let table
    try {
      table = this.client.get(LocalVersionController.TABLENAME) || {}
    } catch {
      table = {}
    }
    if (!(LocalVersionController.KEY in (table || {}))) {
      this.client.set(LocalVersionController.TABLENAME, {
        [LocalVersionController.KEY]: []
      })
    }
  }

  /** Return the ordered list of version UUIDs (empty list if none). */
  getVersions() {
    try {
      const table = this.client.get(LocalVersionController.TABLENAME) || {}
      const ops = table?.[LocalVersionController.KEY]
      return Array.isArray(ops) ? [...ops] : []
    } catch {
      return []
    }
  }

  /** Persist the ordered list of version UUIDs. */
  _setVersions(ops) {
    return this.client.set(LocalVersionController.TABLENAME, {
      [LocalVersionController.KEY]: [...ops]
    })
  }

  findVersion(versionUuid) {
    const versions = this.getVersions()
    const currentVersionIdx =
      this._currentVersion && versions.includes(this._currentVersion)
        ? versions.indexOf(this._currentVersion)
        : -1

    const targetVersionIdx =
      versionUuid && versions.includes(versionUuid)
        ? versions.indexOf(versionUuid)
        : null
    const op =
      targetVersionIdx !== null
        ? this.client.get(
            `${LocalVersionController.TABLENAME}:${versions[targetVersionIdx]}`
          )
        : null

    return [versions, currentVersionIdx, targetVersionIdx, op]
  }

  estimateMemoryMB() {
    try {
      const raw = this.client.model?.memoryUsage?.(true, false)
      if (typeof raw === "number") return raw / (1024 * 1024)
    } catch {
      /* ignore */
    }
    return 0
  }

  /**
   * Append a new operation after the current pointer, truncating any redo tail.
   * Returns a warning string if memory exceeds the limit, else null.
   */
  addOperation(operation, revert = null) {
    const opUuid = uuidv4()

    const tmp = {
      [`${LocalVersionController.TABLENAME}:${opUuid}`]: {
        [LocalVersionController.FORWARD]: operation,
        [LocalVersionController.REVERT]: revert
      }
    }
    const willUseMB = getDeepSize(tmp) / (1024 * 1024)

    while (willUseMB + this.estimateMemoryMB() > this.limitMemoryMB) {
      const popped = this.popOperation(1)
      if (!popped.length) break
    }

    this.client.set(`${LocalVersionController.TABLENAME}:${opUuid}`, {
      [LocalVersionController.FORWARD]: operation,
      [LocalVersionController.REVERT]: revert
    })

    let ops = this.getVersions()
    if (this._currentVersion !== null && ops.includes(this._currentVersion)) {
      const opIdx = ops.indexOf(this._currentVersion)
      ops = ops.slice(0, opIdx + 1) // drop any redo branch
    }
    ops.push(opUuid)
    this._setVersions(ops)
    this._currentVersion = opUuid

    const usage = this.estimateMemoryMB()
    if (usage > this.limitMemoryMB) {
      const res = `[LocalVersionController] Warning: memory usage ${usage.toFixed(
        1
      )} MB exceeds limit of ${this.limitMemoryMB} MB`
      // mirror Python's print + return
      // eslint-disable-next-line no-console
      console.warn(res)
      return res
    }
    return null
  }

  /** Pop n operations from the head (or tail if head is the current op). */
  popOperation(n = 1) {
    if (n <= 0) return []

    let ops = this.getVersions()
    if (!ops.length) return []

    const popped = []
    const count = Math.min(n, ops.length)

    for (let i = 0; i < count; i++) {
      const popIdx = ops[0] !== this._currentVersion ? 0 : ops.length - 1
      const opId = ops[popIdx]
      const opRecord = this.client.get(
        `${LocalVersionController.TABLENAME}:${opId}`
      )
      popped.push([opId, opRecord])

      ops.splice(popIdx, 1)
      this.client.delete(`${LocalVersionController.TABLENAME}:${opId}`)
    }

    this._setVersions(ops)

    if (!this._currentVersion || !ops.includes(this._currentVersion)) {
      this._currentVersion = ops.length ? ops[ops.length - 1] : null
    }
    return popped
  }

  forwardOneOperation(forwardCallback) {
    const [versions, currentVersionIdx] = this.findVersion(this._currentVersion)
    const nextIdx = currentVersionIdx + 1
    if (nextIdx >= versions.length) return

    const op = this.client.get(
      `${LocalVersionController.TABLENAME}:${versions[nextIdx]}`
    )
    if (!op || !(LocalVersionController.FORWARD in op)) return

    // Only advance the pointer if the callback succeeds
    forwardCallback(op[LocalVersionController.FORWARD])
    this._currentVersion = versions[nextIdx]
  }

  revertOneOperation(revertCallback) {
    const [versions, currentVersionIdx, , op] = this.findVersion(
      this._currentVersion
    )
    if (currentVersionIdx <= 0) return
    if (!op || !(LocalVersionController.REVERT in op)) return

    revertCallback(op[LocalVersionController.REVERT])
    this._currentVersion = versions[currentVersionIdx - 1]
  }

  toVersion(versionUuid, versionCallback) {
    let [_, currentIdx, targetIdx] = this.findVersion(versionUuid)
    if (targetIdx === null) throw new Error(`no such version of ${versionUuid}`)

    if (currentIdx === null) currentIdx = -1 // normalize "no current" to -1

    while (currentIdx !== targetIdx) {
      if (currentIdx < targetIdx) {
        this.forwardOneOperation(versionCallback)
        currentIdx += 1
      } else {
        this.revertOneOperation(versionCallback)
        currentIdx -= 1
      }
    }
  }
}

export class SingletonKeyValueStorage {
  versionController = new LocalVersionController()
  eventDispatcher = new EventDispatcherController(new TsDictStorage())
  messageQueue = new MessageQueueController(new TsDictStorage())

  static backends = {
    temp_ts: (...args) =>
      new TsDictStorageController(new TsDictStorage(...args)),
    ts: (...args) => {
      const storage = new TsDictStorage(...args)
      const singleton = storage.getSingleton()
      return new TsDictStorageController(singleton)
    },
    file: (...args) => new TsDictStorageController(new TsDictStorage(...args)),
    couch: (...args) => {
      console.warn(
        "CouchDB backend is registered but requires external implementation"
      )
      return new TsDictStorageController(new TsDictStorage())
    }
  }

  constructor(versionControl = false) {
    this.versionControl = versionControl
    this.conn = null
    this.tsBackend()
  }

  switchBackend(name = "ts", ...args) {
    this.eventDispatcher = new EventDispatcherController(new TsDictStorage())
    this.versionController = new LocalVersionController()
    this.messageQueue = new MessageQueueController(new TsDictStorage())

    const backend = SingletonKeyValueStorage.backends[name.toLowerCase()]
    if (!backend) {
      throw new Error(
        `No backend named ${name}, available backends are: ${Object.keys(
          SingletonKeyValueStorage.backends
        ).join(", ")}`
      )
    }

    return backend(...args)
  }

  s3Backend(
    bucketName,
    awsAccessKeyId,
    awsSecretAccessKey,
    regionName,
    s3StoragePrefixPath = "/SingletonS3Storage"
  ) {
    this.conn = this.switchBackend(
      "s3",
      bucketName,
      awsAccessKeyId,
      awsSecretAccessKey,
      regionName,
      s3StoragePrefixPath
    )
  }

  tempTsBackend() {
    this.conn = this.switchBackend("temp_ts")
  }

  tsBackend() {
    this.conn = this.switchBackend("ts")
  }

  fileBackend(filePath = "storage.json") {
    this.conn = this.switchBackend("file", filePath)
  }

  sqlitePymixBackend(mode = "sqlite.db") {
    this.conn = this.switchBackend("sqlite_pymix", { mode })
  }

  sqliteBackend() {
    this.conn = this.switchBackend("sqlite")
  }

  firestoreBackend(googleProjectId, googleFirestoreCollection) {
    this.conn = this.switchBackend(
      "firestore",
      googleProjectId,
      googleFirestoreCollection
    )
  }

  redisBackend(redisUrl = "redis://127.0.0.1:6379") {
    this.conn = this.switchBackend("redis", redisUrl)
  }

  couchBackend(
    url = "http://localhost:5984",
    dbName = "test",
    username = "",
    password = ""
  ) {
    this.conn = this.switchBackend("couch", url, dbName, username, password)
  }

  mongoBackend(
    mongoUrl = "mongodb://127.0.0.1:27017/",
    dbName = "SingletonDB",
    collectionName = "store"
  ) {
    this.conn = this.switchBackend("mongodb", mongoUrl, dbName, collectionName)
  }

  logMessage(message) {
    console.log(`[SingletonKeyValueStorage]: ${message}`)
  }

  addSlave(slave, eventNames = ["set", "delete"]) {
    if (!slave.uuid) {
      try {
        slave.uuid = uuidv4()
      } catch (error) {
        this.logMessage(`Cannot set UUID for ${slave}. Skipping this slave.`)
        return
      }
    }

    eventNames.forEach(event => {
      if (slave[event]) {
        this.setEvent(event, slave[event].bind(slave), slave.uuid)
      } else {
        this.logMessage(`No method "${event}" in ${slave}. Skipping it.`)
      }
    })
  }

  deleteSlave(slave) {
    if (slave.uuid) {
      this.deleteEvent(slave.uuid)
    }
  }

  editLocal(funcName, key, value) {
    // Validate that funcName is one of the allowed methods
    if (!["set", "delete", "clean", "load", "loads"].includes(funcName)) {
      this.logMessage(`No method "${funcName}". Returning.`)
      return
    }

    // Filter out undefined arguments
    const args = [key, value].filter(arg => arg !== undefined)

    // Safely access the method using a type assertion
    const func = this.conn?.[funcName]
    if (!func) return

    // Call the function with the provided arguments
    return func.bind(this.conn)(...args)
  }

  edit(funcName, key, value) {
    const args = [key, value].filter(arg => arg !== undefined)
    const result = this.editLocal(funcName, key, value)
    this.dispatchEvent(funcName, ...args)
    return result
  }

  tryEditWithErrorHandling(args) {
    const [func, key, value] = args
    if (this.versionControl) {
      let revert
      if (func === "set") {
        if (this.exists(key)) {
          revert = ["set", key, this.get(key)]
        } else {
          revert = ["delete", key]
        }
      } else if (func === "delete") {
        revert = ["set", key, this.get(key)]
      } else if (["clean", "load", "loads"].includes(func)) {
        revert = ["loads", this.dumps()]
      }

      if (revert) {
        this.versionController.addOperation(args, revert)
      }
    }

    try {
      this.edit(func, key, value)
      return true
    } catch (error) {
      if (error instanceof Error) {
        this.logMessage(error.message)
      } else {
        this.logMessage("An unknown error occurred.")
      }

      return false
    }
  }

  revertOneOperation() {
    this.versionController.revertOneOperation(revert => {
      const [func, key, value] = revert
      this.editLocal(func, key, value)
    })
  }

  forwardOneOperation() {
    this.versionController.forwardOneOperation(forward => {
      const [func, key, value] = forward
      this.editLocal(func, key, value)
    })
  }

  getCurrentVersion() {
    return this.versionController._currentVersion
  }

  localToVersion(opUuid) {
    this.versionController.toVersion(opUuid, revert => {
      const [func, key, value] = revert
      this.editLocal(func, key, value)
    })
  }

  set(key, value) {
    return this.tryEditWithErrorHandling(["set", key, value])
  }

  delete(key) {
    return this.tryEditWithErrorHandling(["delete", key])
  }

  clean() {
    return this.tryEditWithErrorHandling(["clean"])
  }

  load(jsonPath) {
    return this.tryEditWithErrorHandling(["load", jsonPath])
  }

  loads(jsonStr) {
    return this.tryEditWithErrorHandling(["loads", jsonStr])
  }

  tryLoadWithErrorHandling(func) {
    try {
      return func()
    } catch (error) {
      if (error instanceof Error) {
        this.logMessage(error.message)
      } else {
        this.logMessage("An unknown error occurred.")
      }

      return null
    }
  }

  exists(key) {
    return this.tryLoadWithErrorHandling(() => this.conn?.exists(key))
  }

  keys(regex = "*") {
    return this.tryLoadWithErrorHandling(() => this.conn?.keys(regex)) || []
  }

  get(key) {
    return this.tryLoadWithErrorHandling(() => this.conn?.get(key))
  }

  dumps() {
    return this.tryLoadWithErrorHandling(() => this.conn?.dumps()) || ""
  }

  dumpRSAs(publicKeyPath, compress = false) {
    return (
      this.tryLoadWithErrorHandling(() =>
        this.conn?.dumpRSAs(publicKeyPath, compress)
      ) || ""
    )
  }

  loadRSAs(content, privateKeyPath) {
    return (
      this.tryLoadWithErrorHandling(() =>
        this.conn?.loadRSAs(content, privateKeyPath)
      ) || ""
    )
  }
  // dump(jsonPath: string): string {
  //     return this.tryLoadWithErrorHandling(() => this.conn?.dump(jsonPath)) || '';
  // }

  events() {
    return this.eventDispatcher.events()
  }

  getEvent(uuid) {
    return this.eventDispatcher.getEvent(uuid)
  }

  deleteEvent(uuid) {
    this.eventDispatcher.deleteEvent(uuid)
  }

  setEvent(eventName, callback, id) {
    return this.eventDispatcher.setEvent(eventName, callback, id)
  }

  dispatchEvent(eventName, ...args) {
    this.eventDispatcher.dispatchEvent(eventName, ...args)
  }

  cleanEvents() {
    this.eventDispatcher.clean()
  }

  // Message Queue methods
  pushMessage(message, queueName = "default") {
    return this.messageQueue.push(message, queueName)
  }

  popMessage(queueName = "default") {
    return this.messageQueue.pop(queueName)
  }

  peekMessage(queueName = "default") {
    return this.messageQueue.peek(queueName)
  }

  queueSize(queueName = "default") {
    return this.messageQueue.size(queueName)
  }

  clearQueue(queueName = "default") {
    this.messageQueue.clear(queueName)
  }
}
