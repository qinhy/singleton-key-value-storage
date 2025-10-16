// from https://github.com/qinhy/singleton-key-value-storage.git

import { PEMFileReader, SimpleRSAChunkEncryptor } from './RSA';
import { Buffer } from 'buffer';

export function uuidv4() {
    const b = new Uint8Array(16);
    crypto.getRandomValues(b);          // 128 random bits

    b[6] = (b[6] & 0x0f) | 0x40;        // version = 4
    b[8] = (b[8] & 0x3f) | 0x80;        // variant = RFC 4122

    const hex = [...b].map(x => x.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
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
export function getDeepSize(
    obj: any,
    seen: WeakSet<object> | null = null,
    shallow: boolean = false
): number {
    const _seen = seen ?? new WeakSet<object>();

    const sizeOfPrimitive = (value: any): number => {
        switch (typeof value) {
            case "string":
                return value.length * 2; // UTF-16, ~2 bytes/char
            case "number":
                return 8; // 64-bit float
            case "boolean":
                return 4;
            case "bigint":
                // very rough: cost ~ length of decimal representation
                return value.toString().length * 2;
            case "symbol":
            case "function":
            case "undefined":
                return 0;
            default:
                return 0;
        }
    };

    const sizeOfObject = (value: object): number => {
        if (_seen.has(value)) return 0;
        _seen.add(value);

        // Node Buffer
        if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
            return (value as Buffer).length;
        }
        // ArrayBuffer / DataView / TypedArray
        if (value instanceof ArrayBuffer) return value.byteLength;
        if (ArrayBuffer.isView(value)) return (value as ArrayBufferView).byteLength;

        // Date ~ 8 bytes
        if (value instanceof Date) return 8;

        // RegExp ~ source length
        if (value instanceof RegExp) return value.source.length * 2;

        // Map: keys + values
        if (value instanceof Map) {
            if (shallow) return value.size * 8; // rough pointer-only estimate
            let s = 0;
            for (const [k, v] of value.entries()) {
                s += getDeepSize(k, _seen, false);
                s += getDeepSize(v, _seen, false);
            }
            return s;
        }

        // Set: items
        if (value instanceof Set) {
            if (shallow) return value.size * 8;
            let s = 0;
            for (const v of value.values()) {
                s += getDeepSize(v, _seen, false);
            }
            return s;
        }

        // Array
        if (Array.isArray(value)) {
            if (shallow) return value.length * 8;
            let s = 0;
            for (const item of value) {
                s += getDeepSize(item, _seen, false);
            }
            return s;
        }

        // Plain object and class instances: own props (including Symbols)
        const propNames = Object.getOwnPropertyNames(value);
        const propSymbols = Object.getOwnPropertySymbols(value);
        let s = 0;

        // If shallow, only account for immediate (rough) pointer cost / primitive bytes
        const visit = (desc: PropertyDescriptor | undefined, val: any) => {
            if (!desc) return;
            // Accessor getters might throw—guard.
            try {
                const cell = desc.get ? desc.get.call(value) : val;
                if (shallow) {
                    s += typeof cell === "object" && cell !== null ? 8 : sizeOfPrimitive(cell);
                } else {
                    s += getDeepSize(cell, _seen, false);
                }
            } catch {
                /* ignore unreadable getter */
            }
        };

        for (const k of propNames) {
            const desc = Object.getOwnPropertyDescriptor(value, k);
            // Pass the current value if it's a data descriptor
            visit(desc, (value as any)[k]);
        }
        for (const sym of propSymbols) {
            const desc = Object.getOwnPropertyDescriptor(value, sym);
            visit(desc, (value as any)[sym as any]);
        }
        return s;
    };

    // primitives/null
    if (obj === null) return 0;
    const t = typeof obj;
    if (t !== "object") return sizeOfPrimitive(obj);

    return sizeOfObject(obj);
}

/** Humanize byte counts like Python's humanize_bytes. */
export function humanizeBytes(n: number): string {
    let size = Number(n);
    const units = ["B", "KB", "MB", "GB", "TB"];
    for (const u of units) {
        if (size < 1024) return `${size.toFixed(1)} ${u}`;
        size /= 1024;
    }
    return `${size.toFixed(1)} PB`;
}

export class AbstractStorage {
    static _uuid: string = uuidv4();
    static _store: any = null;
    static _is_singleton: boolean = true;
    static _meta: Record<string, any> = {};

    uuid: string;
    store: any;
    isSingleton: boolean;

    constructor(id: string | null = null, store: any = null, isSingleton: boolean | null = null) {
        this.uuid = id ?? uuidv4();
        this.store = store ?? null;
        this.isSingleton = isSingleton ?? false;
    }

    getSingleton(): AbstractStorage {
        return new AbstractStorage(AbstractStorage._uuid, AbstractStorage._store, AbstractStorage._is_singleton);
    }

    /** Subclasses must implement memory usage (deep or shallow). */
    memoryUsage(deep?: boolean, humanReadable?: boolean): number | string {
        throw new Error("Subclasses must implement memoryUsage method");  
    }
}

export class TsDictStorage extends AbstractStorage {
    static _uuid: string = uuidv4();
    static _store: Record<string, any> = {};
    private _filePath: string | null = null;

    constructor(id: string | null = null, store: any = null, isSingleton: boolean | null = null) {
        super(id, store, isSingleton);
        this.store = store ?? {};

        // Handle file path if provided as first argument
        if (id && typeof id === 'string' && !store && !isSingleton) {
            this._filePath = id;
            this.load();
        }
    }

    memoryUsage(deep: boolean = true, humanReadable: boolean = true): number | string {
        // deep -> full traversal; shallow -> top-level estimate only
        const size = getDeepSize(this, null, !deep);
        return humanReadable ? humanizeBytes(size) : size;
    }

    dump(): boolean {
        if (!this._filePath) return false;
        try {
            const fs = require('fs');
            fs.writeFileSync(this._filePath, JSON.stringify(this.store), 'utf8');
            return true;
        } catch (error) {
            console.error(`Error writing to file ${this._filePath}:`, error);
            return false;
        }
    }

    load(): boolean {
        if (!this._filePath) return false;
        try {
            const fs = require('fs');
            if (fs.existsSync(this._filePath)) {
                const data = fs.readFileSync(this._filePath, 'utf8');
                this.store = JSON.parse(data);
                return true;
            }
            return false;
        } catch (error) {
            console.error(`Error reading from file ${this._filePath}:`, error);
            return false;
        }
    }
}

abstract class AbstractStorageController {
    model: AbstractStorage;

    constructor(model: AbstractStorage) {
        this.model = model;
    }

    isSingleton(): boolean {
        return this.model.isSingleton;
    }

    exists(key: string): boolean {
        console.error(`[${this.constructor.name}]: not implemented`);
        return false;
    }

    set(key: string, value: Record<string, any>): void {
        console.error(`[${this.constructor.name}]: not implemented`);
    }

    get(key: string): Record<string, any> | null {
        console.error(`[${this.constructor.name}]: not implemented`);
        return null;
    }

    delete(key: string): void {
        console.error(`[${this.constructor.name}]: not implemented`);
    }

    keys(pattern: string = '*'): string[] {
        console.error(`[${this.constructor.name}]: not implemented`);
        return [];
    }

    clean(): void {
        this.keys('*').forEach((key) => this.delete(key));
    }

    dumps(): string {
        const data: Record<string, any> = {};
        this.keys('*').forEach((key) => {
            data[key] = this.get(key);
        });
        return JSON.stringify(data);
    }

    loads(jsonString: string = '{}'): void {
        const data = JSON.parse(jsonString);
        Object.entries(data).forEach(([key, value]) => {
            const val = value ? value : {}
            this.set(key, val);
        });
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
    dumpRSAs(publicKeyPath: string, compress: boolean = false): string {
        const publicKey = new PEMFileReader(publicKeyPath).loadPublicPkcs8Key();
        const encryptor = new SimpleRSAChunkEncryptor(publicKey, null);
        return encryptor.encryptString(this.dumps(), compress);
    }

    loadRSAs(content: string, privateKeyPath: string): void {
        const privateKey = new PEMFileReader(privateKeyPath).loadPrivatePkcs8Key();
        const encryptor = new SimpleRSAChunkEncryptor(null, privateKey);
        const decryptedText = encryptor.decryptString(content);
        this.loads(decryptedText);
    }
}


class TsDictStorageController extends AbstractStorageController {
    store: Record<string, any>;

    constructor(model: TsDictStorage) {
        super(model);
        this.store = model.store;
    }

    exists(key: string): boolean {
        return key in this.store;
    }

    set(key: string, value: Record<string, any>): void {
        this.store[key] = value;
    }

    get(key: string): Record<string, any> | null {
        return this.store[key] || null;
    }

    delete(key: string): void {
        delete this.store[key];
    }

    keys(pattern = '*') {
        const regex = new RegExp('^' + pattern.replace(/\*/g, '.*'));
        return Object.keys(this.model.store).filter(key => key.match(regex));
    }
}

class EventDispatcherController extends TsDictStorageController {
    static ROOT_KEY = 'Event';

    private _findEvent(uuid: string): string[] {
        const keys = this.keys(`*:${uuid}`);
        return keys.length === 0 ? [] : keys;
    }

    set(key: string, value: CallableFunction): void {
        this.store[key] = value;
    }

    get(key: string): CallableFunction | null {
        return this.store[key] || null;
    }

    events(): [string, any][] {
        return this.keys('*').map((key) => [key, this.get(key)]);
    }

    getEvent(uuid: string): any[] {
        return this._findEvent(uuid).map((key) => (key ? this.get(key) : null));
    }

    deleteEvent(uuid: string): void {
        this._findEvent(uuid).forEach((key) => {
            if (key) this.delete(key);
        });
    }

    setEvent(eventName: string, callback: Function, id: string): string {
        if (!id) id = uuidv4();
        this.set(`${EventDispatcherController.ROOT_KEY}:${eventName}:${id}`, callback);
        return id;
    }

    dispatchEvent(eventName: string, ...args: any[]): void {
        this.keys(`${EventDispatcherController.ROOT_KEY}:${eventName}:*`).forEach((key) => {
            const callback = this.get(key);
            if (callback) callback(...args);
        });
    }

    clean(): void {
        super.clean();
    }
}

class MessageQueueController extends TsDictStorageController {
    static ROOT_KEY = '_MessageQueue';
    private counters: Record<string, number> = {};

    constructor(model: TsDictStorage) {
        super(model);
    }

    private _getQueueKey(queueName: string, index: number): string {
        return `${MessageQueueController.ROOT_KEY}:${queueName}:${index}`;
    }

    private _getQueueCounter(queueName: string): number {
        if (!(queueName in this.counters)) {
            this.counters[queueName] = 0;
        }
        return this.counters[queueName];
    }

    private _incrementQueueCounter(queueName: string): void {
        this.counters[queueName] = this._getQueueCounter(queueName) + 1;
    }

    push(message: Record<string, any>, queueName: string = 'default'): string {
        const counter = this._getQueueCounter(queueName);
        const key = this._getQueueKey(queueName, counter);
        this.set(key, message);
        this._incrementQueueCounter(queueName);
        return key;
    }

    pop(queueName: string = 'default'): Record<string, any> | null {
        const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`);
        if (!keys.length) return null; // Queue is empty
        const earliestKey = keys[0];
        const message = this.get(earliestKey);
        this.delete(earliestKey);
        return message;
    }

    peek(queueName: string = 'default'): Record<string, any> | null {
        const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`);
        if (!keys.length) return null; // Queue is empty
        return this.get(keys[0]);
    }

    size(queueName: string = 'default'): number {
        return this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).length;
    }

    clear(queueName: string = 'default'): void {
        this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).forEach(key => {
            this.delete(key);
        });
        if (queueName in this.counters) {
            delete this.counters[queueName];
        }
    }
}


class LocalVersionController {
  static TABLENAME = "_Operation";
  static KEY = "ops";
  static FORWARD = "forward";
  static REVERT = "revert";

  private client: TsDictStorageController;
    _currentVersion: string = "";
  limitMemoryMB: number;

  constructor(client: TsDictStorageController | null = null, limitMemoryMB: number = 128) {
    this.limitMemoryMB = limitMemoryMB;
    this.client = client || (new TsDictStorageController(new TsDictStorage()) as TsDictStorageController);

    // Ensure ops list exists without clobbering existing data
    let table: any;
    try {
      table = this.client.get(LocalVersionController.TABLENAME) || {};
    } catch {
      table = {};
    }
    if (!(LocalVersionController.KEY in (table || {}))) {
      this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [] });
    }
  }

  getCurrentVersion(): string {
    return this._currentVersion;
}

  /** Return the ordered list of version UUIDs (empty list if none). */
  getVersions(): string[] {
    try {
      const table = this.client.get(LocalVersionController.TABLENAME) || {};
      const ops = table?.[LocalVersionController.KEY];
      return Array.isArray(ops) ? [...ops] : [];
    } catch {
      return [];
    }
  }

  /** Persist the ordered list of version UUIDs. */
  private _setVersions(ops: string[]) {
    return this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [...ops] });
  }

  findVersion(
    versionUuid: string | null
  ): [versions: string[], currentVersionIdx: number, targetVersionIdx: number | null, op: any | null] {
    const versions = this.getVersions();
    const currentVersionIdx =
      this._currentVersion && versions.includes(this._currentVersion)
        ? versions.indexOf(this._currentVersion)
        : -1;

    const targetVersionIdx = versionUuid && versions.includes(versionUuid) ? versions.indexOf(versionUuid) : null;
    const op =
      targetVersionIdx !== null
        ? (this.client.get(`${LocalVersionController.TABLENAME}:${versions[targetVersionIdx]}`) as any | null)
        : null;

    return [versions, currentVersionIdx, targetVersionIdx, op];
  }

  estimateMemoryMB(): number {
    try {
      const raw = this.client.model?.memoryUsage?.(true, false);
      if (typeof raw === "number") return raw / (1024 * 1024);
    } catch {
      /* ignore */
    }
    return 0;
  }

  /**
   * Append a new operation after the current pointer, truncating any redo tail.
   * Returns a warning string if memory exceeds the limit, else null.
   */
  addOperation(operation: any, revert: any = null): string | null {
    const opUuid = uuidv4();

    const tmp = {
      [`${LocalVersionController.TABLENAME}:${opUuid}`]: {
        [LocalVersionController.FORWARD]: operation,
        [LocalVersionController.REVERT]: revert,
      },
    };
    const willUseMB = getDeepSize(tmp) / (1024 * 1024);

    while (willUseMB + this.estimateMemoryMB() > this.limitMemoryMB) {
      const popped = this.popOperation(1);
      if (!popped.length) break;
    }

    this.client.set(`${LocalVersionController.TABLENAME}:${opUuid}`, {
      [LocalVersionController.FORWARD]: operation,
      [LocalVersionController.REVERT]: revert,
    });

    let ops = this.getVersions();
    if (this._currentVersion !== null && ops.includes(this._currentVersion)) {
      const opIdx = ops.indexOf(this._currentVersion);
      ops = ops.slice(0, opIdx + 1); // drop any redo branch
    }
    ops.push(opUuid);
    this._setVersions(ops);
    this._currentVersion = opUuid;

    const usage = this.estimateMemoryMB();
    if (usage > this.limitMemoryMB) {
      const res = `[LocalVersionController] Warning: memory usage ${usage.toFixed(1)} MB exceeds limit of ${this.limitMemoryMB} MB`;
      // mirror Python's print + return
      // eslint-disable-next-line no-console
      console.warn(res);
      return res;
    }
    return null;
  }

  /** Pop n operations from the head (or tail if head is the current op). */
  popOperation(n: number = 1): Array<[string, any]> {
    if (n <= 0) return [];

    let ops = this.getVersions();
    if (!ops.length) return [];

    const popped: Array<[string, any]> = [];
    const count = Math.min(n, ops.length);

    for (let i = 0; i < count; i++) {
      const popIdx = ops[0] !== this._currentVersion ? 0 : ops.length - 1;
      const opId = ops[popIdx];
      const opRecord = this.client.get(`${LocalVersionController.TABLENAME}:${opId}`) as any;
      popped.push([opId, opRecord]);

      ops.splice(popIdx, 1);
      this.client.delete(`${LocalVersionController.TABLENAME}:${opId}`);
    }

    this._setVersions(ops);

    if (!this._currentVersion || !ops.includes(this._currentVersion)) {
            this._currentVersion = ops.length ? ops[ops.length - 1] : null;
    }
    return popped;
  }

  forwardOneOperation(forwardCallback: (forward: any) => void): void {
    const [versions, currentVersionIdx] = this.findVersion(this._currentVersion);
    const nextIdx = currentVersionIdx + 1;
    if (nextIdx >= versions.length) return;

    const op = this.client.get(`${LocalVersionController.TABLENAME}:${versions[nextIdx]}`) as any | null;
    if (!op || !(LocalVersionController.FORWARD in op)) return;

    // Only advance the pointer if the callback succeeds
    forwardCallback(op[LocalVersionController.FORWARD]);
    this._currentVersion = versions[nextIdx];
  }

  revertOneOperation(revertCallback: (revert: any) => void): void {
    const [versions, currentVersionIdx, , op] = this.findVersion(this._currentVersion);
    if (currentVersionIdx <= 0) return;
    if (!op || !(LocalVersionController.REVERT in op)) return;

    revertCallback(op[LocalVersionController.REVERT]);
    this._currentVersion = versions[currentVersionIdx - 1];
  }

  toVersion(versionUuid: string, versionCallback: (opArgs: any) => void): void {
    let [_, currentIdx, targetIdx] = this.findVersion(versionUuid);
    if (targetIdx === null) throw new Error(`no such version of ${versionUuid}`);

    if (currentIdx === null) currentIdx = -1; // normalize "no current" to -1

    while (currentIdx !== targetIdx) {
      if (currentIdx < targetIdx) {
        this.forwardOneOperation(versionCallback);
        currentIdx += 1;
      } else {
        this.revertOneOperation(versionCallback);
        currentIdx -= 1;
      }
    }
  }
}

export class SingletonKeyValueStorage {
    versionControl: boolean;
    conn: TsDictStorageController | null;
    versionController: LocalVersionController = new LocalVersionController();
    private eventDispatcher: EventDispatcherController = new EventDispatcherController(new TsDictStorage());    
    private messageQueue: MessageQueueController = new MessageQueueController(new TsDictStorage());

    private static backends: Record<string, (...args: any[]) => TsDictStorageController> = {
        temp_ts: (...args: any[]) => new TsDictStorageController(new TsDictStorage(...args)),
        ts: (...args: any[]) => {
            const storage = new TsDictStorage(...args);
            const singleton = storage.getSingleton() as TsDictStorage;
            return new TsDictStorageController(singleton);
        },
        file: (...args: any[]) => new TsDictStorageController(new TsDictStorage(...args)),
        couch: (...args: any[]) => {
            console.warn('CouchDB backend is registered but requires external implementation');
            return new TsDictStorageController(new TsDictStorage());
        }
    };

    constructor(versionControl = false) {
        this.versionControl = versionControl;
        this.conn = null;
        this.tsBackend();
    }

    private switchBackend(name: string = 'ts', ...args: any[]): TsDictStorageController {
        this.eventDispatcher = new EventDispatcherController(new TsDictStorage());
        this.versionController = new LocalVersionController();
        this.messageQueue = new MessageQueueController(new TsDictStorage());

        const backend = SingletonKeyValueStorage.backends[name.toLowerCase()];
        if (!backend) {
            throw new Error(`No backend named ${name}, available backends are: ${Object.keys(SingletonKeyValueStorage.backends).join(', ')}`);
        }

        return backend(...args);
    }

    s3Backend(
        bucketName: string,
        awsAccessKeyId: string,
        awsSecretAccessKey: string,
        regionName: string,
        s3StoragePrefixPath: string = '/SingletonS3Storage'
    ): void {
        this.conn = this.switchBackend('s3', bucketName, awsAccessKeyId, awsSecretAccessKey, regionName, s3StoragePrefixPath);
    }

    tempTsBackend(): void {
        this.conn = this.switchBackend('temp_ts');
    }

    tsBackend(): void {
        this.conn = this.switchBackend('ts');
    }

    fileBackend(filePath: string = 'storage.json'): void {
        this.conn = this.switchBackend('file', filePath);
    }

    sqlitePymixBackend(mode: string = 'sqlite.db'): void {
        this.conn = this.switchBackend('sqlite_pymix', { mode });
    }

    sqliteBackend(): void {
        this.conn = this.switchBackend('sqlite');
    }

    firestoreBackend(googleProjectId: string, googleFirestoreCollection: string): void {
        this.conn = this.switchBackend('firestore', googleProjectId, googleFirestoreCollection);
    }

    redisBackend(redisUrl: string = 'redis://127.0.0.1:6379'): void {
        this.conn = this.switchBackend('redis', redisUrl);
    }

    couchBackend(url: string = 'http://localhost:5984', dbName: string = 'test', username: string = '', password: string = ''): void {
        this.conn = this.switchBackend('couch', url, dbName, username, password);
    }

    mongoBackend(mongoUrl: string = 'mongodb://127.0.0.1:27017/', dbName: string = 'SingletonDB', collectionName: string = 'store'): void {
        this.conn = this.switchBackend('mongodb', mongoUrl, dbName, collectionName);
    }

    private logMessage(message: string): void {
        console.log(`[SingletonKeyValueStorage]: ${message}`);
    }

    addSlave(slave: any, eventNames: string[] = ['set', 'delete']): void {
        if (!slave.uuid) {
            try {
                slave.uuid = uuidv4();
            } catch (error) {
                this.logMessage(`Cannot set UUID for ${slave}. Skipping this slave.`);
                return;
            }
        }

        eventNames.forEach((event) => {
            if (slave[event]) {
                this.setEvent(event, slave[event].bind(slave), slave.uuid);
            } else {
                this.logMessage(`No method "${event}" in ${slave}. Skipping it.`);
            }
        });
    }

    deleteSlave(slave: any): void {
        if (slave.uuid) {
            this.deleteEvent(slave.uuid);
        }
    }

    private createRevert(
        args: any[],
    ) {
        const [func, key, value] = args;
        let revert = null;
        if (func === 'set') {
            if (this.exists(key)) {
                revert = ['set', key, this.get(key)];
            } else {
                revert = ['delete', key];
            }
        } else if (func === 'delete') {
            revert = ['set', key, this.get(key)];
        } else if (['clean', 'load', 'loads'].includes(func)) {
            revert = ['loads', this.dumps()];
        }
        return revert;
    }

    private addRevertOperation(
        args: any[]) {
        let revert = this.createRevert(args);
        if (revert) {
            this.versionController.addOperation(args, revert);
        }
    }

    private editConn(
        funcName: 'set' | 'delete' | 'clean' | 'load' | 'loads',
        key?: string,
        value?: Record<string, any>
    ): any {
        // Validate that funcName is one of the allowed methods
        if (!['set', 'delete', 'clean', 'load', 'loads'].includes(funcName)) {
            this.logMessage(`No method "${funcName}". Returning.`);
            return;
        }

        // Filter out undefined arguments
        const args = [key, value].filter((arg) => arg !== undefined);

        // Safely access the method using a type assertion
        const func = this.conn?.[funcName as keyof typeof this.conn] as Function;
        if (!func) return;

        const result = func.bind(this.conn)(...args);

        return result;
    }

    private tryEditWithErrorHandling(
        args: any[],
        vc: boolean = true,
        de: boolean = true,
    ): boolean {
        const [func, key, value] = args;
        if (this.versionControl && vc) {
            this.addRevertOperation([func, key, value]);
        }
        try {
            this.editConn(func, key, value);
            if(de)this.dispatchEvent(func, ...args);
            return true;
        } catch (error) {

            if (error instanceof Error) {
                this.logMessage(error.message);
            } else {
                this.logMessage('An unknown error occurred.');
            }

            return false;
        }
    }

    revertOneOperation(): void {
        this.versionController.revertOneOperation((revert) => {
            this.tryEditWithErrorHandling(revert, false, false);
        });
    }

    forwardOneOperation(): void {
        this.versionController.forwardOneOperation((forward) => {
            this.tryEditWithErrorHandling(forward, false, false);
        });
    }
    
    getCurrentVersion() {
        return this.versionController?.getCurrentVersion();
    }
    getVersions() {
       return this.versionController?.getVersions();
    }

    localToVersion(opUuid: string): void {
        this.versionController.toVersion(opUuid, (revert) => {
            this.tryEditWithErrorHandling(revert, false, false);
        });
    }

    set(key: string, value: Record<string, any>): boolean {
        return this.tryEditWithErrorHandling(['set', key, value]);
    }

    delete(key: string): boolean {
        return this.tryEditWithErrorHandling(['delete', key]);
    }

    clean(): boolean {
        return this.tryEditWithErrorHandling(['clean']);
    }

    load(jsonPath: string): boolean {
        return this.tryEditWithErrorHandling(['load', jsonPath]);
    }

    loads(jsonStr: string): boolean {
        return this.tryEditWithErrorHandling(['loads', jsonStr]);
    }

    private tryLoadWithErrorHandling(func: () => any): any {
        try {
            return func();
        } catch (error) {

            if (error instanceof Error) {
                this.logMessage(error.message);
            } else {
                this.logMessage('An unknown error occurred.');
            }

            return null;
        }
    }

    exists(key: string): boolean {
        return this.tryLoadWithErrorHandling(() => this.conn?.exists(key));
    }

    keys(regex: string = '*'): string[] {
        return this.tryLoadWithErrorHandling(() => this.conn?.keys(regex)) || [];
    }

    get(key: string): Record<string, any> | null {
        return this.tryLoadWithErrorHandling(() => this.conn?.get(key));
    }

    dumps(): string {
        return this.tryLoadWithErrorHandling(() => this.conn?.dumps()) || '';
    }

    dumpRSAs(publicKeyPath: string, compress: boolean = false): string {
        return this.tryLoadWithErrorHandling(() => this.conn?.dumpRSAs(publicKeyPath, compress)) || '';
    }

    loadRSAs(content: string, privateKeyPath: string): void {
        return this.tryLoadWithErrorHandling(() => this.conn?.loadRSAs(content, privateKeyPath)) || '';
    }
    // dump(jsonPath: string): string {
    //     return this.tryLoadWithErrorHandling(() => this.conn?.dump(jsonPath)) || '';
    // }

    events(): any[] {
        return this.eventDispatcher.events();
    }

    getEvent(uuid: string): any {
        return this.eventDispatcher.getEvent(uuid);
    }

    deleteEvent(uuid: string): void {
        this.eventDispatcher.deleteEvent(uuid);
    }

    setEvent(eventName: string, callback: Function, id: string): string {
        return this.eventDispatcher.setEvent(eventName, callback, id);
    }

    dispatchEvent(eventName: string, ...args: any[]): void {
        this.eventDispatcher.dispatchEvent(eventName, ...args);
    }

    cleanEvents(): void {
        this.eventDispatcher.clean();
    }

    // Message Queue methods
    pushMessage(message: Record<string, any>, queueName: string = 'default'): string {
        return this.messageQueue.push(message, queueName);
    }

    popMessage(queueName: string = 'default'): Record<string, any> | null {
        return this.messageQueue.pop(queueName);
    }

    peekMessage(queueName: string = 'default'): Record<string, any> | null {
        return this.messageQueue.peek(queueName);
    }

    queueSize(queueName: string = 'default'): number {
        return this.messageQueue.size(queueName);
    }

    clearQueue(queueName: string = 'default'): void {
        this.messageQueue.clear(queueName);
    }
}
