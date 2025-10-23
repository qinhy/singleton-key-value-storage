import * as fs from 'fs';
import { randomBytes } from 'crypto';
import { PEMFileReader, SimpleRSAChunkEncryptor } from './RSA.js';

/** @typedef {any} StoreValue */
/** @typedef {{[key: string]: StoreValue}} StoreRecord */
/** @typedef {Array<any>} Operation */
/** @typedef {(...args: any[]) => unknown} EventCallback */
/** @typedef {(key: string, value: StoreValue | null) => void} EvictHandler */

function getGlobalCrypto() {
    if (typeof globalThis === 'undefined') return null;
    const maybeCrypto = globalThis.crypto ?? globalThis.webcrypto;
    if (maybeCrypto && typeof maybeCrypto.getRandomValues === 'function') {
        return maybeCrypto;
    }
    return null;
}

export function uuidv4() {
    const bytes = new Uint8Array(16);
    const globalCrypto = getGlobalCrypto();
    if (globalCrypto) {
        globalCrypto.getRandomValues(bytes);
    } else {
        bytes.set(randomBytes(16));
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

const POINTER_SIZE_BYTES = 8;

function sizeOfPrimitive(value) {
    switch (typeof value) {
        case 'string':
            return Buffer.byteLength(value, 'utf8');
        case 'number':
            return 8;
        case 'boolean':
            return 4;
        case 'bigint':
            return Buffer.byteLength(value.toString(), 'utf8');
        case 'symbol':
            return 0;
        case 'function':
            return POINTER_SIZE_BYTES;
        default:
            return 0;
    }
}

function getDeepBytesSize(obj, seen = new WeakSet(), shallow = false) {
    if (obj === null || obj === undefined) return 0;
    if (typeof obj !== 'object') return sizeOfPrimitive(obj);
    if (seen.has(obj)) return 0;
    seen.add(obj);

    if (Buffer.isBuffer(obj)) return obj.length;
    if (obj instanceof ArrayBuffer) return obj.byteLength;
    if (ArrayBuffer.isView(obj)) return obj.byteLength;
    if (obj instanceof Date) return 8;
    if (obj instanceof RegExp) return Buffer.byteLength(obj.source, 'utf8');

    if (Array.isArray(obj)) {
        if (shallow) return obj.length * POINTER_SIZE_BYTES;
        return obj.reduce((acc, item) => acc + getDeepBytesSize(item, seen, false), 0);
    }

    if (obj instanceof Map) {
        if (shallow) return obj.size * POINTER_SIZE_BYTES;
        let total = 0;
        for (const [k, v] of obj.entries()) {
            total += getDeepBytesSize(k, seen, false);
            total += getDeepBytesSize(v, seen, false);
        }
        return total;
    }

    if (obj instanceof Set) {
        if (shallow) return obj.size * POINTER_SIZE_BYTES;
        let total = 0;
        for (const v of obj.values()) {
            total += getDeepBytesSize(v, seen, false);
        }
        return total;
    }

    let total = 0;
    const props = Object.getOwnPropertyNames(obj);
    const symbols = Object.getOwnPropertySymbols(obj);

    const visit = (descriptor, value) => {
        if (!descriptor) return;
        try {
            let resolved;
            if (descriptor.get && !descriptor.set) {
                resolved = descriptor.get.call(obj);
            } else if ('value' in descriptor) {
                resolved = value;
            }
            if (shallow) {
                total += typeof resolved === 'object' && resolved !== null ? POINTER_SIZE_BYTES : sizeOfPrimitive(resolved);
            } else {
                total += getDeepBytesSize(resolved, seen, false);
            }
        } catch {
            // ignore accessor errors
        }
    };

    for (const key of props) {
        const descriptor = Object.getOwnPropertyDescriptor(obj, key);
        visit(descriptor, obj[key]);
    }

    for (const sym of symbols) {
        const descriptor = Object.getOwnPropertyDescriptor(obj, sym);
        visit(descriptor, obj[sym]);
    }

    return total;
}

const getShallowBytesSize = obj => getDeepBytesSize(obj, new WeakSet(), true);

export function humanizeBytes(n) {
    let size = Number(n);
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    for (const unit of units) {
        if (size < 1024) return `${size.toFixed(1)} ${unit}`;
        size /= 1024;
    }
    return `${size.toFixed(1)} PB`;
}

function globToRegExp(pattern) {
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    return new RegExp('^' + escaped.replace(/\*/g, '.*').replace(/\?/g, '.') + '$');
}

export class AbstractStorage {
    static _uuid = uuidv4();
    static _store = null;
    static _is_singleton = true;
    static _meta = {};

    constructor(id = null, store = null, isSingleton = null) {
        if (new.target === AbstractStorage) {
            throw new TypeError('Cannot instantiate AbstractStorage directly');
        }
        this.uuid = id ?? uuidv4();
        this.store = store ?? null;
        this.isSingleton = isSingleton ?? false;
    }

    getSingleton() {
        const ctor = this.constructor;
        return new ctor(ctor._uuid, ctor._store, ctor._is_singleton);
    }

    bytesUsed(_deep = true, _humanReadable = true) {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }
}

export class DictStorage extends AbstractStorage {
    static _uuid = uuidv4();
    static _store = {};

    constructor(id = null, store = null, isSingleton = null) {
        super(id, store ?? {}, isSingleton);
        this.store = store ?? {};
    }

    bytesUsed(deep = true, humanReadable = true) {
        const size = deep ? getDeepBytesSize(this.store) : getShallowBytesSize(this.store);
        return humanReadable ? humanizeBytes(size) : size;
    }

    static buildTmp() {
        return new DictStorageController(new DictStorage());
    }

    static build() {
        return new DictStorageController(new DictStorage().getSingleton());
    }
}

export class AbstractStorageController {
    constructor(model) {
        this.model = model;
    }

    isSingleton() {
        return Boolean(this.model?.isSingleton);
    }

    bytesUsed(deep = true, humanReadable = true) {
        if (typeof this.model.bytesUsed === 'function') {
            return this.model.bytesUsed(deep, humanReadable);
        }
        return humanReadable ? humanizeBytes(0) : 0;
    }

    exists(_key) {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    set(_key, _value) {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    get(_key) {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    delete(_key) {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    keys(_pattern = '*') {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    clean() {
        for (const key of this.keys('*')) {
            this.delete(key);
        }
    }

    dumps() {
        const snapshot = {};
        for (const key of this.keys('*')) {
            snapshot[key] = this.get(key);
        }
        return JSON.stringify(snapshot);
    }

    loads(jsonString = '{}') {
        const data = JSON.parse(jsonString);
        for (const [key, value] of Object.entries(data)) {
            this.set(key, value);
        }
    }

    dump(path) {
        fs.writeFileSync(path, this.dumps(), 'utf8');
    }

    load(path) {
        const text = fs.readFileSync(path, 'utf8');
        this.loads(text);
    }

    dumpRSA(path, publicPkcs8KeyPath) {
        const encryptor = new SimpleRSAChunkEncryptor(
            new PEMFileReader(publicPkcs8KeyPath).loadPublicPkcs8Key(),
            undefined
        );
        fs.writeFileSync(path, encryptor.encryptString(this.dumps()), 'utf8');
    }

    loadRSA(path, privatePkcs8KeyPath) {
        const encryptor = new SimpleRSAChunkEncryptor(
            undefined,
            new PEMFileReader(privatePkcs8KeyPath).loadPrivatePkcs8Key()
        );
        const decrypted = encryptor.decryptString(fs.readFileSync(path, 'utf8'));
        this.loads(decrypted);
    }
}

export class DictStorageController extends AbstractStorageController {
    constructor(model) {
        super(model);
        this.store = model.store ?? {};
        model.store = this.store;
    }

    exists(key) {
        return Object.prototype.hasOwnProperty.call(this.store, key);
    }

    set(key, value) {
        this.store[key] = value;
    }

    get(key) {
        return this.exists(key) ? this.store[key] : null;
    }

    delete(key) {
        delete this.store[key];
    }

    keys(pattern = '*') {
        const matcher = globToRegExp(pattern);
        return Object.keys(this.store).filter(key => matcher.test(key));
    }
}

export class MemoryLimitedDictStorageController extends DictStorageController {
    constructor(
        model,
        maxMemoryMb = 1024,
        policy = 'lru',
        onEvict = () => undefined,
        pinned = []
    ) {
        super(model);
        this.maxBytes = Math.max(0, maxMemoryMb) * 1024 * 1024;
        this.policy = policy;
        if (!['lru', 'fifo'].includes(this.policy)) {
            throw new Error("policy must be 'lru' or 'fifo'");
        }
        this.onEvict = onEvict;
        this.pinned = new Set(pinned);
        this.sizes = new Map();
        this.order = new Map();
        this.currentBytes = 0;
    }

    entrySize(key, value) {
        return getDeepBytesSize(key) + getDeepBytesSize(value);
    }

    bytesUsed(_deep = true, humanReadable = false) {
        return humanReadable ? humanizeBytes(this.currentBytes) : this.currentBytes;
    }

    reduce(key) {
        if (this.order.has(key)) {
            this.order.delete(key);
        }
        const tracked = this.sizes.get(key) ?? 0;
        if (tracked) {
            this.currentBytes = Math.max(0, this.currentBytes - tracked);
        }
        this.sizes.delete(key);
    }

    pickVictim() {
        for (const key of this.order.keys()) {
            if (!this.pinned.has(key)) {
                return key;
            }
        }
        return null;
    }

    maybeEvict() {
        if (this.maxBytes <= 0) return;
        while (this.currentBytes > this.maxBytes && this.order.size > 0) {
            const victim = this.pickVictim();
            if (!victim) break;
            const value = DictStorageController.prototype.get.call(this, victim);
            this.reduce(victim);
            DictStorageController.prototype.delete.call(this, victim);
            this.onEvict(victim, value);
        }
    }

    set(key, value) {
        const existed = this.exists(key);
        if (existed) {
            this.reduce(key);
        }
        super.set(key, value);

        const size = this.entrySize(key, value);
        this.sizes.set(key, size);
        this.currentBytes += size;

        if (this.order.has(key)) {
            this.order.delete(key);
        }
        this.order.set(key, null);

        this.maybeEvict();
    }

    get(key) {
        const value = super.get(key);
        if (value !== null && this.policy === 'lru' && this.order.has(key)) {
            this.order.delete(key);
            this.order.set(key, null);
        }
        return value;
    }

    delete(key) {
        if (this.exists(key)) {
            this.reduce(key);
        }
        super.delete(key);
    }

    clean() {
        super.clean();
        this.sizes.clear();
        this.order.clear();
        this.currentBytes = 0;
    }
}

export class EventDispatcherController extends DictStorageController {
    static ROOT_KEY = '_Event';

    eventKey(eventName, eventId) {
        return `${EventDispatcherController.ROOT_KEY}:${eventName}:${eventId}`;
    }

    eventPattern(eventName = '*', eventId = '*') {
        return this.eventKey(eventName, eventId);
    }

    findEventKeys(eventId) {
        return this.keys(this.eventPattern('*', eventId));
    }

    events() {
        return this.keys(this.eventPattern()).map(key => [key, this.get(key)]);
    }

    getEvent(eventId) {
        return this.findEventKeys(eventId).map(key => this.get(key));
    }

    deleteEvent(eventId) {
        const keys = this.findEventKeys(eventId);
        for (const key of keys) {
            this.delete(key);
        }
        return keys.length;
    }

    setEvent(eventName, callback, id) {
        const eventId = id ?? uuidv4();
        this.set(this.eventKey(eventName, eventId), callback);
        return eventId;
    }

    dispatchEvent(eventName, ...args) {
        for (const key of this.keys(this.eventPattern(eventName, '*'))) {
            const entry = this.get(key);
            
            if (typeof entry === 'function') {
                try {
                    entry(...args);
                } catch {
                    // swallow callback errors
                }
            }
        }
    }
}

export class MessageQueueController extends MemoryLimitedDictStorageController {
    static ROOT_KEY = '_MessageQueue';
    static ROOT_KEY_EVENT = 'MQE';

    constructor(
        model,
        maxMemoryMb = 1024,
        policy = 'lru',
        onEvict = () => undefined,
        pinned = [],
        dispatcher
    ) {
        super(model, maxMemoryMb, policy, onEvict, pinned);
        this.counters = new Map();
        this.dispatcher = dispatcher ?? new EventDispatcherController(model);
    }

    queueKey(queue, index) {
        return `${MessageQueueController.ROOT_KEY}:${queue}:${index}`;
    }

    static extractIndex(key) {
        const part = key.split(':').pop();
        const idx = part ? Number(part) : NaN;
        return Number.isFinite(idx) ? idx : NaN;
    }

    ensureCounter(queue) {
        if (this.counters.has(queue)) return;
        const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queue}:*`);
        let maxIdx = -1;
        for (const key of keys) {
            const idx = MessageQueueController.extractIndex(key);
            if (!Number.isNaN(idx)) {
                maxIdx = Math.max(maxIdx, idx);
            }
        }
        this.counters.set(queue, maxIdx < 0 ? 0 : maxIdx + 1);
    }

    nextIndex(queue) {
        this.ensureCounter(queue);
        const idx = this.counters.get(queue) ?? 0;
        this.counters.set(queue, idx + 1);
        return idx;
    }

    eventName(queueName, kind) {
        return `${MessageQueueController.ROOT_KEY_EVENT}:${queueName}:${kind}`;
    }

    addListener(queueName, callback, eventName = 'pushed', listenerId) {
        return this.dispatcher.setEvent(this.eventName(queueName, eventName), callback, listenerId);
    }

    tryDispatchEvent(queueName, kind, key, message) {
        try {
            const opMap = {
                pushed: 'push',
                popped: 'pop',
                empty: 'empty',
                cleared: 'clear'
            };
            this.dispatcher.dispatchEvent(
                this.eventName(queueName, kind),
                { queue: queueName, key, message, op: opMap[kind] }
            );
        } catch {
            // ignore listener failures
        }
    }

    removeListener(listenerId) {
        return this.dispatcher.deleteEvent(listenerId);
    }

    listListeners(queueName, event) {
        const events = this.dispatcher.events();
        if (!queueName && !event) {
            return events.filter(([, cb]) => typeof cb === 'function');
        }
        const out = [];
        for (const [key, cb] of events) {
            if (typeof cb !== 'function') continue;
            const parts = key.split(':');
            if (parts.length !== 5) continue;
            const [, root, qn, kind] = parts;
            if (root !== MessageQueueController.ROOT_KEY_EVENT) continue;
            if (queueName && qn !== queueName) continue;
            if (event && kind !== event) continue;
            out.push([key, cb]);
        }
        return out;
    }

    push(message, queueName = 'default') {
        const key = this.queueKey(queueName, this.nextIndex(queueName));
        this.set(key, message);
        this.tryDispatchEvent(queueName, 'pushed', key, message);
        return key;
    }

    earliestKey(queueName) {
        const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`);
        const candidates = keys
            .map(key => ({ key, idx: MessageQueueController.extractIndex(key) }))
            .filter(item => !Number.isNaN(item.idx));
        if (!candidates.length) return null;
        candidates.sort((a, b) => a.idx - b.idx);
        return candidates[0].key;
    }

    pop(queueName = 'default') {
        const [, message] = this.popItem(queueName);
        return message;
    }

    peek(queueName = 'default') {
        const [, message] = this.popItem(queueName, true);
        return message;
    }

    popItem(queueName = 'default', peek = false) {
        const key = this.earliestKey(queueName);
        if (!key) return [null, null];
        const message = this.get(key);
        if (peek) return [key, message];
        this.delete(key);
        this.tryDispatchEvent(queueName, 'popped', key, message);
        if (this.queueSize(queueName) === 0) {
            this.tryDispatchEvent(queueName, 'empty', null, null);
        }
        return [key, message];
    }

    queueSize(queueName = 'default') {
        return this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).length;
    }

    clear(queueName = 'default') {
        for (const key of this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`)) {
            this.delete(key);
        }
        this.counters.delete(queueName);
        this.tryDispatchEvent(queueName, 'cleared', null, null);
    }

    listQueues() {
        const queues = new Set();
        for (const key of this.keys(`${MessageQueueController.ROOT_KEY}:*`)) {
            const parts = key.split(':');
            if (parts.length !== 3) continue;
            const [root, queue] = parts;
            queues.add(`${root}:${queue}`);
        }
        return Array.from(queues).sort();
    }
}

export class LocalVersionController {
    static TABLENAME = '_Operation';
    static KEY = 'ops';
    static FORWARD = 'forward';
    static REVERT = 'revert';

    constructor(
        client = null,
        limitMemoryMB = 128,
        evictionPolicy = 'fifo'
    ) {
        this.limitMemoryMB = Number(limitMemoryMB);
        if (client) {
            this.client = client;
        } else {
            const model = new DictStorage();
            this.client = new MemoryLimitedDictStorageController(
                model,
                this.limitMemoryMB,
                evictionPolicy,
                this._onEvict.bind(this),
                [LocalVersionController.TABLENAME]
            );
        }
        const table = this.client.get(LocalVersionController.TABLENAME);
        if (!table || !(LocalVersionController.KEY in table)) {
            this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [] });
        }
        this._currentVersion = null;
    }

    _onEvict(key, _value) {
        const prefix = `${LocalVersionController.TABLENAME}:`;
        if (!key.startsWith(prefix)) return;
        const opId = key.slice(prefix.length);
        const ops = this.getVersions();
        const idx = ops.indexOf(opId);
        if (idx >= 0) {
            ops.splice(idx, 1);
            this._setVersions(ops);
        }
        if (this._currentVersion === opId) {
            throw new Error('auto removed current_version');
        }
    }

    getVersions() {
        const table = this.client.get(LocalVersionController.TABLENAME);
        const ops = table?.[LocalVersionController.KEY];
        return Array.isArray(ops) ? [...ops] : [];
    }

    _setVersions(ops) {
        this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [...ops] });
    }

    findVersion(versionUuid) {
        const versions = this.getVersions();
        const currentIdx = this._currentVersion ? versions.indexOf(this._currentVersion) : -1;
        const targetIdx = versionUuid && versions.includes(versionUuid) ? versions.indexOf(versionUuid) : null;
        let op = null;
        if (targetIdx !== null) {
            const opId = versions[targetIdx];
            op = this.client.get(`${LocalVersionController.TABLENAME}:${opId}`);
        }
        return [versions, currentIdx, targetIdx, op];
    }

    estimateMemoryMB() {
        const bytes = Number(this.client.bytesUsed(true, false));
        return bytes / (1024 * 1024);
    }

    addOperation(operation, revert = null, verbose=false) {
        const opuuid = uuidv4();
        this.client.set(
            `${LocalVersionController.TABLENAME}:${opuuid}`,
            { [LocalVersionController.FORWARD]: operation, [LocalVersionController.REVERT]: revert }
        );

        let ops = this.getVersions();
        if (this._currentVersion && ops.includes(this._currentVersion)) {
            const idx = ops.indexOf(this._currentVersion);
            ops = ops.slice(0, idx + 1);
        }
        ops.push(opuuid);
        this._setVersions(ops);
        this._currentVersion = opuuid;

        if (this.estimateMemoryMB() > this.limitMemoryMB) {
            const warning = `[LocalVersionController] Warning: memory usage ${this.estimateMemoryMB().toFixed(1)} MB exceeds limit of ${this.limitMemoryMB} MB`;
            verbose??console.warn(warning);
            return warning;
        }
        return null;
    }

    popOperation(n = 1) {
        if (n <= 0) return [];
        const ops = this.getVersions();
        if (!ops.length) return [];
        const popped = [];
        const count = Math.min(n, ops.length);
        for (let i = 0; i < count; i += 1) {
            const popIdx = ops.length && ops[0] !== this._currentVersion ? 0 : ops.length - 1;
            const opId = ops[popIdx];
            const opKey = `${LocalVersionController.TABLENAME}:${opId}`;
            const opRecord = this.client.get(opKey);
            popped.push([opId, opRecord]);
            ops.splice(popIdx, 1);
            this.client.delete(opKey);
        }
        this._setVersions(ops);
        if (!this._currentVersion || !ops.includes(this._currentVersion)) {
            this._currentVersion = ops.length ? ops[ops.length - 1] : null;
        }
        return popped;
    }

    forwardOneOperation(forwardCallback) {
        const [versions, currentIdx] = this.findVersion(this._currentVersion);
        const nextIdx = currentIdx + 1;
        if (nextIdx >= versions.length) return;
        const op = this.client.get(`${LocalVersionController.TABLENAME}:${versions[nextIdx]}`);
        if (!op || !(LocalVersionController.FORWARD in op)) return;
        forwardCallback(op[LocalVersionController.FORWARD]);
        this._currentVersion = versions[nextIdx];
    }

    revertOneOperation(revertCallback) {
        const [versions, currentIdx, , op] = this.findVersion(this._currentVersion);
        if (currentIdx <= 0) return;
        if (!op || !(LocalVersionController.REVERT in op)) return;
        revertCallback(op[LocalVersionController.REVERT]);
        this._currentVersion = currentIdx > 0 ? versions[currentIdx - 1] : null;
    }

    toVersion(versionUuid, versionCallback) {
        let [versions, currentIdx, targetIdx] = this.findVersion(versionUuid);
        if (targetIdx === null) {
            throw new Error(`no such version of ${versionUuid}`);
        }
        while (currentIdx !== targetIdx) {
            if (currentIdx < targetIdx) {
                this.forwardOneOperation(versionCallback);
                currentIdx += 1;
            } else {
                this.revertOneOperation(op => {
                    if (op) versionCallback(op);
                });
                currentIdx -= 1;
                versions = this.getVersions();
                targetIdx = versions.indexOf(versionUuid);
            }
        }
    }

    getCurrentVersion() {
        return this._currentVersion;
    }
}

export class SingletonKeyValueStorage {
    constructor(versionControl = false, encryptor) {
        this.versionControl = versionControl;
        this.encryptor = encryptor;
        this.switchBackend(DictStorage.build());
    }

    switchBackend(controller) {
        this.eventDispatcher = new EventDispatcherController(new DictStorage());
        this.versionController = new LocalVersionController();
        this.messageQueue = new MessageQueueController(new DictStorage());
        this.conn = controller;
        return this;
    }

    log(message) {
        console.log(`[SingletonKeyValueStorage]: ${message instanceof Error ? message.message : message}`);
    }

    deleteSlave(slave) {
        const id = slave?.uuid ?? null;
        return id ? this.deleteEvent(id) : 0;
    }

    addSlave(slave, eventNames = ['set', 'delete']) {
        if (!slave) return false;
        if (!slave.uuid) {
            try {
                slave.uuid = uuidv4();
            } catch (error) {
                this.log(`can not set uuid to ${slave}. Skip this slave.`);
                return false;
            }
        }
        for (const name of eventNames) {
            const fn = (...args)=>slave[name](...args);
            if (typeof fn === 'function') {
                this.setEvent(name, fn, slave.uuid);
            } else {
                this.log(`no func of "${name}" in ${slave}. Skip it.`);
            }
        }
        return true;
    }

    editLocal(funcName, key, value) {
        if (!['set', 'delete', 'clean', 'load', 'loads'].includes(funcName)) {
            throw new Error(`no func of "${funcName}". return.`);
        }
        const fn = this.conn[funcName];
        if (typeof fn !== 'function') {
            throw new Error(`no func of "${funcName}"`);
        }
        const args = [key, value].filter(arg => arg !== undefined);
        return fn.apply(this.conn, args);
    }

    edit(funcName, key, value) {
        const argsForEvent = [key, value].filter(arg => arg !== undefined);
        let payload = value;
        if (this.encryptor && funcName === 'set' && value !== undefined) {
            payload = { rjson: this.encryptor.encryptString(JSON.stringify(value)) };
        }
        const result = this.editLocal(funcName, key, payload);
        this.dispatchEvent(funcName, ...argsForEvent);
        return result;
    }

    tryEdit(operation) {
        if (this.versionControl) {
            const [func, key] = operation;
            let revert = null;
            if (func === 'set' && typeof key === 'string') {
                if (this.exists(key)) {
                    revert = ['set', key, this.get(key)];
                } else {
                    revert = ['delete', key];
                }
            } else if (func === 'delete' && typeof key === 'string') {
                revert = ['set', key, this.get(key)];
            } else if (func === 'clean' || func === 'load' || func === 'loads') {
                revert = ['loads', this.dumps()];
            }
            if (revert) {
                this.versionController.addOperation(operation, revert);
            }
        }
        try {
            this.edit(operation[0], operation[1], operation[2]);
            return true;
        } catch (error) {
            this.log(error);
            return false;
        }
    }

    tryLoad(fn) {
        try {
            return fn();
        } catch (error) {
            this.log(error);
            return null;
        }
    }

    revertOneOperation() {
        this.versionController.revertOneOperation(op => {
            if (!op) return;
            const [func, key, value] = op;
            this.editLocal(func, key, value);
        });
    }

    forwardOneOperation() {
        this.versionController.forwardOneOperation(op => {
            const [func, key, value] = op;
            this.editLocal(func, key, value);
        });
    }

    getCurrentVersion() {
        return this.versionController.getCurrentVersion();
    }

    localToVersion(opuuid) {
        this.versionController.toVersion(opuuid, op => {
            const [func, key, value] = op;
            this.editLocal(func, key, value);
        });
    }

    set(key, value) {
        return this.tryEdit(['set', key, value]);
    }

    delete(key) {
        return this.tryEdit(['delete', key]);
    }

    clean() {
        return this.tryEdit(['clean']);
    }

    load(jsonPath) {
        return this.tryEdit(['load', jsonPath]);
    }

    loads(jsonString) {
        return this.tryEdit(['loads', jsonString]);
    }

    exists(key) {
        return this.tryLoad(() => this.conn.exists(key));
    }

    keys(pattern = '*') {
        return this.tryLoad(() => this.conn.keys(pattern));
    }

    get(key) {
        const value = this.tryLoad(() => this.conn.get(key));
        if (value && this.encryptor && typeof value === 'object' && 'rjson' in value) {
            return this.tryLoad(() => JSON.parse(this.encryptor.decryptString(value.rjson)));
        }
        return value;
    }

    dumps() {
        return this.tryLoad(() => {
            const output = {};
            const keys = this.conn.keys('*') || [];
            for (const key of keys) {
                output[key] = this.get(key);
            }
            return JSON.stringify(output);
        });
    }

    dump(jsonPath) {
        return this.tryLoad(() => this.conn.dump(jsonPath));
    }

    events() {
        return this.eventDispatcher.events();
    }

    getEvent(uuid) {
        return this.eventDispatcher.getEvent(uuid);
    }

    deleteEvent(uuid) {
        return this.eventDispatcher.deleteEvent(uuid);
    }

    setEvent(eventName, callback, id) {
        return this.eventDispatcher.setEvent(eventName, callback, id);
    }

    dispatchEvent(eventName, ...args) {
        this.eventDispatcher.dispatchEvent(eventName, ...args);
    }

    cleanEvents() {
        this.eventDispatcher.clean();
    }
}
