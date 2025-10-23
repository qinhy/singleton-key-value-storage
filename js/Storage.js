import * as fs from 'fs';
import { randomBytes } from 'crypto';
import { Buffer } from 'buffer';
import { PEMFileReader, SimpleRSAChunkEncryptor } from './RSA.js';

/** @typedef {any} StoreValue */
/** @typedef {Record<string, StoreValue>} StoreRecord */
/** @typedef {[string, ...any[]]} Operation */

function getGlobalCrypto() {
    if (typeof globalThis === 'undefined') return null;
    const maybeCrypto = globalThis.crypto ?? globalThis.webcrypto;
    if (maybeCrypto && typeof maybeCrypto.getRandomValues === 'function') {
        return /** @type {Crypto} */ (maybeCrypto);
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

    const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
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
        default:
            return 0;
    }
}

function getDeepBytesSize(obj, seen = new WeakSet(), shallow = false) {
    if (obj === null || obj === undefined) return 0;

    if (typeof obj !== 'object') {
        return sizeOfPrimitive(obj);
    }

    if (seen.has(obj)) return 0;
    seen.add(obj);

    if (Buffer.isBuffer(obj)) return obj.length;
    if (obj instanceof ArrayBuffer) return obj.byteLength;
    if (ArrayBuffer.isView(obj)) return obj.byteLength;
    if (obj instanceof Date) return 8;
    if (obj instanceof RegExp) return Buffer.byteLength(obj.source, 'utf8');

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

    if (Array.isArray(obj)) {
        if (shallow) return obj.length * POINTER_SIZE_BYTES;
        let total = 0;
        for (const item of obj) {
            total += getDeepBytesSize(item, seen, false);
        }
        return total;
    }

    let total = 0;
    const props = Object.getOwnPropertyNames(obj);
    const symbols = Object.getOwnPropertySymbols(obj);

    const visit = (descriptor, currentValue) => {
        if (!descriptor) return;
        try {
            let value;
            if (descriptor.get && !descriptor.set) {
                value = descriptor.get.call(obj);
            } else if ('value' in descriptor) {
                value = currentValue;
            } else {
                value = undefined;
            }

            if (shallow) {
                total += typeof value === 'object' && value !== null ? POINTER_SIZE_BYTES : sizeOfPrimitive(value);
            } else {
                total += getDeepBytesSize(value, seen, false);
            }
        } catch {
            /* ignore accessor errors */
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
        this.uuid = id ?? uuidv4();
        this.store = store ?? null;
        this.isSingleton = isSingleton ?? false;
    }

    getSingleton() {
        return new AbstractStorage(AbstractStorage._uuid, AbstractStorage._store, AbstractStorage._is_singleton);
    }

    bytesUsed() {
        throw new Error('Subclasses must implement memoryUsage method');
    }
}

export class DictStorage extends AbstractStorage {
    static _uuid = uuidv4();
    static _store = {};

    constructor(id = null, store = null, isSingleton = null) {
        super(id, store, isSingleton);
        this.store = store ?? {};
    }

    bytesUsed(deep = true, humanReadable = true) {
        const size = deep ? getDeepBytesSize(this) : getShallowBytesSize(this);
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
        return Boolean(this.model.isSingleton);
    }

    bytesUsed(deep = true, humanReadable = true) {
        return this.model.bytesUsed(deep, humanReadable);
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
        /** @type {StoreRecord} */
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

export class EventDispatcherController extends DictStorageController {
    static ROOT_KEY = 'Event';

    findEvent(uuid) {
        const matches = this.keys(`*:${uuid}`);
        return matches.length ? matches : [];
    }

    events() {
        return this.keys('*').map(key => [key, this.get(key)]);
    }

    getEvent(uuid) {
        return this.findEvent(uuid).map(key => this.get(key));
    }

    deleteEvent(uuid) {
        for (const key of this.findEvent(uuid)) {
            this.delete(key);
        }
    }

    setEvent(eventName, callback, id) {
        const eventId = id ?? uuidv4();
        this.set(`${EventDispatcherController.ROOT_KEY}:${eventName}:${eventId}`, callback);
        return eventId;
    }

    dispatchEvent(eventName, ...args) {
        for (const key of this.keys(`${EventDispatcherController.ROOT_KEY}:${eventName}:*`)) {
            const entry = this.get(key);
            if (typeof entry === 'function') {
                entry(...args);
            }
        }
    }
}

export class MessageQueueController extends DictStorageController {
    static ROOT_KEY = '_MessageQueue';
    counters = new Map();

    getQueueKey(queueName, index) {
        return `${MessageQueueController.ROOT_KEY}:${queueName}:${index}`;
    }

    getQueueCounter(queueName) {
        const current = this.counters.get(queueName) ?? 0;
        this.counters.set(queueName, current);
        return current;
    }

    incrementQueueCounter(queueName) {
        this.counters.set(queueName, this.getQueueCounter(queueName) + 1);
    }

    listQueueKeys(queueName) {
        const pattern = `${MessageQueueController.ROOT_KEY}:${queueName}:*`;
        const parseIndex = key => {
            const parts = key.split(':');
            return Number(parts[parts.length - 1]) || 0;
        };
        return this.keys(pattern).sort((a, b) => parseIndex(a) - parseIndex(b));
    }

    push(message, queueName = 'default') {
        const counter = this.getQueueCounter(queueName);
        const key = this.getQueueKey(queueName, counter);
        this.set(key, message);
        this.incrementQueueCounter(queueName);
        return key;
    }

    pop(queueName = 'default') {
        const keys = this.listQueueKeys(queueName);
        if (!keys.length) return null;
        const earliestKey = keys[0];
        const message = this.get(earliestKey);
        if (message !== null) {
            this.delete(earliestKey);
        }
        return message;
    }

    peek(queueName = 'default') {
        const keys = this.listQueueKeys(queueName);
        if (!keys.length) return null;
        return this.get(keys[0]);
    }

    size(queueName = 'default') {
        return this.listQueueKeys(queueName).length;
    }

    clear(queueName = 'default') {
        for (const key of this.listQueueKeys(queueName)) {
            this.delete(key);
        }
        this.counters.delete(queueName);
    }
}

export class MemoryLimitedDictStorageController extends DictStorageController {
    constructor(
        model,
        maxMemoryMb = 1024,
        policy = 'lru',
        onEvict = undefined,
        pinned = new Set(),
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
        this.sizes.delete(key);
        this.currentBytes = Math.max(0, this.currentBytes - tracked);
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
            const value = super.get(victim);
            this.reduce(victim);
            super.delete(victim);
            if (this.onEvict) {
                this.onEvict(victim, value);
            }
        }
    }

    set(key, value) {
        if (this.exists(key)) {
            this.reduce(key);
        }
        super.set(key, value);

        const size = this.entrySize(key, value);
        this.sizes.set(key, size);
        this.currentBytes += size;

        this.order.delete(key);
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
        for (const key of Array.from(this.order.keys())) {
            super.delete(key);
        }
        this.sizes.clear();
        this.order.clear();
        this.currentBytes = 0;
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
        evictionPolicy = 'fifo',
    ) {
        this.limitMemoryMB = limitMemoryMB;
        this.evictionPolicy = evictionPolicy;
        this._currentVersion = null;

        if (client) {
            this.client = client;
        } else {
            const model = new DictStorage();
            this.client = new MemoryLimitedDictStorageController(
                model,
                this.limitMemoryMB,
                this.evictionPolicy,
                this.onEvict.bind(this),
                new Set([LocalVersionController.TABLENAME]),
            );
        }

        const table = this.client.get(LocalVersionController.TABLENAME) ?? {};
        if (!Array.isArray((table ?? {})[LocalVersionController.KEY])) {
            this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [] });
        }
    }

    onEvict(key, _value) {
        const prefix = `${LocalVersionController.TABLENAME}:`;
        if (!key.startsWith(prefix)) return;

        const opId = key.slice(prefix.length);
        const versions = this.getVersions();
        if (versions.includes(opId)) {
            const updated = versions.filter(v => v !== opId);
            this.setVersions(updated);
        }

        if (this._currentVersion === opId) {
            throw new Error('auto removed current_version');
        }
    }

    getCurrentVersion() {
        return this._currentVersion;
    }

    getVersions() {
        try {
            const table = this.client.get(LocalVersionController.TABLENAME) ?? {};
            const ops = table?.[LocalVersionController.KEY];
            return Array.isArray(ops) ? [...ops] : [];
        } catch {
            return [];
        }
    }

    setVersions(ops) {
        this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [...ops] });
    }

    findVersion(versionUuid) {
        const versions = this.getVersions();
        const currentIdx = this._currentVersion && versions.includes(this._currentVersion)
            ? versions.indexOf(this._currentVersion)
            : -1;
        const targetIdx = versionUuid && versions.includes(versionUuid)
            ? versions.indexOf(versionUuid)
            : null;
        const op = targetIdx !== null ? this.client.get(`${LocalVersionController.TABLENAME}:${versions[targetIdx]}`) : null;
        return [versions, currentIdx, targetIdx, op];
    }

    estimateMemoryMB() {
        const raw = this.client.bytesUsed(true, false);
        if (typeof raw === 'number') {
            return raw / (1024 * 1024);
        }
        return 0;
    }

    addOperation(operation, revert = null) {
        const opUuid = uuidv4();
        this.client.set(`${LocalVersionController.TABLENAME}:${opUuid}`, {
            [LocalVersionController.FORWARD]: operation,
            [LocalVersionController.REVERT]: revert,
        });

        let ops = this.getVersions();
        if (this._currentVersion && ops.includes(this._currentVersion)) {
            const idx = ops.indexOf(this._currentVersion);
            ops = ops.slice(0, idx + 1);
        }
        ops.push(opUuid);
        this.setVersions(ops);
        this._currentVersion = opUuid;

        const usage = this.estimateMemoryMB();
        if (usage > this.limitMemoryMB) {
            const message = `[LocalVersionController] Warning: memory usage ${usage.toFixed(1)} MB exceeds limit of ${this.limitMemoryMB} MB`;
            console.log(message);
            return message;
        }
        return null;
    }

    popOperation(n = 1) {
        if (n <= 0) return [];

        const ops = this.getVersions();
        if (!ops.length) return [];

        const popped = [];
        for (let i = 0; i < Math.min(n, ops.length); i += 1) {
            const popIdx = ops.length && ops[0] !== this._currentVersion ? 0 : ops.length - 1;
            const opId = ops[popIdx];
            const opKey = `${LocalVersionController.TABLENAME}:${opId}`;
            const opRecord = this.client.get(opKey);
            popped.push([opId, opRecord]);

            ops.splice(popIdx, 1);
            this.client.delete(opKey);
        }

        this.setVersions(ops);
        if (!ops.includes(this._currentVersion ?? '')) {
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
        this._currentVersion = versions[currentIdx - 1];
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
}

export class SingletonKeyValueStorage {
    constructor(versionControl = false, encryptor = undefined) {
        this.versionControl = versionControl;
        this.encryptor = encryptor;
        this.switchBackend(DictStorage.build());
    }

    switchBackend(controller) {
        this.eventDispatcher = new EventDispatcherController(new DictStorage());
        this.messageQueue = new MessageQueueController(new DictStorage());
        this.versionController = new LocalVersionController();
        this.conn = controller;
        return this;
    }

    log(message) {
        console.log(`[SingletonKeyValueStorage]: ${message instanceof Error ? message.message : message}`);
    }

    editLocal(funcName, key, value) {
        if (!['set', 'delete', 'clean', 'load', 'loads'].includes(funcName)) {
            throw new Error(`no func of "${funcName}". return.`);
        }
        const fn = this.conn?.[funcName];
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
        this.versionController.revertOneOperation(revert => {
            if (!revert) return;
            const [func, key, value] = revert;
            this.editLocal(func, key, value);
        });
    }

    forwardOneOperation() {
        this.versionController.forwardOneOperation(forward => {
            const [func, key, value] = forward;
            this.editLocal(func, key, value);
        });
    }

    getCurrentVersion() {
        return this.versionController.getCurrentVersion();
    }

    localToVersion(opUuid) {
        this.versionController.toVersion(opUuid, op => {
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

    loads(jsonStr) {
        return this.tryEdit(['loads', jsonStr]);
    }

    exists(key) {
        const result = this.tryLoad(() => this.conn.exists(key));
        return Boolean(result);
    }

    keys(pattern = '*') {
        return this.tryLoad(() => this.conn.keys(pattern)) ?? [];
    }

    get(key) {
        const value = this.tryLoad(() => this.conn.get(key));
        if (!value) return null;

        if (this.encryptor && typeof value === 'object' && value && 'rjson' in value) {
            try {
                const decrypted = this.encryptor.decryptString(value.rjson);
                return JSON.parse(decrypted);
            } catch (error) {
                this.log(error);
                return null;
            }
        }

        return value;
    }

    dumps() {
        return this.tryLoad(() => {
            /** @type {StoreRecord} */
            const snapshot = {};
            for (const key of this.conn.keys('*')) {
                snapshot[key] = this.get(key);
            }
            return JSON.stringify(snapshot);
        }) ?? '{}';
    }

    dump(jsonPath) {
        this.tryLoad(() => this.conn.dump(jsonPath));
    }

    dumpRSA(path, publicPkcs8KeyPath) {
        this.tryLoad(() => this.conn.dumpRSA(path, publicPkcs8KeyPath));
    }

    loadRSA(path, privatePkcs8KeyPath) {
        this.tryLoad(() => this.conn.loadRSA(path, privatePkcs8KeyPath));
    }

    events() {
        return this.eventDispatcher.events();
    }

    getEvent(uuid) {
        return this.eventDispatcher.getEvent(uuid);
    }

    deleteEvent(uuid) {
        this.eventDispatcher.deleteEvent(uuid);
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

    addSlave(slave, eventNames = ['set', 'delete']) {
        if (!slave) return;
        if (!slave.uuid) {
            try {
                slave.uuid = uuidv4();
            } catch (error) {
                this.log(`can not set uuid to ${slave}. Skip this slave.`);
                return;
            }
        }

        for (const event of eventNames) {
            if (typeof slave[event] === 'function') {
                this.setEvent(event, slave[event].bind(slave), slave.uuid);
            } else {
                this.log(`no func of "${event}" in ${slave}. Skip it.`);
            }
        }
    }

    deleteSlave(slave) {
        if (slave?.uuid) {
            this.deleteEvent(slave.uuid);
        }
    }
}
