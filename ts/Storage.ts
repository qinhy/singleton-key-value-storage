import * as fs from 'fs';
import { randomBytes } from 'crypto';
import { PEMFileReader, SimpleRSAChunkEncryptor } from './RSA';

export type StoreValue = any;
export type StoreRecord = Record<string, StoreValue>;
export type Operation = [string, ...any[]];

type EventCallback = (...args: any[]) => unknown;

type EvictHandler = (key: string, value: StoreValue | null) => void;

function getGlobalCrypto(): Crypto | null {
    if (typeof globalThis === 'undefined') return null;
    const maybeCrypto = (globalThis as any).crypto ?? (globalThis as any).webcrypto;
    if (maybeCrypto && typeof maybeCrypto.getRandomValues === 'function') {
        return maybeCrypto as Crypto;
    }
    return null;
}

export function uuidv4(): string {
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

function sizeOfPrimitive(value: unknown): number {
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

function getDeepBytesSize(obj: any, seen: WeakSet<object> = new WeakSet(), shallow = false): number {
    if (obj === null || obj === undefined) return 0;
    if (typeof obj !== 'object') return sizeOfPrimitive(obj);
    if (seen.has(obj)) return 0;
    seen.add(obj);

    if (Buffer.isBuffer(obj)) return obj.length;
    if (obj instanceof ArrayBuffer) return obj.byteLength;
    if (ArrayBuffer.isView(obj)) return (obj as ArrayBufferView).byteLength;
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

    const visit = (descriptor: PropertyDescriptor | undefined, value: any) => {
        if (!descriptor) return;
        try {
            let resolved: any;
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
        visit(descriptor, (obj as any)[key]);
    }

    for (const sym of symbols) {
        const descriptor = Object.getOwnPropertyDescriptor(obj, sym);
        visit(descriptor, (obj as any)[sym as any]);
    }

    return total;
}

const getShallowBytesSize = (obj: any): number => getDeepBytesSize(obj, new WeakSet(), true);

export function humanizeBytes(n: number): string {
    let size = Number(n);
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    for (const unit of units) {
        if (size < 1024) return `${size.toFixed(1)} ${unit}`;
        size /= 1024;
    }
    return `${size.toFixed(1)} PB`;
}

function globToRegExp(pattern: string): RegExp {
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    return new RegExp('^' + escaped.replace(/\*/g, '.*').replace(/\?/g, '.') + '$');
}

interface AbstractStorageStatic<T extends AbstractStorage> {
    new (id?: string | null, store?: any, isSingleton?: boolean | null): T;
    _uuid: string;
    _store: any;
    _is_singleton: boolean;
    _meta: Record<string, any>;
}

export abstract class AbstractStorage {
    static _uuid: string = uuidv4();
    static _store: any = null;
    static _is_singleton = true;
    static _meta: Record<string, any> = {};

    uuid: string;
    store: any;
    isSingleton: boolean;

    constructor(id: string | null = null, store: any = null, isSingleton: boolean | null = null) {
        this.uuid = id ?? uuidv4();
        this.store = store ?? null;
        this.isSingleton = isSingleton ?? false;
    }

    getSingleton<T extends AbstractStorage>(this: T): T {
        const ctor = this.constructor as AbstractStorageStatic<T>;
        return new ctor(ctor._uuid, ctor._store, ctor._is_singleton);
    }

    abstract bytesUsed(deep?: boolean, humanReadable?: boolean): number | string;
}

export class DictStorage extends AbstractStorage {
    static override _uuid: string = uuidv4();
    static override _store: StoreRecord = {};

    constructor(id: string | null = null, store: StoreRecord | null = null, isSingleton: boolean | null = null) {
        super(id, store ?? {}, isSingleton);
        this.store = store ?? {};
    }

    override bytesUsed(deep: boolean = true, humanReadable: boolean = true): number | string {
        const size = deep ? getDeepBytesSize(this.store) : getShallowBytesSize(this.store);
        return humanReadable ? humanizeBytes(size) : size;
    }

    static buildTmp(): DictStorageController {
        return new DictStorageController(new DictStorage());
    }

    static build(): DictStorageController {
        return new DictStorageController(new DictStorage().getSingleton());
    }
}

export abstract class AbstractStorageController {
    protected model: AbstractStorage;

    constructor(model: AbstractStorage) {
        this.model = model;
    }

    isSingleton(): boolean {
        return Boolean(this.model?.isSingleton);
    }

    bytesUsed(deep: boolean = true, humanReadable: boolean = true): number | string {
        if (typeof this.model.bytesUsed === 'function') {
            return this.model.bytesUsed(deep, humanReadable);
        }
        return humanReadable ? humanizeBytes(0) : 0;
    }

    exists(_key: string): boolean {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    set(_key: string, _value: StoreValue): void {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    get(_key: string): StoreValue | null {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    delete(_key: string): void {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    keys(_pattern: string = '*'): string[] {
        throw new Error(`[${this.constructor.name}]: not implemented`);
    }

    clean(): void {
        for (const key of this.keys('*')) {
            this.delete(key);
        }
    }

    dumps(): string {
        const snapshot: StoreRecord = {};
        for (const key of this.keys('*')) {
            snapshot[key] = this.get(key);
        }
        return JSON.stringify(snapshot);
    }

    loads(jsonString: string = '{}'): void {
        const data = JSON.parse(jsonString) as StoreRecord;
        for (const [key, value] of Object.entries(data)) {
            this.set(key, value);
        }
    }

    dump(path: string): void {
        fs.writeFileSync(path, this.dumps(), 'utf8');
    }

    load(path: string): void {
        const text = fs.readFileSync(path, 'utf8');
        this.loads(text);
    }

    dumpRSA(path: string, publicPkcs8KeyPath: string): void {
        const encryptor = new SimpleRSAChunkEncryptor(
            new PEMFileReader(publicPkcs8KeyPath).loadPublicPkcs8Key(),
            undefined
        );
        fs.writeFileSync(path, encryptor.encryptString(this.dumps()), 'utf8');
    }

    loadRSA(path: string, privatePkcs8KeyPath: string): void {
        const encryptor = new SimpleRSAChunkEncryptor(
            undefined,
            new PEMFileReader(privatePkcs8KeyPath).loadPrivatePkcs8Key()
        );
        const decrypted = encryptor.decryptString(fs.readFileSync(path, 'utf8'));
        this.loads(decrypted);
    }
}

export class DictStorageController extends AbstractStorageController {
    protected store: StoreRecord;

    constructor(model: DictStorage) {
        super(model);
        this.store = (model.store ?? {}) as StoreRecord;
        model.store = this.store;
    }

    override exists(key: string): boolean {
        return Object.prototype.hasOwnProperty.call(this.store, key);
    }

    override set(key: string, value: StoreValue): void {
        this.store[key] = value;
    }

    override get(key: string): StoreValue | null {
        return this.exists(key) ? this.store[key] : null;
    }

    override delete(key: string): void {
        delete this.store[key];
    }

    override keys(pattern: string = '*'): string[] {
        const matcher = globToRegExp(pattern);
        return Object.keys(this.store).filter(key => matcher.test(key));
    }
}

export class MemoryLimitedDictStorageController extends DictStorageController {
    private maxBytes: number;
    private policy: 'lru' | 'fifo';
    private onEvict: EvictHandler;
    private pinned: Set<string>;
    private sizes: Map<string, number>;
    private order: Map<string, null>;
    private currentBytes: number;

    constructor(
        model: DictStorage,
        maxMemoryMb: number = 1024,
        policy: 'lru' | 'fifo' = 'lru',
        onEvict: EvictHandler = () => undefined,
        pinned: Iterable<string> = []
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

    private entrySize(key: string, value: StoreValue): number {
        return getDeepBytesSize(key) + getDeepBytesSize(value);
    }

    override bytesUsed(_deep: boolean = true, humanReadable: boolean = false): number | string {
        return humanReadable ? humanizeBytes(this.currentBytes) : this.currentBytes;
    }

    private reduce(key: string): void {
        if (this.order.has(key)) {
            this.order.delete(key);
        }
        const tracked = this.sizes.get(key) ?? 0;
        if (tracked) {
            this.currentBytes = Math.max(0, this.currentBytes - tracked);
        }
        this.sizes.delete(key);
    }

    private pickVictim(): string | null {
        for (const key of this.order.keys()) {
            if (!this.pinned.has(key)) {
                return key;
            }
        }
        return null;
    }

    private maybeEvict(): void {
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

    override set(key: string, value: StoreValue): void {
        const existed = this.exists(key);
        if (existed) {
            this.reduce(key);
        }
        super.set(key, value);

        const size = this.entrySize(key, value);
        this.sizes.set(key, size);
        this.currentBytes += size;

        if (this.policy === 'lru') {
        if (this.order.has(key)) {
            this.order.delete(key);
        }
        this.order.set(key, null);
        } else {
            if (this.order.has(key)) this.order.delete(key);
            this.order.set(key, null);
        }

        this.maybeEvict();
    }

    override get(key: string): StoreValue | null {
        const value = super.get(key);
        if (value !== null && this.policy === 'lru' && this.order.has(key)) {
            this.order.delete(key);
            this.order.set(key, null);
        }
        return value;
    }

    override delete(key: string): void {
        if (this.exists(key)) {
            this.reduce(key);
        }
        super.delete(key);
    }

    override clean(): void {
        super.clean();
        this.sizes.clear();
        this.order.clear();
        this.currentBytes = 0;
    }
}

export class EventDispatcherController extends DictStorageController {
    static ROOT_KEY = '_Event';

    private eventKey(eventName: string, eventId: string): string {
        return `${EventDispatcherController.ROOT_KEY}:${eventName}:${eventId}`;
    }

    private eventPattern(eventName: string = '*', eventId: string = '*'): string {
        return this.eventKey(eventName, eventId);
    }

    private findEventKeys(eventId: string): string[] {
        return this.keys(this.eventPattern('*', eventId));
    }

    events(): Array<[string, EventCallback | null]> {
        return this.keys(this.eventPattern()).map(key => [key, this.get(key) as EventCallback | null]);
    }

    getEvent(eventId: string): Array<EventCallback | null> {
        return this.findEventKeys(eventId).map(key => this.get(key) as EventCallback | null);
    }

    deleteEvent(eventId: string): number {
        const keys = this.findEventKeys(eventId);
        for (const key of keys) {
            this.delete(key);
        }
        return keys.length;
    }

    setEvent(eventName: string, callback: EventCallback, id?: string): string {
        const eventId = id ?? uuidv4();
        this.set(this.eventKey(eventName, eventId), callback);
        return eventId;
    }

    dispatchEvent(eventName: string, ...args: any[]): void {
        for (const key of this.keys(this.eventPattern(eventName, '*'))) {
            const entry = this.get(key);
            if (typeof entry === 'function') {
                try {
                    (entry as EventCallback)(...args);
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

    private counters: Map<string, number>;
    private dispatcher: EventDispatcherController;

    constructor(
        model: DictStorage,
        maxMemoryMb: number = 1024,
        policy: 'lru' | 'fifo' = 'lru',
        onEvict: EvictHandler = () => undefined,
        pinned: Iterable<string> = [],
        dispatcher?: EventDispatcherController
    ) {
        super(model, maxMemoryMb, policy, onEvict, pinned);
        this.counters = new Map();
        this.dispatcher = dispatcher ?? new EventDispatcherController(model);
    }

    private queueKey(queue: string, index: number): string {
        return `${MessageQueueController.ROOT_KEY}:${queue}:${index}`;
    }

    private static extractIndex(key: string): number {
        const part = key.split(':').pop();
        const idx = part ? Number(part) : NaN;
        return Number.isFinite(idx) ? idx : NaN;
    }

    private ensureCounter(queue: string): void {
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

    private nextIndex(queue: string): number {
        this.ensureCounter(queue);
        const idx = this.counters.get(queue) ?? 0;
        this.counters.set(queue, idx + 1);
        return idx;
    }

    private eventName(queueName: string, kind: string): string {
        return `${MessageQueueController.ROOT_KEY_EVENT}:${queueName}:${kind}`;
    }

    addListener(queueName: string, callback: EventCallback, event_name: string = 'pushed', listenerId?: string): string {
        return this.dispatcher.setEvent(this.eventName(queueName, event_name), callback, listenerId);
    }

    private tryDispatchEvent(queueName: string, kind: 'pushed' | 'popped' | 'empty' | 'cleared', 
                            key: string | null, message: StoreValue | null): void {
        try {
            const opMap: Record<'pushed' | 'popped' | 'empty' | 'cleared', string> = { 
                pushed: 'push', popped: 'pop', empty: 'empty', cleared: 'clear'
            };
            this.dispatcher.dispatchEvent(
                this.eventName(queueName, kind),
                { queue: queueName, key, message, op: opMap[kind] }
            );
        } catch {
            // ignore listener failures
        }
    }

    removeListener(listenerId: string): number {
        return this.dispatcher.deleteEvent(listenerId);
    }

    listListeners(queueName?: string, event?: string): Array<[string, EventCallback]> {
        const events = this.dispatcher.events();
        if (!queueName && !event) {
            return events.filter(([, cb]) => typeof cb === 'function') as Array<[string, EventCallback]>;
        }
        const out: Array<[string, EventCallback]> = [];
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

    push(message: StoreValue, queueName: string = 'default'): string {
        const key = this.queueKey(queueName, this.nextIndex(queueName));
        this.set(key, message);
        this.tryDispatchEvent(queueName, 'pushed', key, message);
        return key;
    }

    private earliestKey(queueName: string): string | null {
        const keys = this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`);
        const candidates = keys
            .map(key => ({ key, idx: MessageQueueController.extractIndex(key) }))
            .filter(item => !Number.isNaN(item.idx));
        if (!candidates.length) return null;
        candidates.sort((a, b) => a.idx - b.idx);
        return candidates[0].key;
    }

    pop(queueName: string = 'default'): StoreValue | null {
        const [, message] = this.popItem(queueName);
        return message;
    }

    peek(queueName: string = 'default'): StoreValue | null {
        const [, message] = this.popItem(queueName, true);
        return message;
    }

    popItem(queueName: string = 'default', peek: boolean = false): [string | null, StoreValue | null] {
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

    queueSize(queueName: string = 'default'): number {
        return this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`).length;
    }

    clear(queueName: string = 'default'): void {
        for (const key of this.keys(`${MessageQueueController.ROOT_KEY}:${queueName}:*`)) {
            this.delete(key);
        }
        this.counters.delete(queueName);
        this.tryDispatchEvent(queueName, 'cleared', null, null);
    }

    listQueues(): string[] {
        const queues = new Set<string>();
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

    readonly limitMemoryMB: number;
    readonly client: AbstractStorageController;
    private _currentVersion: string | null;

    constructor(
        client: AbstractStorageController | null = null,
        limitMemoryMB: number = 128,
        evictionPolicy: 'fifo' | 'lru' = 'fifo'
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
        const table = this.client.get(LocalVersionController.TABLENAME) as StoreRecord | null;
        if (!table || !(LocalVersionController.KEY in table)) {
            this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [] });
        }
        this._currentVersion = null;
    }

    private _onEvict(key: string, _value: StoreValue | null): void {
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

    getVersions(): string[] {
        const table = this.client.get(LocalVersionController.TABLENAME) as StoreRecord | null;
        const ops = table?.[LocalVersionController.KEY];
        return Array.isArray(ops) ? [...ops] : [];
    }

    private _setVersions(ops: string[]): void {
        this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: [...ops] });
    }

    findVersion(versionUuid: string | null): [string[], number, number | null, StoreRecord | null] {
        const versions = this.getVersions();
        const currentIdx = this._currentVersion ? versions.indexOf(this._currentVersion) : -1;
        const targetIdx = versionUuid && versions.includes(versionUuid) ? versions.indexOf(versionUuid) : null;
        let op: StoreRecord | null = null;
        if (targetIdx !== null) {
            const opId = versions[targetIdx];
            op = this.client.get(`${LocalVersionController.TABLENAME}:${opId}`) as StoreRecord | null;
        }
        return [versions, currentIdx, targetIdx, op];
    }

    estimateMemoryMB(): number {
        const bytes = Number(this.client.bytesUsed(true, false));
        return bytes / (1024 * 1024);
    }

    addOperation(operation: Operation, revert: Operation | null = null, verbose=false): string | null {
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

    popOperation(n: number = 1): Array<[string, StoreRecord | null]> {
        if (n <= 0) return [];
        const ops = this.getVersions();
        if (!ops.length) return [];
        const popped: Array<[string, StoreRecord | null]> = [];
        const count = Math.min(n, ops.length);
        for (let i = 0; i < count; i += 1) {
            const popIdx = ops.length && ops[0] !== this._currentVersion ? 0 : ops.length - 1;
            const opId = ops[popIdx];
            const opKey = `${LocalVersionController.TABLENAME}:${opId}`;
            const opRecord = this.client.get(opKey) as StoreRecord | null;
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

    forwardOneOperation(forwardCallback: (operation: Operation) => void): void {
        const [versions, currentIdx] = this.findVersion(this._currentVersion);
        const nextIdx = currentIdx + 1;
        if (nextIdx >= versions.length) return;
        const op = this.client.get(`${LocalVersionController.TABLENAME}:${versions[nextIdx]}`) as StoreRecord | null;
        if (!op || !(LocalVersionController.FORWARD in op)) return;
        forwardCallback(op[LocalVersionController.FORWARD] as Operation);
        this._currentVersion = versions[nextIdx];
    }

    revertOneOperation(revertCallback: (operation: Operation | null) => void): void {
        const [versions, currentIdx, , op] = this.findVersion(this._currentVersion);
        if (currentIdx <= 0) return;
        if (!op || !(LocalVersionController.REVERT in op)) return;
        revertCallback(op[LocalVersionController.REVERT] as Operation | null);
        this._currentVersion = currentIdx > 0 ? versions[currentIdx - 1] : null;
    }

    toVersion(versionUuid: string, versionCallback: (operation: Operation) => void): void {
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

    getCurrentVersion(): string | null {
        return this._currentVersion;
    }
}

export class SingletonKeyValueStorage {
    readonly versionControl: boolean;
    readonly encryptor?: SimpleRSAChunkEncryptor;

    conn: AbstractStorageController;
    messageQueue: MessageQueueController;
    private versionController: LocalVersionController;
    private eventDispatcher: EventDispatcherController;

    constructor(versionControl: boolean = false, encryptor?: SimpleRSAChunkEncryptor) {
        this.versionControl = versionControl;
        this.encryptor = encryptor;
        this.switchBackend(DictStorage.build());
    }

    switchBackend(controller: AbstractStorageController): this {
        this.eventDispatcher = new EventDispatcherController(new DictStorage());
        this.versionController = new LocalVersionController();
        this.messageQueue = new MessageQueueController(new DictStorage());
        this.conn = controller;
        return this;
    }

    private log(message: unknown): void {
        console.log(`[SingletonKeyValueStorage]: ${message instanceof Error ? message.message : message}`);
    }

    deleteSlave(slave: { uuid?: string } | null): number {
        const id = slave?.uuid ?? null;
        return id ? this.deleteEvent(id) : 0;
    }

    addSlave(slave: Record<string, any>, eventNames: string[] = ['set', 'delete']): boolean {
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
            const fn = (...args: any[])=>slave[name](...args);
            if (typeof fn === 'function') {
                this.setEvent(name, fn, slave.uuid);
            } else {
                this.log(`no func of "${name}" in ${slave}. Skip it.`);
            }
        }
        return true;
    }

    private editLocal(funcName: 'set' | 'delete' | 'clean' | 'load' | 'loads', key?: string, value?: StoreValue): any {
        if (!['set', 'delete', 'clean', 'load', 'loads'].includes(funcName)) {
            throw new Error(`no func of "${funcName}". return.`);
        }
        const fn = (this.conn as any)[funcName];
        if (typeof fn !== 'function') {
            throw new Error(`no func of "${funcName}"`);
        }
        const args = [key, value].filter(arg => arg !== undefined);
        return fn.apply(this.conn, args);
    }

    private edit(funcName: 'set' | 'delete' | 'clean' | 'load' | 'loads', key?: string, value?: StoreValue): any {
        const argsForEvent = [key, value].filter(arg => arg !== undefined);
        let payload = value;
        if (this.encryptor && funcName === 'set' && value !== undefined) {
            payload = { rjson: this.encryptor.encryptString(JSON.stringify(value)) };
        }
        const result = this.editLocal(funcName, key, payload);
        this.dispatchEvent(funcName, ...argsForEvent);
        return result;
    }

    private tryEdit(operation: Operation): boolean {
        if (this.versionControl) {
            const [func, key] = operation;
            let revert: Operation | null = null;
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
            this.edit(operation[0] as any, operation[1] as string | undefined, operation[2]);
            return true;
        } catch (error) {
            this.log(error);
            return false;
        }
    }

    private tryLoad<T>(fn: () => T): T | null {
        try {
            return fn();
        } catch (error) {
            this.log(error);
            return null;
        }
    }

    revertOneOperation(): void {
        this.versionController.revertOneOperation(op => {
            if (!op) return;
            const [func, key, value] = op;
            this.editLocal(func as any, key as string | undefined, value);
        });
    }

    forwardOneOperation(): void {
        this.versionController.forwardOneOperation(op => {
            const [func, key, value] = op;
            this.editLocal(func as any, key as string | undefined, value);
        });
    }

    getCurrentVersion(): string | null {
        return this.versionController.getCurrentVersion();
    }

    localToVersion(opuuid: string): void {
        this.versionController.toVersion(opuuid, op => {
            const [func, key, value] = op;
            this.editLocal(func as any, key as string | undefined, value);
        });
    }

    set(key: string, value: StoreValue): boolean {
        return this.tryEdit(['set', key, value]);
    }

    delete(key: string): boolean {
        return this.tryEdit(['delete', key]);
    }

    clean(): boolean {
        return this.tryEdit(['clean']);
    }

    load(jsonPath: string): boolean {
        return this.tryEdit(['load', jsonPath]);
    }

    loads(jsonString: string): boolean {
        return this.tryEdit(['loads', jsonString]);
    }

    exists(key: string): boolean | null {
        return this.tryLoad(() => this.conn.exists(key));
    }

    keys(pattern: string = '*'): string[] | null {
        return this.tryLoad(() => this.conn.keys(pattern));
    }

    get(key: string): StoreValue | null {
        const value = this.tryLoad(() => this.conn.get(key));
        if (value && this.encryptor && typeof value === 'object' && 'rjson' in value) {
            return this.tryLoad(() => JSON.parse(this.encryptor.decryptString((value as any).rjson)));
        }
        return value;
    }

    dumps(): string | null {
        return this.tryLoad(() => {
            const output: StoreRecord = {};
            for (const key of this.conn.keys('*')) {
                output[key] = this.get(key);
            }
            return JSON.stringify(output);
        });
    }

    dump(jsonPath: string): void | null {
        return this.tryLoad(() => this.conn.dump(jsonPath));
    }

    events(): Array<[string, EventCallback | null]> {
        return this.eventDispatcher.events();
    }

    getEvent(uuid: string): Array<EventCallback | null> {
        return this.eventDispatcher.getEvent(uuid);
    }

    deleteEvent(uuid: string): number {
        return this.eventDispatcher.deleteEvent(uuid);
    }

    setEvent(eventName: string, callback: EventCallback, id?: string): string {
        return this.eventDispatcher.setEvent(eventName, callback, id);
    }

    dispatchEvent(eventName: string, ...args: any[]): void {
        this.eventDispatcher.dispatchEvent(eventName, ...args);
    }

    cleanEvents(): void {
        this.eventDispatcher.clean();
    }
}
