// import * as fs from 'fs';
import {PEMFileReader,SimpleRSAChunkEncryptor} from './RSA';

function uuidv4() {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);          // 128 random bits

  b[6] = (b[6] & 0x0f) | 0x40;        // version = 4
  b[8] = (b[8] & 0x3f) | 0x80;        // variant = RFC 4122

  const hex = [...b].map(x => x.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}

class AbstractStorage {
    static _uuid = uuidv4();
    static _store: any = null;
    static _is_singleton = true;
    static _meta: Record<string, any> = {};

    uuid: string;
    store: any;
    isSingleton: boolean;

    constructor(id: string | null = null, store: any = null, isSingleton: boolean | null = null) {
        this.uuid = id || uuidv4();
        this.store = store || null;
        this.isSingleton = isSingleton ?? false;
    }

    getSingleton(): AbstractStorage {
        return new AbstractStorage(AbstractStorage._uuid, AbstractStorage._store, AbstractStorage._is_singleton);
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
        return encryptor.encryptString(this.dumps(),compress);
    }

    loadRSAs(content: string, privateKeyPath: string): void {        
        const privateKey = new PEMFileReader(privateKeyPath).loadPrivatePkcs8Key();
        const encryptor = new SimpleRSAChunkEncryptor(null, privateKey);
        const decryptedText = encryptor.decryptString(content);
        this.loads(decryptedText);
    }
}

class TsDictStorage extends AbstractStorage {
    constructor(id: string | null = null, store: any = null, isSingleton: boolean | null = null) {
        super(id, store, isSingleton);
        this.store = store || {};
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

class LocalVersionController {
    static TABLENAME = '_Operation';
    static LISTNAME = '_Operations';
    static KEY = 'ops';
    static FORWARD = 'forward';
    static REVERT = 'revert';

    private client: TsDictStorageController;
    private _currentVersion: string;

    constructor(client: TsDictStorageController | null = null) {
        this.client = client || new TsDictStorageController(new TsDictStorage());
        this._setVersions([]);
        this._currentVersion = 'null';
    }

    getVersions(): string[] {
        return this.client.get(LocalVersionController.TABLENAME)?.[LocalVersionController.KEY] || [];
    }

    private _setVersions(ops: string[]): void {
        this.client.set(LocalVersionController.TABLENAME, { [LocalVersionController.KEY]: ops });
    }

    findVersion(versionUuid: string): [string[], number | null, number | null, any] {
        const versions = [...this.getVersions()];
        const currentVersionIdx = this._currentVersion ? versions.indexOf(this._currentVersion) : null;
        const targetVersionIdx = versions.includes(versionUuid) ? versions.indexOf(versionUuid) : null;
        const op = targetVersionIdx !== null ? this.client.get(`${LocalVersionController.TABLENAME}:${versions[targetVersionIdx]}`) : null;
        return [versions, currentVersionIdx, targetVersionIdx, op];
    }

    addOperation(operation: any, revert: any = null): void {
        const opUuid = uuidv4();
        this.client.set(`${LocalVersionController.TABLENAME}:${opUuid}`, {
            [LocalVersionController.FORWARD]: operation,
            [LocalVersionController.REVERT]: revert
        });

        let ops = this.getVersions();
        if (this._currentVersion) {
            const opIdx = ops.indexOf(this._currentVersion);
            ops = ops.slice(0, opIdx + 1);
        }

        ops.push(opUuid);
        this._setVersions(ops);
        this._currentVersion = opUuid;
    }

    forwardOneOperation(forwardCallback: (forward: any) => void): void {
        const [versions, currentVersionIdx] = this.findVersion(this._currentVersion);
        if (currentVersionIdx === null || versions.length <= currentVersionIdx + 1) return;

        const op = this.client.get(`${LocalVersionController.TABLENAME}:${versions[currentVersionIdx + 1]}`);
        if (op) {
            forwardCallback(op[LocalVersionController.FORWARD]);
            this._currentVersion = versions[currentVersionIdx + 1];
        }
    }

    revertOneOperation(revertCallback: (revert: any) => void): void {
        const [versions, currentVersionIdx, , op] = this.findVersion(this._currentVersion);
        if (currentVersionIdx === null || currentVersionIdx - 1 < 0) return;

        if (op) {
            revertCallback(op[LocalVersionController.REVERT]);
            this._currentVersion = versions[currentVersionIdx - 1];
        }
    }

    toVersion(versionUuid: string, versionCallback: (ops: any) => void): void {
        const [_, currentVersionIdx, targetVersionIdx] = this.findVersion(versionUuid);
        if (targetVersionIdx === null) throw new Error(`No such version: ${versionUuid}`);

        let deltaIdx = targetVersionIdx - (currentVersionIdx || 0);
        const sign = Math.sign(deltaIdx);

        while (Math.abs(deltaIdx) > 0) {
            if (sign > 0) {
                this.forwardOneOperation(versionCallback);
            } else {
                this.revertOneOperation(versionCallback);
            }
            deltaIdx -= sign;
        }
    }
}

export class SingletonKeyValueStorage {
    versionControl: boolean;
    conn: TsDictStorageController | null;
    private eventDispatcher: EventDispatcherController = new EventDispatcherController(new TsDictStorage());
    private versionController: LocalVersionController = new LocalVersionController();

    private static backends: Record<string, (...args: any[]) => TsDictStorageController> = {
        temp_ts: (...args: any[]) => new TsDictStorageController(new TsDictStorage(...args)),
        ts: (...args: any[]) => new TsDictStorageController(new TsDictStorage(...args).getSingleton()),
    };

    constructor(versionControl = false) {
        this.versionControl = versionControl;
        this.conn = null;
        this.tsBackend();
    }

    private switchBackend(name: string = 'ts', ...args: any[]): TsDictStorageController {
        this.eventDispatcher = new EventDispatcherController(new TsDictStorage());
        this.versionController = new LocalVersionController();

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

    private editLocal(
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

        // Call the function with the provided arguments
        return func.bind(this.conn)(...args);
    }

    private edit(
        funcName: 'set' | 'delete' | 'clean' | 'load' | 'loads',
        key?: string,
        value?: Record<string, any>
    ): any {
        const args = [key, value].filter((arg) => arg !== undefined);
        const result = this.editLocal(funcName, key, value);
        this.dispatchEvent(funcName, ...args);
        return result;
    }

    private tryEditWithErrorHandling(args: any[]): boolean {
        const [func, key, value] = args;
        if (this.versionControl) {
            let revert;
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

            if (revert) {
                this.versionController.addOperation(args, revert);
            }
        }

        try {
            this.edit(func, key, value);
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
            const [func, key, value] = revert;
            this.editLocal(func, key, value);
        });
    }

    getCurrentVersion(): string | null {
        const versions = this.versionController.getVersions();
        return versions.length > 0 ? versions[versions.length - 1] : null;
    }

    localToVersion(opUuid: string): void {
        this.versionController.toVersion(opUuid, (revert) => {
            const [func, key, value] = revert;
            this.editLocal(func, key, value);
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
        return this.tryLoadWithErrorHandling(() => this.conn?.dumpRSAs(publicKeyPath,compress)) || '';
    }

    loadRSAs(content: string, privateKeyPath: string): void {
        return this.tryLoadWithErrorHandling(() => this.conn?.loadRSAs(content,privateKeyPath)) || '';
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
}

class Tests {
    private store: SingletonKeyValueStorage;

    constructor() {
        this.store = new SingletonKeyValueStorage();
    }

    testAll(num: number = 1): void {
        // this.testJs(num);
        this.testLocalStorage(num);
    }

    private testJs(num: number = 1): void {
        this.store.tsBackend();
        for (let i = 0; i < num; i++) this.testAllCases();
    }

    private testLocalStorage(num: number = 1): void {
        this.store.tempTsBackend();
        for (let i = 0; i < num; i++) this.testAllCases();
    }

    private testAllCases(): void {
        this.testSetAndGet();
        this.testExists();
        this.testDelete();
        this.testKeys();
        this.testGetNonexistent();
        this.testDumpAndLoad();
        this.testVersion();
        this.testSlaves();
        this.store.clean();
        console.log('All tests end.');        
    }

    private testSetAndGet(): void {
        this.store.set('test1', { data: 123 });
        console.assert(
            JSON.stringify(this.store.get('test1')) === JSON.stringify({ data: 123 }),
            "The retrieved value should match the set value."
        );
    }

    private testExists(): void {
        this.store.set('test2', { data: 456 });
        console.assert(this.store.exists('test2') === true, "Key should exist after being set.");
    }

    private testDelete(): void {
        this.store.set('test3', { data: 789 });
        this.store.delete('test3');
        console.assert(this.store.exists('test3') === false, "Key should not exist after being deleted.");
    }

    private testKeys(): void {
        this.store.set('alpha', { info: 'first' });
        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });
        const expectedKeys = ['alpha', 'abeta'];
        console.assert(
            JSON.stringify(this.store.keys('a*').sort()) === JSON.stringify(expectedKeys.sort()),
            "Should return the correct keys matching the pattern."
        );
    }

    private testGetNonexistent(): void {
        console.assert(this.store.get('nonexistent') === null, "Getting a non-existent key should return null.");
    }

    private testDumpAndLoad(): void {
        const raw = {
            "test1": { "data": 123 }
        };

        this.store.clean();
        console.assert(this.store.dumps() === '{}', "Should return the correct keys and values.");

        this.store.clean();
        this.store.loads(JSON.stringify(raw));
        console.assert(
            JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(raw),
            "Should return the correct keys and values."
        );
    }

    private testSlaves(): void {
        this.store.clean();
        this.store.loads(JSON.stringify({
            "alpha": { "info": "first" },
            "abeta": { "info": "second" },
            "gamma": { "info": "third" }
        }));

        if (this.store.conn?.constructor.name === 'SingletonTsDictStorageController') return;

        const store2 = new SingletonKeyValueStorage();
        store2.tempTsBackend();

        this.store.addSlave(store2);
        this.store.set('alpha', { info: 'first' });
        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });
        this.store.delete('abeta');

        console.assert(
            JSON.stringify(JSON.parse(this.store.dumps()).gamma) === JSON.stringify(JSON.parse(store2.dumps()).gamma),
            "Should return the correct keys and values."
        );
    }

    private testVersion(): void {
        this.store.versionControl = true;
        this.store.clean();
        this.store.set('alpha', { info: 'first' });
        const data = this.store.dumps();
        const version = this.store.getCurrentVersion();

        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });

        // console.log(this.store.dumpRSAs('../tmp/public_key.pem'));
        
        // this.store.revertOperationsUntil(version);

        // console.assert(
        //     JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(JSON.parse(data)),
        //     "Should return the same keys and values."
        // );
    }
}

// Running tests
// new Tests().testAll();
