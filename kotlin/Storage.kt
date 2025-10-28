import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.LinkedHashMap
import java.util.Locale
import java.util.UUID

typealias StoreMap = MutableMap<String, Any?>
typealias Operation = List<Any?>

data class EventInvocation(
    val args: List<Any?> = emptyList(),
    val kwargs: Map<String, Any?> = emptyMap()
)

typealias EventListener = (EventInvocation) -> Unit

/* ---------- Base64 helpers ---------- */

fun b64UrlEncode(value: String): String {
    val encoder = Base64.getUrlEncoder().withoutPadding()
    return encoder.encodeToString(value.toByteArray(StandardCharsets.UTF_8))
}

fun b64UrlDecode(value: String): String {
    val padding = (4 - value.length % 4) % 4
    val padded = value + "=".repeat(padding)
    val decoder = Base64.getUrlDecoder()
    val decoded = decoder.decode(padded)
    return String(decoded, StandardCharsets.UTF_8)
}

fun isB64Url(value: String): Boolean {
    return try {
        b64UrlEncode(b64UrlDecode(value)) == value
    } catch (_: Exception) {
        false
    }
}

/* ---------- Size helpers ---------- */

fun getDeepBytesSize(obj: Any?, seen: MutableSet<Int> = mutableSetOf()): Long {
    if (obj == null) return 0
    val identity = System.identityHashCode(obj)
    if (!seen.add(identity)) return 0
    return when (obj) {
        is String -> obj.toByteArray(StandardCharsets.UTF_8).size.toLong()
        is Boolean -> 1
        is Byte -> 1
        is Short -> Short.SIZE_BYTES.toLong()
        is Int -> Int.SIZE_BYTES.toLong()
        is Long -> Long.SIZE_BYTES.toLong()
        is Float -> Float.SIZE_BYTES.toLong()
        is Double -> Double.SIZE_BYTES.toLong()
        is Char -> Char.SIZE_BYTES.toLong()
        is BigInteger -> obj.toByteArray().size.toLong()
        is BigDecimal -> {
            val unscaledSize = obj.unscaledValue().toByteArray().size.toLong()
            unscaledSize + Int.SIZE_BYTES
        }
        is ByteArray -> obj.size.toLong()
        is ShortArray -> obj.size * Short.SIZE_BYTES.toLong()
        is IntArray -> obj.size * Int.SIZE_BYTES.toLong()
        is LongArray -> obj.size * Long.SIZE_BYTES.toLong()
        is FloatArray -> obj.size * Float.SIZE_BYTES.toLong()
        is DoubleArray -> obj.size * Double.SIZE_BYTES.toLong()
        is CharArray -> obj.size * Char.SIZE_BYTES.toLong()
        is BooleanArray -> obj.size.toLong()
        is Map<*, *> -> {
            var total = 32L
            for ((k, v) in obj) {
                total += getDeepBytesSize(k, seen)
                total += getDeepBytesSize(v, seen)
            }
            total
        }
        is Collection<*> -> {
            var total = 24L
            for (item in obj) {
                total += getDeepBytesSize(item, seen)
            }
            total
        }
        is Array<*> -> {
            var total = 24L
            for (item in obj) {
                total += getDeepBytesSize(item, seen)
            }
            total
        }
        else -> obj.toString().toByteArray(StandardCharsets.UTF_8).size.toLong()
    }
}

private fun shallowMapSize(map: Map<*, *>): Long = 32L * map.size

fun humanizeBytes(bytes: Long): String {
    val units = arrayOf("B", "KB", "MB", "GB", "TB", "PB")
    if (bytes <= 0) return "0.0 B"
    var size = bytes.toDouble()
    var idx = 0
    while (size >= 1024.0 && idx < units.lastIndex) {
        size /= 1024.0
        idx++
    }
    return String.format(Locale.US, "%3.1f %s", size, units[idx])
}

/* ---------- Storage models ---------- */

abstract class AbstractStorage(
    val uuid: UUID = UUID.randomUUID(),
    open var store: StoreMap = mutableMapOf(),
    var isSingleton: Boolean = false
) {
    companion object {
        val meta: MutableMap<String, Any?> = mutableMapOf()
    }

    abstract fun getSingleton(): AbstractStorage
    abstract fun bytesUsed(deep: Boolean = true, humanReadable: Boolean = true): Any
}

class DictStorage(
    id: UUID? = null,
    backingStore: StoreMap? = null,
    singleton: Boolean? = null
) : AbstractStorage(
    uuid = id ?: UUID.randomUUID(),
    store = backingStore ?: mutableMapOf(),
    isSingleton = singleton ?: false
) {
    companion object {
        private val singletonUuid: UUID = UUID.randomUUID()
        private val singletonStore: StoreMap = mutableMapOf()
        private const val singletonFlag: Boolean = true

        fun buildTmp(): DictStorageController = DictStorageController(DictStorage())
        fun build(): DictStorageController =
            DictStorageController(DictStorage(singletonUuid, singletonStore, singletonFlag))
    }

    override fun getSingleton(): DictStorage = DictStorage(singletonUuid, singletonStore, true)

    override fun bytesUsed(deep: Boolean, humanReadable: Boolean): Any {
        val size = if (deep) {
            getDeepBytesSize(store)
        } else {
            shallowMapSize(store)
        }
        return if (humanReadable) humanizeBytes(size) else size
    }
}

/* ---------- Storage controllers ---------- */

open class AbstractStorageController(protected val model: AbstractStorage) {
    open fun isSingleton(): Boolean = model.isSingleton
    open fun exists(key: String): Boolean {
        println("[${this::class.simpleName}]: not implement")
        return false
    }

    open fun set(key: String, value: Any?) {
        println("[${this::class.simpleName}]: not implement")
    }

    open fun get(key: String): Any? {
        println("[${this::class.simpleName}]: not implement")
        return null
    }

    open fun delete(key: String): Any? {
        println("[${this::class.simpleName}]: not implement")
        return null
    }

    open fun keys(pattern: String = "*"): List<String> {
        println("[${this::class.simpleName}]: not implement")
        return emptyList()
    }

    open fun clean() {
        keys("*").forEach { delete(it) }
    }

    open fun bytesUsed(deep: Boolean = true, humanReadable: Boolean = true): Any =
        model.bytesUsed(deep, humanReadable)

    open fun dumps(): String {
        val snapshot = LinkedHashMap<String, Any?>()
        for (key in keys("*")) {
            snapshot[key] = get(key)
        }
        return SimpleJson.stringify(snapshot)
    }

    open fun loads(jsonString: String = "{}") {
        val parsed = SimpleJson.parse(jsonString)
        if (parsed !is Map<*, *>) {
            throw IllegalArgumentException("Expected top-level JSON object.")
        }
        for ((k, v) in parsed) {
            val key = k as? String ?: throw IllegalArgumentException("JSON object keys must be strings.")
            set(key, toMutableJsonValue(v))
        }
    }

    open fun dump(path: String): Path {
        val target = Path.of(path)
        Files.writeString(target, dumps())
        return target
    }

    open fun load(path: String) {
        val content = Files.readString(Path.of(path))
        loads(content)
    }

    open fun dumpRSA(path: String, publicPkcs8KeyPath: String): Path {
        val (e, n) = PEMFileReader(publicPkcs8KeyPath).loadPublicPkcs8Key()
        val encryptor = SimpleRSAChunkEncryptor(publicKey = e to n)
        val encrypted = encryptor.encryptString(dumps())
        val target = Path.of(path)
        Files.writeString(target, encrypted)
        return target
    }

    open fun loadRSA(path: String, privatePkcs8KeyPath: String) {
        val (d, n) = PEMFileReader(privatePkcs8KeyPath).loadPrivatePkcs8Key()
        val encryptor = SimpleRSAChunkEncryptor(privateKey = d to n)
        val decrypted = encryptor.decryptString(Files.readString(Path.of(path)))
        loads(decrypted)
    }

    protected fun toMutableJsonValue(value: Any?): Any? = when (value) {
        is Map<*, *> -> value.entries.associate { entry ->
            val key = entry.key as? String
                ?: throw IllegalArgumentException("Nested JSON object keys must be strings.")
            key to toMutableJsonValue(entry.value)
        }.toMutableMap()
        is List<*> -> value.map { toMutableJsonValue(it) }.toMutableList()
        else -> value
    }
}

open class DictStorageController(private val dictModel: DictStorage) : AbstractStorageController(dictModel) {
    private val store: StoreMap = dictModel.store

    override fun exists(key: String): Boolean = store.containsKey(key)

    override fun set(key: String, value: Any?) {
        store[key] = when (value) {
            is Map<*, *> -> value.entries.associate { (k, v) ->
                val key = k as? String ?: throw IllegalArgumentException("Nested map keys must be strings.")
                key to v
            }.toMutableMap()
            is List<*> -> value.toMutableList()
            else -> value
        }
    }

    override fun get(key: String): Any? = store[key]

    override fun delete(key: String): Any? = store.remove(key)

    override fun keys(pattern: String): List<String> {
        val regex = wildcardToRegex(pattern)
        return store.keys.filter { regex.matches(it) }
    }
}

open class MemoryLimitedDictStorageController(
    model: DictStorage,
    maxMemoryMb: Double = 1024.0,
    policy: String = "lru",
    private val onEvict: (String, Any?) -> Unit = { _, _ -> },
    pinned: Set<String>? = null
) : DictStorageController(model) {
    private val maxBytes: Long = (maxMemoryMb.coerceAtLeast(0.0) * 1024 * 1024).toLong()
    private val policyName: String = policy.lowercase(Locale.getDefault()).trim()
    private val pinnedKeys: MutableSet<String> = (pinned?.toMutableSet()) ?: mutableSetOf()
    private val sizes: MutableMap<String, Long> = mutableMapOf()
    private val order: LinkedHashMap<String, Unit> = LinkedHashMap()
    private var currentBytes: Long = 0

    init {
        if (policyName !in setOf("lru", "fifo")) {
            throw IllegalArgumentException("policy must be 'lru' or 'fifo'")
        }
    }

    private fun entrySize(key: String, value: Any?): Long {
        return getDeepBytesSize(key) + getDeepBytesSize(value)
    }

    override fun bytesUsed(deep: Boolean, humanReadable: Boolean): Any {
        val size = currentBytes
        return if (humanReadable) humanizeBytes(size) else size
    }

    private fun reduce(key: String) {
        order.remove(key)
        val removed = sizes.remove(key) ?: 0L
        currentBytes -= removed
        if (currentBytes < 0) currentBytes = 0
    }

    private fun pickVictim(): String? {
        for (entryKey in order.keys) {
            if (!pinnedKeys.contains(entryKey)) {
                return entryKey
            }
        }
        return null
    }

    private fun maybeEvict() {
        if (maxBytes <= 0) return
        while (currentBytes > maxBytes && order.isNotEmpty()) {
            val victim = pickVictim() ?: break
            val value = super.get(victim)
            reduce(victim)
            super.delete(victim)
            onEvict(victim, value)
        }
    }

    private fun touchKey(key: String) {
        if (policyName == "lru") {
            order.remove(key)
            order[key] = Unit
        } else if (!order.containsKey(key)) {
            order[key] = Unit
        }
    }

    override fun set(key: String, value: Any?) {
        if (exists(key)) {
            reduce(key)
        }
        super.set(key, value)
        val stored = super.get(key)
        val size = entrySize(key, stored)
        sizes[key] = size
        currentBytes += size
        touchKey(key)
        maybeEvict()
    }

    override fun get(key: String): Any? {
        val value = super.get(key)
        if (value != null && policyName == "lru" && order.containsKey(key)) {
            order.remove(key)
            order[key] = Unit
        }
        return value
    }

    override fun delete(key: String): Any? {
        if (exists(key)) {
            reduce(key)
        }
        return super.delete(key)
    }
}

/* ---------- Utility helpers ---------- */

private class BiDirectionalB64Cache {
    private val forward: MutableMap<String, String> = mutableMapOf("*" to "*")
    private val reverse: MutableMap<String, String> = mutableMapOf("*" to "*")

    fun encode(value: String): String {
        if (forward.containsKey(value)) return forward[value]!!
        val encoded = b64UrlEncode(value)
        forward[value] = encoded
        reverse[encoded] = value
        return encoded
    }

    fun decode(value: String): String {
        return reverse.getOrPut(value) {
            val decoded = b64UrlDecode(value)
            forward[decoded] = value
            decoded
        }
    }
}

class EventDispatcherController(model: DictStorage) : DictStorageController(model) {
    companion object {
        private const val ROOT_KEY = "_Event"
    }

    private val cache = BiDirectionalB64Cache()

    private fun eventGlob(eventName: String = "*", eventId: String = "*"): String {
        val safeName = cache.encode(eventName)
        return "$ROOT_KEY:$safeName:$eventId"
    }

    private fun findEventKeys(eventId: String): List<String> =
        keys(eventGlob("*", eventId))

    fun events(): List<Pair<String, EventListener>> {
        val out = mutableListOf<Pair<String, EventListener>>()
        for (key in keys(eventGlob())) {
            val listener = get(key) as? EventListener ?: continue
            out += key to listener
        }
        return out
    }

    fun getEvent(eventId: String): List<EventListener> =
        findEventKeys(eventId).mapNotNull { get(it) as? EventListener }

    fun deleteEvent(eventId: String): Int {
        val keys = findEventKeys(eventId)
        for (key in keys) {
            delete(key)
        }
        return keys.size
    }

    fun setEvent(eventName: String, callback: EventListener, eventId: String? = null): String {
        val eid = eventId ?: UUID.randomUUID().toString()
        set(eventGlob(eventName, eid), callback)
        return eid
    }

    fun dispatchEvent(eventName: String, args: List<Any?> = emptyList(), kwargs: Map<String, Any?> = emptyMap()) {
        val glob = eventGlob(eventName, "*")
        for (key in keys(glob)) {
            val cb = get(key)
            val listener = cb as? EventListener ?: continue
            try {
                listener(EventInvocation(args, kwargs))
            } catch (_: Exception) {
                // ignore listener errors
            }
        }
    }
}

class MessageQueueController(
    model: DictStorage,
    maxMemoryMb: Double = 1024.0,
    policy: String = "lru",
    onEvict: (String, Any?) -> Unit = { _, _ -> },
    pinned: Set<String>? = null,
    dispatcher: EventDispatcherController? = null
) : MemoryLimitedDictStorageController(model, maxMemoryMb, policy, onEvict, pinned) {

    companion object {
        private const val ROOT_KEY = "_MessageQueue"
        private const val ROOT_EVENT_KEY = "MQE"
    }

    private val cache = BiDirectionalB64Cache()
    private val dispatcherController = dispatcher ?: EventDispatcherController(model)

    private fun queueNameEncoded(queueName: String): String = cache.encode(queueName)

    private fun queueKey(queueName: String, index: String? = null): String {
        val encoded = queueNameEncoded(queueName)
        return listOfNotNull(ROOT_KEY, encoded, index).joinToString(":")
    }

    private fun eventName(queueName: String, kind: String): String =
        "$ROOT_EVENT_KEY:${queueNameEncoded(queueName)}:$kind"

    private data class QueueMeta(var head: Long, var tail: Long)

    private fun loadMeta(queueName: String): QueueMeta {
        val key = queueKey(queueName)
        val stored = super.get(key)
        val meta = if (stored is Map<*, *>) {
            val head = (stored["head"] as? Number)?.toLong() ?: 0L
            val tail = (stored["tail"] as? Number)?.toLong() ?: 0L
            QueueMeta(head, tail)
        } else {
            QueueMeta(0L, 0L)
        }
        if (meta.head < 0 || meta.tail < meta.head) {
            val reset = QueueMeta(0L, 0L)
            saveMeta(queueName, reset)
            return reset
        }
        return meta
    }

    private fun saveMeta(queueName: String, meta: QueueMeta) {
        super.set(queueKey(queueName), mutableMapOf("head" to meta.head, "tail" to meta.tail))
    }

    private fun sizeFromMeta(meta: QueueMeta): Int = (meta.tail - meta.head).coerceAtLeast(0L).toInt()

    private fun dispatch(queueName: String, kind: String, key: String?, message: Any?) {
        dispatcherController.dispatchEvent(
            eventName(queueName, kind),
            args = emptyList(),
            kwargs = mapOf("queue" to queueName, "key" to key, "message" to message, "op" to kind)
        )
    }

    fun addListener(
        queueName: String,
        callback: EventListener,
        eventKind: String = "pushed",
        listenerId: String? = null
    ): String {
        return dispatcherController.setEvent(eventName(queueName, eventKind), callback, listenerId)
    }

    fun removeListener(listenerId: String): Int = dispatcherController.deleteEvent(listenerId)

    fun listListeners(queueName: String? = null, eventKind: String? = null): List<Pair<String, EventListener>> {
        val events = dispatcherController.events()
        if (queueName == null && eventKind == null) return events

        val encodedQueue = queueName?.let { queueNameEncoded(it) }
        val filtered = mutableListOf<Pair<String, EventListener>>()

        for ((key, cb) in events) {
            val parts = key.split(':')
            if (parts.size < 3) continue

            val encodedEventName = parts[1]
            val decodedEventName = runCatching { cache.decode(encodedEventName) }.getOrNull() ?: continue
            val eventParts = decodedEventName.split(':')
            if (eventParts.size < 3) continue

            val rootKind = eventParts[0]
            val queuePart = eventParts[1]
            val kind = eventParts[2]

            if (rootKind != ROOT_EVENT_KEY) continue

            val queueMatches = encodedQueue == null || queuePart == encodedQueue
            val kindMatches = eventKind == null || kind == eventKind
            if (queueMatches && kindMatches) {
                filtered += key to cb
            }
        }
        return filtered
    }

    private fun advanceHeadPastHoles(queueName: String, meta: QueueMeta): QueueMeta {
        while (meta.head < meta.tail) {
            val key = queueKey(queueName, meta.head.toString())
            if (super.get(key) != null) break
            meta.head += 1
        }
        return meta
    }

    fun push(message: Any?, queueName: String = "default"): String {
        val meta = loadMeta(queueName)
        val idx = meta.tail
        val key = queueKey(queueName, idx.toString())
        super.set(key, toMutableJsonValue(message))
        meta.tail = idx + 1
        saveMeta(queueName, meta)
        dispatch(queueName, "pushed", key, message)
        return key
    }

    private fun normalizeMessage(message: Any?): Any? = when (message) {
        is Map<*, *> -> toMutableJsonValue(message)
        is List<*> -> message.toMutableList()
        else -> message
    }

    fun popItem(queueName: String = "default", peek: Boolean = false): Pair<String?, Any?> {
        var meta = loadMeta(queueName)
        meta = advanceHeadPastHoles(queueName, meta)
        if (meta.head >= meta.tail) return null to null

        val key = queueKey(queueName, meta.head.toString())
        val message = super.get(key)
        if (message == null) {
            meta.head += 1
            saveMeta(queueName, meta)
            meta = advanceHeadPastHoles(queueName, meta)
            return if (meta.head >= meta.tail) null to null else popItem(queueName, peek)
        }

        if (peek) return key to normalizeMessage(message)

        super.delete(key)
        meta.head += 1
        saveMeta(queueName, meta)
        val normalized = normalizeMessage(message)
        dispatch(queueName, "popped", key, normalized)
        if (sizeFromMeta(meta) == 0) {
            dispatch(queueName, "empty", null, null)
        }
        return key to normalized
    }

    fun pop(queueName: String = "default"): Any? = popItem(queueName).second

    fun peek(queueName: String = "default"): Any? = popItem(queueName, peek = true).second

    fun queueSize(queueName: String = "default"): Int = sizeFromMeta(loadMeta(queueName))

    fun clear(queueName: String = "default") {
        val prefix = "${ROOT_KEY}:${queueNameEncoded(queueName)}:"
        for (key in keys("$prefix*")) {
            super.delete(key)
        }
        super.delete(queueKey(queueName))
        dispatch(queueName, "cleared", null, null)
    }

    fun listQueues(): List<String> {
        val queues = mutableSetOf<String>()
        for (key in keys("$ROOT_KEY:*")) {
            val parts = key.split(':')
            if (parts.size >= 2 && parts[0] == ROOT_KEY) {
                val decoded = runCatching { cache.decode(parts[1]) }.getOrNull() ?: continue
                queues += decoded
            }
        }
        return queues.sorted()
    }
}

class LocalVersionController(
    client: AbstractStorageController? = null,
    var limitMemoryMB: Double = 128.0,
    evictionPolicy: String = "fifo"
) {
    companion object {
        private const val TABLE_NAME = "_Operation"
        private const val KEY = "ops"
        private const val FORWARD = "forward"
        private const val REVERT = "revert"
    }

    private val storage: AbstractStorageController
    var currentVersion: String? = null
        private set

    init {
        storage = client ?: MemoryLimitedDictStorageController(
            DictStorage(),
            maxMemoryMb = limitMemoryMB,
            policy = evictionPolicy,
            onEvict = ::onEvict,
            pinned = setOf(TABLE_NAME)
        )

        val table = storage.get(TABLE_NAME) as? Map<*, *>
        if (table == null || table[KEY] == null) {
            storage.set(TABLE_NAME, mutableMapOf(KEY to mutableListOf<String>()))
        }
    }

    private fun onEvict(key: String, value: Any?) {
        val prefix = "$TABLE_NAME:"
        if (!key.startsWith(prefix)) return
        val opId = key.removePrefix(prefix)
        val ops = getVersions().toMutableList()
        if (ops.remove(opId)) {
            setVersions(ops)
        }
        if (currentVersion == opId) {
            throw IllegalStateException("auto removed current_version")
        }
    }

    fun getVersions(): List<String> {
        val table = storage.get(TABLE_NAME) as? Map<*, *> ?: return emptyList()
        val ops = table[KEY]
        return when (ops) {
            is List<*> -> ops.filterIsInstance<String>()
            else -> emptyList()
        }
    }

    private fun setVersions(ops: List<String>) {
        storage.set(TABLE_NAME, mutableMapOf(KEY to ops.toMutableList()))
    }

    private data class VersionLookup(
        val versions: List<String>,
        val currentIndex: Int,
        val targetIndex: Int?,
        val operation: Map<String, Any?>?
    )

    private fun findVersion(versionUuid: String?): VersionLookup {
        val versions = getVersions()
        val currentIdx = versions.indexOf(currentVersion)
        val targetIdx = if (versionUuid != null && versions.contains(versionUuid)) {
            versions.indexOf(versionUuid)
        } else {
            null
        }
        val op = if (targetIdx != null && targetIdx in versions.indices) {
            val opId = versions[targetIdx]
            storage.get("$TABLE_NAME:$opId") as? Map<String, Any?>
        } else {
            null
        }
        return VersionLookup(versions, currentIdx, targetIdx, op)
    }

    fun estimateMemoryMB(): Double {
        val bytes = (storage.bytesUsed(true, false) as? Number)?.toLong() ?: 0L
        return bytes.toDouble() / (1024.0 * 1024.0)
    }

    fun addOperation(operation: Operation, revert: Operation? = null, verbose: Boolean = false): String? {
        val opId = UUID.randomUUID().toString()
        storage.set("$TABLE_NAME:$opId", mutableMapOf(FORWARD to operation, REVERT to revert))
        val ops = getVersions().toMutableList()
        if (currentVersion != null && ops.contains(currentVersion)) {
            val idx = ops.indexOf(currentVersion)
            val trimmed = ops.subList(0, idx + 1).toList()
            ops.clear()
            ops.addAll(trimmed)
        }
        ops += opId
        setVersions(ops)
        currentVersion = opId

        if (estimateMemoryMB() > limitMemoryMB) {
            val message = "[LocalVersionController] Warning: memory usage %.1f MB exceeds limit of %.1f MB".format(
                Locale.US,
                estimateMemoryMB(),
                limitMemoryMB
            )
            if (verbose) println(message)
            return message
        }
        return null
    }

    fun popOperation(n: Int = 1): List<Pair<String, Map<String, Any?>?>> {
        if (n <= 0) return emptyList()
        val ops = getVersions().toMutableList()
        if (ops.isEmpty()) return emptyList()

        val popped = mutableListOf<Pair<String, Map<String, Any?>?>>()
        repeat(minOf(n, ops.size)) {
            val popIdx = if (ops.isNotEmpty() && ops.first() != currentVersion) 0 else ops.lastIndex
            val opId = ops.removeAt(popIdx)
            val record = storage.get("$TABLE_NAME:$opId") as? Map<String, Any?>
            storage.delete("$TABLE_NAME:$opId")
            popped += opId to record
        }
        setVersions(ops)
        if (currentVersion !in ops) {
            currentVersion = ops.lastOrNull()
        }
        return popped
    }

    fun forwardOneOperation(callback: (Operation) -> Unit) {
        val lookup = findVersion(currentVersion)
        val nextIdx = lookup.currentIndex + 1
        if (nextIdx !in lookup.versions.indices) return
        val op = storage.get("$TABLE_NAME:${lookup.versions[nextIdx]}") as? Map<String, Any?> ?: return
        val forward = op[FORWARD] as? Operation ?: return
        callback(forward)
        currentVersion = lookup.versions[nextIdx]
    }

    fun revertOneOperation(callback: (Operation?) -> Unit) {
        val lookup = findVersion(currentVersion)
        val currentIdx = lookup.currentIndex
        if (currentIdx <= 0) return
        val op = storage.get("$TABLE_NAME:${lookup.versions[currentIdx]}") as? Map<String, Any?> ?: return
        val revert = op[REVERT] as? Operation
        callback(revert)
        currentVersion = lookup.versions[currentIdx - 1]
    }

    fun toVersion(versionUuid: String, callback: (Operation) -> Unit) {
        val lookup = findVersion(versionUuid)
        val targetIdx = lookup.targetIndex ?: throw IllegalArgumentException("no such version of $versionUuid")
        var currentIdx = lookup.currentIndex.coerceAtLeast(-1)

        while (currentIdx != targetIdx) {
            if (currentIdx < targetIdx) {
                forwardOneOperation(callback)
                currentIdx += 1
            } else {
                revertOneOperation { revert ->
                    if (revert != null) callback(revert)
                }
                currentIdx -= 1
            }
        }
    }
}

class SingletonKeyValueStorage(
    var versionControl: Boolean = false,
    private val encryptor: SimpleRSAChunkEncryptor? = null
) {
    private lateinit var connection: AbstractStorageController
    private lateinit var eventDispatcher: EventDispatcherController
    internal lateinit var versionController: LocalVersionController
    lateinit var messageQueue: MessageQueueController
        private set
    private val slaveListeners: MutableMap<SingletonKeyValueStorage, MutableList<String>> = mutableMapOf()
    val connectionController: AbstractStorageController
        get() = connection

    init {
        switchBackend(DictStorage.build())
    }

    fun switchBackend(controller: AbstractStorageController): SingletonKeyValueStorage {
        connection = controller
        eventDispatcher = EventDispatcherController(DictStorage())
        versionController = LocalVersionController()
        messageQueue = MessageQueueController(DictStorage())
        slaveListeners.clear()
        return this
    }

    private fun printMessage(msg: Any?) {
        println("[${this::class.simpleName}]: $msg")
    }

    private fun editLocal(functionName: String, arg1: String? = null, arg2: Any? = null): Any? {
        return when (functionName) {
            "set" -> connection.set(requireNotNull(arg1), arg2)
            "delete" -> connection.delete(requireNotNull(arg1))
            "clean" -> connection.clean()
            "load" -> connection.load(requireNotNull(arg1))
            "loads" -> connection.loads(requireNotNull(arg1))
            else -> {
                printMessage("no func of \"$functionName\". return.")
                null
            }
        }
    }

    private fun edit(functionName: String, key: String? = null, value: Any? = null): Any? {
        val argsValue = if (encryptor != null && functionName == "set" && value is Map<*, *>) {
            val payload = SimpleJson.stringify(value)
            mutableMapOf("rjson" to encryptor.encryptString(payload))
        } else {
            value
        }
        val result = editLocal(functionName, key, argsValue)
        val dispatchArgs = mutableListOf<Any?>()
        if (key != null) dispatchArgs += key
        if (argsValue != null) dispatchArgs += argsValue
        eventDispatcher.dispatchEvent(functionName, args = dispatchArgs)
        return result
    }

    private fun tryEdit(vararg args: Any?): Boolean {
        if (versionControl) {
            val func = args[0] as String
            when (func) {
                "set" -> {
                    val key = args[1] as String
                    val value = args[2]
                    val revert = if (exists(key)) {
                        listOf("set", key, get(key))
                    } else {
                        listOf("delete", key)
                    }
                    versionController.addOperation(args.toList(), revert)
                }

                "delete" -> {
                    val key = args[1] as String
                    val revert = listOf("set", key, get(key))
                    versionController.addOperation(args.toList(), revert)
                }

                "clean", "load", "loads" -> {
                    val snapshot = dumps()
                    versionController.addOperation(args.toList(), listOf("loads", snapshot))
                }
            }
        }

        return try {
            edit(args[0] as String, args.getOrNull(1) as? String, args.getOrNull(2))
            true
        } catch (ex: Exception) {
            printMessage(ex.message)
            false
        }
    }

    fun revertOneOperation() {
        versionController.revertOneOperation { revert ->
            if (revert != null) {
                editLocal(revert[0] as String, revert.getOrNull(1) as? String, revert.getOrNull(2))
            }
        }
    }

    fun forwardOneOperation() {
        versionController.forwardOneOperation { forward ->
            editLocal(forward[0] as String, forward.getOrNull(1) as? String, forward.getOrNull(2))
        }
    }

    fun getCurrentVersion(): String? = versionController.currentVersion

    fun localToVersion(opuuid: String) {
        versionController.toVersion(opuuid) { op ->
            editLocal(op[0] as String, op.getOrNull(1) as? String, op.getOrNull(2))
        }
    }

    fun set(key: String, value: Map<String, Any?>): Boolean = tryEdit("set", key, value)

    fun delete(key: String): Boolean = tryEdit("delete", key)

    fun clean(): Boolean = tryEdit("clean")

    fun load(path: String): Boolean = tryEdit("load", path)

    fun loads(jsonString: String): Boolean = tryEdit("loads", jsonString)

    private fun <T> tryLoad(block: () -> T): T? =
        try {
            block()
        } catch (ex: Exception) {
            printMessage(ex.message)
            null
        }

    fun exists(key: String): Boolean = tryLoad { connection.exists(key) } ?: false

    fun keys(pattern: String = "*"): List<String> = tryLoad { connection.keys(pattern) } ?: emptyList()

    fun get(key: String): Any? {
        val raw = tryLoad { connection.get(key) } ?: return null
        if (encryptor != null && raw is Map<*, *> && raw.containsKey("rjson")) {
            val cipher = raw["rjson"] as? String ?: return raw
            val decrypted = tryLoad { encryptor.decryptString(cipher) } ?: return null
            return SimpleJson.parse(decrypted)
        }
        return raw
    }

    fun dumps(): String =
        tryLoad {
            val snapshot = LinkedHashMap<String, Any?>()
            for (key in keys("*")) {
                snapshot[key] = get(key)
            }
            SimpleJson.stringify(snapshot)
        } ?: "{}"

    fun dump(path: String): Boolean = tryLoad { connection.dump(path); true } ?: false

    fun events(): List<Pair<String, EventListener>> = eventDispatcher.events()

    fun getEvent(uuid: String): List<EventListener> = eventDispatcher.getEvent(uuid)

    fun deleteEvent(uuid: String): Int = eventDispatcher.deleteEvent(uuid)

    fun setEvent(eventName: String, callback: EventListener, id: String? = null): String =
        eventDispatcher.setEvent(eventName, callback, id)

    fun dispatchEvent(eventName: String, args: List<Any?> = emptyList(), kwargs: Map<String, Any?> = emptyMap()) {
        eventDispatcher.dispatchEvent(eventName, args, kwargs)
    }

    fun cleanEvents() = eventDispatcher.clean()

    fun addSlave(slave: SingletonKeyValueStorage, eventNames: List<String> = listOf("set", "delete")): Boolean {
        val listeners = mutableListOf<String>()
        for (event in eventNames) {
            val listener: EventListener = listener@{ invocation ->
                when (event) {
                    "set" -> {
                        val key = invocation.args.getOrNull(0) as? String ?: return@listener
                        val value = invocation.args.getOrNull(1)
                        if (value is Map<*, *>) {
                            @Suppress("UNCHECKED_CAST")
                            slave.set(key, value as Map<String, Any?>)
                        }
                    }

                    "delete" -> {
                        val key = invocation.args.getOrNull(0) as? String ?: return@listener
                        slave.delete(key)
                    }
                }
            }
            val id = setEvent(event, listener)
            listeners += id
        }
        slaveListeners[slave] = listeners
        return listeners.isNotEmpty()
    }

    fun deleteSlave(slave: SingletonKeyValueStorage): Boolean {
        val listeners = slaveListeners.remove(slave) ?: return false
        var removed = 0
        for (id in listeners) {
            removed += deleteEvent(id)
        }
        return removed > 0
    }
}

private fun wildcardToRegex(pattern: String): Regex {
    val sb = StringBuilder("^")
    for (ch in pattern) {
        when (ch) {
            '*' -> sb.append(".*")
            '?' -> sb.append('.')
            '.', '(', ')', '+', '|', '^', '$', '@', '%', '{', '}', '[', ']', '\\' -> {
                sb.append('\\').append(ch)
            }
            else -> sb.append(ch)
        }
    }
    sb.append('$')
    return Regex(sb.toString())
}

/* ---------- Minimal JSON (stringify + parse) ---------- */

object SimpleJson {
    fun stringify(value: Any?): String {
        return when (value) {
            null -> "null"
            is String -> "\"${escape(value)}\""
            is Number -> formatNumber(value)
            is Boolean -> value.toString()
            is Map<*, *> -> stringifyObject(value)
            is Collection<*> -> stringifyArray(value)
            is Array<*> -> stringifyArray(value.asList())
            else -> "\"${escape(value.toString())}\""
        }
    }

    fun parse(json: String): Any? = Parser(json).parse()

    private fun stringifyObject(map: Map<*, *>): String {
        val builder = StringBuilder()
        builder.append('{')
        var first = true
        for ((keyAny, value) in map) {
            val key = keyAny as? String
                ?: throw IllegalArgumentException("JSON object keys must be strings.")
            if (!first) builder.append(',')
            builder.append('"').append(escape(key)).append('"').append(':')
            builder.append(stringify(value))
            first = false
        }
        builder.append('}')
        return builder.toString()
    }

    private fun stringifyArray(col: Collection<*>): String {
        val builder = StringBuilder()
        builder.append('[')
        var first = true
        for (item in col) {
            if (!first) builder.append(',')
            builder.append(stringify(item))
            first = false
        }
        builder.append(']')
        return builder.toString()
    }

    private fun escape(text: String): String {
        val builder = StringBuilder(text.length + 16)
        for (c in text) {
            when (c) {
                '\\' -> builder.append("\\\\")
                '"' -> builder.append("\\\"")
                '\b' -> builder.append("\\b")
                '\u000C' -> builder.append("\\f")
                '\n' -> builder.append("\\n")
                '\r' -> builder.append("\\r")
                '\t' -> builder.append("\\t")
                else -> {
                    if (c.code < 0x20) {
                        builder.append(String.format("\\u%04x", c.code))
                    } else {
                        builder.append(c)
                    }
                }
            }
        }
        return builder.toString()
    }

    private fun formatNumber(number: Number): String {
        return when (number) {
            is Double, is Float -> {
                val dbl = number.toDouble()
                if (dbl.isNaN() || dbl.isInfinite()) {
                    throw IllegalArgumentException("JSON does not support NaN or Infinity.")
                }
                dbl.toString()
            }
            else -> number.toString()
        }
    }

    private class Parser(private val text: String) {
        private var index: Int = 0

        fun parse(): Any? {
            skipWhitespace()
            val value = parseValue()
            skipWhitespace()
            if (!isAtEnd()) {
                throw IllegalArgumentException("Unexpected trailing data at index $index")
            }
            return value
        }

        private fun parseValue(): Any? {
            skipWhitespace()
            if (isAtEnd()) throw IllegalArgumentException("Unexpected end of input")
            return when (val ch = peek()) {
                '{' -> parseObject()
                '[' -> parseArray()
                '"' -> parseString()
                't' -> parseTrue()
                'f' -> parseFalse()
                'n' -> parseNull()
                '-', in '0'..'9' -> parseNumber()
                else -> throw IllegalArgumentException("Unexpected character '$ch' at index $index")
            }
        }

        private fun parseObject(): MutableMap<String, Any?> {
            consume('{')
            skipWhitespace()
            val result = LinkedHashMap<String, Any?>()
            if (peekIf('}')) {
                consume('}')
                return result
            }
            while (true) {
                skipWhitespace()
                val key = parseString()
                skipWhitespace()
                consume(':')
                skipWhitespace()
                val value = parseValue()
                result[key] = value
                skipWhitespace()
                if (peekIf('}')) {
                    consume('}')
                    break
                }
                consume(',')
            }
            return result
        }

        private fun parseArray(): MutableList<Any?> {
            consume('[')
            skipWhitespace()
            val result = mutableListOf<Any?>()
            if (peekIf(']')) {
                consume(']')
                return result
            }
            while (true) {
                val value = parseValue()
                result.add(value)
                skipWhitespace()
                if (peekIf(']')) {
                    consume(']')
                    break
                }
                consume(',')
            }
            return result
        }

        private fun parseString(): String {
            consume('"')
            val builder = StringBuilder()
            while (!isAtEnd()) {
                val ch = advance()
                when (ch) {
                    '"' -> return builder.toString()
                    '\\' -> {
                        if (isAtEnd()) throw IllegalArgumentException("Invalid escape at end of input")
                        val esc = advance()
                        builder.append(
                            when (esc) {
                                '"', '\\', '/' -> esc
                                'b' -> '\b'
                                'f' -> '\u000C'
                                'n' -> '\n'
                                'r' -> '\r'
                                't' -> '\t'
                                'u' -> {
                                    val hex = take(4)
                                    hex.toInt(16).toChar()
                                }
                                else -> throw IllegalArgumentException("Invalid escape sequence: \\$esc")
                            }
                        )
                    }
                    else -> builder.append(ch)
                }
            }
            throw IllegalArgumentException("Unterminated string")
        }

        private fun parseTrue(): Boolean {
            expect("true")
            return true
        }

        private fun parseFalse(): Boolean {
            expect("false")
            return false
        }

        private fun parseNull(): Nothing? {
            expect("null")
            return null
        }

        private fun parseNumber(): Number {
            val start = index
            if (peekIf('-')) advance()
            if (peekIf('0')) {
                advance()
            } else {
                takeDigits()
            }
            var isFloat = false
            if (peekIf('.')) {
                isFloat = true
                advance()
                val digits = takeDigits()
                if (digits.isEmpty()) throw IllegalArgumentException("Invalid number format")
            }
            if (peekIf('e') || peekIf('E')) {
                isFloat = true
                advance()
                if (peekIf('+') || peekIf('-')) advance()
                val digits = takeDigits()
                if (digits.isEmpty()) throw IllegalArgumentException("Invalid exponent format")
            }
            val numberText = text.substring(start, index)
            if (isFloat) {
                return numberText.toDouble()
            }
            return try {
                val longValue = numberText.toLong()
                if (longValue in Int.MIN_VALUE..Int.MAX_VALUE) {
                    longValue.toInt()
                } else {
                    longValue
                }
            } catch (_: NumberFormatException) {
                BigInteger(numberText)
            }
        }

        private fun takeDigits(): String {
            val start = index
            while (!isAtEnd() && peek().isDigit()) {
                advance()
            }
            return text.substring(start, index)
        }

        private fun skipWhitespace() {
            while (!isAtEnd() && peek().isWhitespace()) {
                advance()
            }
        }

        private fun peek(): Char = text[index]

        private fun peekIf(expected: Char): Boolean =
            !isAtEnd() && text[index] == expected

        private fun advance(): Char = text[index++]

        private fun consume(expected: Char) {
            if (isAtEnd() || text[index] != expected) {
                throw IllegalArgumentException("Expected '$expected' at index $index")
            }
            index++
        }

        private fun expect(keyword: String) {
            for (ch in keyword) {
                consume(ch)
            }
        }

        private fun take(count: Int): String {
            if (index + count > text.length) {
                throw IllegalArgumentException("Unexpected end of input")
            }
            val start = index
            index += count
            return text.substring(start, index)
        }

        private fun isAtEnd(): Boolean = index >= text.length
    }
}
