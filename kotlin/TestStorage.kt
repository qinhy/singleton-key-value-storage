import java.nio.file.Files
import java.nio.file.Path

private fun fail(message: String): Nothing = throw AssertionError(message)

private fun assertTrue(condition: Boolean, message: String? = null) {
    if (!condition) {
        fail(message ?: "Expected condition to be true.")
    }
}

private fun assertFalse(condition: Boolean, message: String? = null) =
    assertTrue(!condition, message ?: "Expected condition to be false.")

private fun assertNull(value: Any?, message: String? = null) {
    if (value != null) {
        fail(message ?: "Expected value to be null but was <$value>.")
    }
}

private fun assertEquals(expected: Any?, actual: Any?, message: String? = null) {
    if (expected != actual) {
        val suffix = message?.let { " - $it" } ?: ""
        fail("Expected <$expected> but was <$actual>$suffix")
    }
}

class Tests {
    private val store = SingletonKeyValueStorage()

    fun testAll(num: Int = 1) {
        repeat(num) { testDict() }
    }

    fun testFile(num: Int = 1) {
        println("###### test_file ######")
        repeat(num) { testAllCases() }
    }

    fun testDict(num: Int = 1) {
        println("###### test_dict ######")
        store.switchBackend(DictStorage.build())
        testMsg()
        repeat(num) { testAllCases() }
    }

    fun testMsg() {
        println("start : test_msg()")

        store.messageQueue.push(mutableMapOf("n" to 1))
        store.messageQueue.push(mutableMapOf("n" to 2))
        store.messageQueue.push(mutableMapOf("n" to 3))

        assertEquals(3, store.messageQueue.queueSize(), "Size should reflect number of enqueued items.")
        assertEquals(mapOf("n" to 1), store.messageQueue.pop() as Map<*, *>?, "Queue must be FIFO: first pop returns first pushed.")
        assertEquals(mapOf("n" to 2), store.messageQueue.pop() as Map<*, *>?, "Second pop should return second item.")
        assertEquals(mapOf("n" to 3), store.messageQueue.pop() as Map<*, *>?, "Third pop should return third item.")
        assertNull(store.messageQueue.pop(), "Popping an empty queue should return null.")
        assertEquals(0, store.messageQueue.queueSize(), "Size should be zero after popping all items.")

        store.messageQueue.push(mutableMapOf("a" to 1))
        assertEquals(mapOf("a" to 1), store.messageQueue.peek() as Map<*, *>?, "Peek should return earliest message without removing it.")
        assertEquals(1, store.messageQueue.queueSize(), "Peek should not change the queue size.")
        assertEquals(mapOf("a" to 1), store.messageQueue.pop() as Map<*, *>?, "Pop should still return the same earliest message after peek.")

        store.messageQueue.push(mutableMapOf("x" to 1))
        store.messageQueue.push(mutableMapOf("y" to 2))
        store.messageQueue.clear()
        assertEquals(0, store.messageQueue.queueSize(), "Clear should remove all items from the queue.")
        assertNull(store.messageQueue.pop(), "After clear, popping should return null.")

        val events = mutableListOf<EventInvocation>()
        val capture: EventListener = { invocation -> events += invocation }

        store.messageQueue.addListener("default", capture, eventKind = "pushed")
        store.messageQueue.addListener("default", capture, eventKind = "popped")
        store.messageQueue.addListener("default", capture, eventKind = "empty")
        store.messageQueue.addListener("default", capture, eventKind = "cleared")

        store.messageQueue.push(mutableMapOf("m" to 1))
        store.messageQueue.push(mutableMapOf("m" to 2))
        store.messageQueue.pop()
        store.messageQueue.pop()
        store.messageQueue.clear()

        val queue = "t_listener_fail_${this::class.simpleName}"
        val badListener: EventListener = { throw RuntimeException("boom") }
        store.messageQueue.addListener(queue, badListener, eventKind = "pushed")

        store.messageQueue.push(mutableMapOf("ok" to true), queueName = queue)
        assertEquals(1, store.messageQueue.queueSize(queue), "ops should succeed even if a listener fails.")
        assertEquals(mapOf("ok" to true), store.messageQueue.pop(queue) as Map<*, *>?)

        store.messageQueue.push(mutableMapOf("a" to 1), queueName = "q1")
        store.messageQueue.push(mutableMapOf("b" to 2), queueName = "q2")

        assertEquals(1, store.messageQueue.queueSize("q1"), "q1 should have one item.")
        assertEquals(1, store.messageQueue.queueSize("q2"), "q2 should have one item.")
        assertEquals(mapOf("a" to 1), store.messageQueue.pop("q1") as Map<*, *>?, "Popping q1 should return its own item.")
        assertEquals(1, store.messageQueue.queueSize("q2"), "Popping q1 should not affect q2.")
    }

    fun testAllCases() {
        println("start : self.test_set_and_get()")
        testSetAndGet()
        println("start : self.test_exists()")
        testExists()
        println("start : self.test_delete()")
        testDelete()
        println("start : self.test_keys()")
        testKeys()
        println("start : self.test_get_nonexistent()")
        testGetNonexistent()
        println("start : self.test_dump_and_load()")
        testDumpAndLoad()
        println("start : self.test_version()")
        testVersion()
        println("start : self.test_slaves()")
        testSlaves()
        println("start : self.store.clean()")
        store.clean()
        println("end all.")
    }

    fun testSetAndGet() {
        store.set("test1", mutableMapOf("data" to 123))
        assertEquals(mapOf("data" to 123), store.get("test1") as Map<*, *>?)
    }

    fun testExists() {
        store.set("test2", mutableMapOf("data" to 456))
        assertTrue(store.exists("test2"), "Key should exist after being set.")
    }

    fun testDelete() {
        store.set("test3", mutableMapOf("data" to 789))
        store.delete("test3")
        assertFalse(store.exists("test3"), "Key should not exist after being deleted.")
    }

    fun testKeys() {
        store.set("alpha", mutableMapOf("info" to "first"))
        store.set("abeta", mutableMapOf("info" to "second"))
        store.set("gamma", mutableMapOf("info" to "third"))
        val expected = listOf("alpha", "abeta")
        assertEquals(expected.sorted(), store.keys("a*").sorted())
    }

    fun testGetNonexistent() {
        assertNull(store.get("nonexistent"), "Getting a non-existent key should return null.")
    }

    fun testDumpAndLoad() {
        val raw = mapOf(
            "test1" to mapOf("data" to 123),
            "test2" to mapOf("data" to 456),
            "alpha" to mapOf("info" to "first"),
            "abeta" to mapOf("info" to "second"),
            "gamma" to mapOf("info" to "third")
        )
        val path = "test.json"
        store.dump(path)

        store.clean()
        assertEquals("{}", store.dumps(), "Should return {} after clean.")

        store.load(path)
        assertJsonEquals(raw, store.dumps())

        store.clean()
        store.loads(SimpleJson.stringify(raw))
        assertJsonEquals(raw, store.dumps())

        Files.deleteIfExists(Path.of(path))
    }

    fun testSlaves() {
        val store2 = SingletonKeyValueStorage()
        store2.switchBackend(DictStorage.buildTmp())
        store.addSlave(store2)
        store.set("alpha", mutableMapOf("info" to "first"))
        store.set("abeta", mutableMapOf("info" to "second"))
        store.set("gamma", mutableMapOf("info" to "third"))
        store.delete("abeta")

        assertEquals(
            SimpleJson.parse(store.dumps()),
            SimpleJson.parse(store2.dumps()),
            "Should return the correct keys and values."
        )
    }

    fun testVersion() {
        store.clean()
        store.versionControl = true
        store.set("alpha", mutableMapOf("info" to "first"))
        val data1 = store.dumps()
        val v1 = store.getCurrentVersion()
        val data1Map = parseJsonObject(data1)

        store.set("abeta", mutableMapOf("info" to "second"))
        val v2 = store.getCurrentVersion()
        val data2 = store.dumps()
        val data2Map = parseJsonObject(data2)

        store.set("gamma", mutableMapOf("info" to "third"))
        if (v1 != null) {
            store.localToVersion(v1)
        }

        assertJsonEquals(data1Map, store.dumps())

        if (v2 != null) {
            store.localToVersion(v2)
        }
        assertJsonEquals(data2Map, store.dumps())

        fun makeBigPayload(sizeKb: Int): String = "X".repeat(1024 * sizeKb)
        store.versionController.limitMemoryMB = 0.2
        val controller = store.versionController

        repeat(3) { idx ->
            val payload = makeBigPayload(62)
            val res = controller.addOperation(
                listOf("write", "small_$idx", payload),
                listOf("delete", "small_$idx")
            )
            assertNull(res, "Should not return any warning message for small payloads.")
        }

        val bigPayload = makeBigPayload(600)
        val res = controller.addOperation(
            listOf("write", "too_big", bigPayload),
            listOf("delete", "too_big")
        )
        val expected = "[LocalVersionController] Warning: memory usage"
        requireNotNull(res)
        assertTrue(res.startsWith(expected), "Should return warning message about memory usage.")
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertJsonEquals(expected: Map<String, Any?>, jsonString: String) {
        val parsed = SimpleJson.parse(jsonString) as? Map<String, Any?>
            ?: fail("Expected JSON object when parsing <$jsonString>.")
        assertEquals(expected, parsed)
    }

    private fun parseJsonObject(json: String): Map<String, Any?> {
        val parsed = SimpleJson.parse(json)
        if (parsed !is Map<*, *>) {
            fail("Expected JSON object when parsing <$json>.")
        }
        @Suppress("UNCHECKED_CAST")
        return parsed as Map<String, Any?>
    }
}

fun main() {
    Tests().testAll()
}
