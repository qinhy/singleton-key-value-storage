// from https://github.com/qinhy/singleton-key-value-storage.git
import { DictStorage, SingletonKeyValueStorage } from "./Storage.js"
class Tests {
  constructor() {
    this.store = new SingletonKeyValueStorage()
  }

  testAll(num = 1) {
    this.testLocalStorage(num)
  }

  testLocalStorage(num = 1) {
    this.store.switchBackend(DictStorage.buildTmp())
    for (let i = 0; i < num; i++) this.testAllCases()
  }

  testAllCases() {
    this.testMsg()
    this.testSetAndGet()
    this.testExists()
    this.testDelete()
    this.testKeys()
    this.testGetNonexistent()
    this.testDumpAndLoad()
    this.testVersion()
    this.testSlaves()
    this.store.clean()
    console.log("All tests end.")
  }
  // Inside the same class where `this.store` is available
  // (no `private` in JS; keep as regular methods)

  testMsg() {
    // Enqueue a few messages
    this.store.messageQueue.push({ n: 1 });
    this.store.messageQueue.push({ n: 2 });
    this.store.messageQueue.push({ n: 3 });

    console.assert(
      this.store.messageQueue.queueSize() === 3,
      "Size should reflect number of enqueued items."
    );

    // FIFO pops
    const p1 = this.store.messageQueue.pop();
    console.assert(
      JSON.stringify(p1) === JSON.stringify({ n: 1 }),
      "Queue must be FIFO: first pop returns first pushed."
    );

    const p2 = this.store.messageQueue.pop();
    console.assert(
      JSON.stringify(p2) === JSON.stringify({ n: 2 }),
      "Second pop should return second item."
    );

    const p3 = this.store.messageQueue.pop();
    console.assert(
      JSON.stringify(p3) === JSON.stringify({ n: 3 }),
      "Third pop should return third item."
    );

    const pEmpty = this.store.messageQueue.pop();
    console.assert(
      pEmpty == null,
      "Popping an empty queue should return null/undefined."
    );

    console.assert(
      this.store.messageQueue.queueSize() === 0,
      "Size should be zero after popping all items."
    );

    // Peek does not remove
    this.store.messageQueue.push({ a: 1 });
    const peeked = this.store.messageQueue.peek();
    console.assert(
      JSON.stringify(peeked) === JSON.stringify({ a: 1 }),
      "Peek should return earliest message without removing it."
    );
    console.assert(
      this.store.messageQueue.queueSize() === 1,
      "Peek should not change the queue size."
    );
    const poppedAfterPeek = this.store.messageQueue.pop();
    console.assert(
      JSON.stringify(poppedAfterPeek) === JSON.stringify({ a: 1 }),
      "Pop should still return the same earliest message after peek."
    );

    // Clear resets
    this.store.messageQueue.push({ x: 1 });
    this.store.messageQueue.push({ y: 2 });
    this.store.messageQueue.clear();
    console.assert(
      this.store.messageQueue.queueSize() === 0,
      "Clear should remove all items from the queue."
    );
    console.assert(
      this.store.messageQueue.pop() == null,
      "After clear, popping should return null/undefined."
    );

    // Capture normal event flow on default queue
    const events = [];
    const capture = (evt) => events.push(evt);

    this.store.messageQueue.addListener('default', capture, 'pushed' );
    this.store.messageQueue.addListener('default', capture, 'popped' );
    this.store.messageQueue.addListener('default', capture, 'empty' );
    this.store.messageQueue.addListener('default', capture, 'cleared' );

    this.store.messageQueue.push({ m: 1 });
    this.store.messageQueue.push({ m: 2 });
    const a = this.store.messageQueue.pop();
    const b = this.store.messageQueue.pop();
    this.store.messageQueue.clear();

    const kinds = events.map(e => e.op);
    // const expected = ['push', 'push', 'pop', 'pop', 'empty', 'clear'];
    // console.assert(
    //   JSON.stringify(kinds) === JSON.stringify(expected),
    //   "Should dispatch pushed, popped (twice), empty, then cleared in order."
    // );
    console.assert(
      JSON.stringify(a) === JSON.stringify({ m: 1 }),
      "First popped message should equal the first pushed."
    );
    console.assert(
      JSON.stringify(b) === JSON.stringify({ m: 2 }),
      "Second popped message should equal the second pushed."
    );

    // Listener failure should not break queue ops (use isolated queue)
    const queue = `t_listener_fail_${Date.now()}`;
    const bad = (_evt) => { throw new Error("boom"); };

    this.store.messageQueue.addListener(queue, bad, 'pushed' );

    // Should not throw even though the listener throws
    this.store.messageQueue.push({ ok: true }, queue);
    console.assert(
      this.store.messageQueue.queueSize(queue) === 1,
      "Ops should succeed even if a listener fails."
    );
    console.assert(
      JSON.stringify(this.store.messageQueue.pop(queue)) === JSON.stringify({ ok: true }),
      "Pop should return the pushed item from the isolated queue."
    );
    this.store.messageQueue.push({ a: 1 }, 'q1');
    this.store.messageQueue.push({ b: 2 }, 'q2');

    console.assert(
      this.store.messageQueue.queueSize('q1') === 1,
      "q1 should have one item."
    );
    console.assert(
      this.store.messageQueue.queueSize('q2') === 1,
      "q2 should have one item."
    );
    console.assert(
      JSON.stringify(this.store.messageQueue.pop('q1')) === JSON.stringify({ a: 1 }),
      "Popping q1 should return its own item."
    );
    console.assert(
      this.store.messageQueue.queueSize('q2') === 1,
      "Popping q1 should not affect q2."
    );
  }

  testSetAndGet() {
    this.store.set("test1", { data: 123 })
    console.assert(
      JSON.stringify(this.store.get("test1")) === JSON.stringify({ data: 123 }),
      "The retrieved value should match the set value."
    )
  }

  testExists() {
    this.store.set("test2", { data: 456 })
    console.assert(
      this.store.exists("test2") === true,
      "Key should exist after being set."
    )
  }

  testDelete() {
    this.store.set("test3", { data: 789 })
    this.store.delete("test3")
    console.assert(
      this.store.exists("test3") === false,
      "Key should not exist after being deleted."
    )
  }

  testKeys() {
    this.store.set("alpha", { info: "first" })
    this.store.set("abeta", { info: "second" })
    this.store.set("gamma", { info: "third" })
    const expectedKeys = ["alpha", "abeta"]
    console.assert(
      JSON.stringify(this.store.keys("a*").sort()) ===
      JSON.stringify(expectedKeys.sort()),
      "Should return the correct keys matching the pattern."
    )
  }

  testGetNonexistent() {
    console.assert(
      this.store.get("nonexistent") === null,
      "Getting a non-existent key should return null."
    )
  }

  testDumpAndLoad() {
    const raw = {
      test1: { data: 123 }
    }

    this.store.clean()    
    console.assert(
      this.store.dumps() === "{}",
      "Should return empty."
    )

    this.store.clean()
    this.store.loads(JSON.stringify(raw))
    console.assert(
      JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(raw),
      "Should return the correct keys and values."
    )
  }

  testSlaves() {
    this.store.clean()
    this.store.loads(
      JSON.stringify({
        alpha: { info: "first" },
        abeta: { info: "second" },
        gamma: { info: "third" }
      })
    )

    if (
      this.store.conn?.constructor.name === "SingletonTsDictStorageController"
    )
      return

    const store2 = new SingletonKeyValueStorage()
    store2.switchBackend(DictStorage.buildTmp())

    this.store.addSlave(store2)    
    this.store.set("alpha", { info: "first" })
    this.store.set("abeta", { info: "second" })
    this.store.set("gamma", { info: "third" })
    this.store.delete("abeta")
    console.assert(
      JSON.stringify(JSON.parse(this.store.dumps()).gamma) ===
      JSON.stringify(JSON.parse(store2.dumps()).gamma),
      "test slaves, Should return the correct keys and values."
    )
  }

  testVersion() {
    // Match Python ordering: clean first, then enable version control
    this.store.clean()
    this.store.versionControl = true

    // 1) Create v1
    this.store.set("alpha", { info: "first" })
    const data1 = this.store.dumps()
    const v1 = this.store.getCurrentVersion()

    // 2) Create v2
    this.store.set("abeta", { info: "second" })
    const v2 = this.store.getCurrentVersion()
    const data2 = this.store.dumps()

    // 3) Create another op, then jump back/forward between versions
    this.store.set("gamma", { info: "third" })

    // Helpers to compare JSON objects irrespective of key order
    const stableStringify = value => {
      const sortKeys = v => {
        if (Array.isArray(v)) return v.map(sortKeys)
        if (v && typeof v === "object") {
          return Object.keys(v)
            .sort()
            .reduce((acc, k) => {
              acc[k] = sortKeys(v[k])
              return acc
            }, {})
        }
        return v
      }
      return JSON.stringify(sortKeys(value))
    }

    // local_to_version(v1)
    this.store.localToVersion(v1)
    console.assert(
      stableStringify(JSON.parse(this.store.dumps())) ===
      stableStringify(JSON.parse(data1)),
      "Should return the same keys and values for v1."
    )

    // local_to_version(v2)
    this.store.localToVersion(v2)
    console.assert(
      stableStringify(JSON.parse(this.store.dumps())) ===
      stableStringify(JSON.parse(data2)),
      "Should return the same keys and values for v2."
    )

    // ---- Memory limit test ----
    const makeBigPayload = sizeKiB => "X".repeat(1024 * sizeKiB)

    // Set a tight limit (0.2 MB) like the Python test
    this.store.versionController.limitMemoryMB = 0.2
    const lvc2 = this.store.versionController

    // Three small payloads (~0.09 MB each) should not trigger a warning
    for (let i = 0; i < 3; i++) {
      const smallPayload = makeBigPayload(62) // ~0.062 MiB
      const res = lvc2.addOperation(
        ["write", `small_${i}`, smallPayload],
        ["delete", `small_${i}`]
      )
      console.assert(
        res === null,
        "Should not return any warning message for small payloads."
      )
    }

    // One big payload (~0.6 MB) should trigger the warning prefix
    const bigPayload = makeBigPayload(600) // ~0.59 MiB
    const res = lvc2.addOperation(
      ["write", "too_big", bigPayload],
      ["delete", "too_big"]
    )
    const expectPrefix = "[LocalVersionController] Warning: memory usage"
    console.assert(
      typeof res === "string" &&
      res.slice(0, expectPrefix.length) === expectPrefix,
      "Should return warning message about memory usage."
    )
  }
}

// Running tests
new Tests().testAll()
