// from https://github.com/qinhy/singleton-key-value-storage.git
import { SingletonKeyValueStorage } from "./Storage.js"
class Tests {
  constructor() {
    this.store = new SingletonKeyValueStorage()
  }

  testAll(num = 1) {
    this.testLocalStorage(num)
  }

  testLocalStorage(num = 1) {
    this.store.tempTsBackend()
    for (let i = 0; i < num; i++) this.testAllCases()
  }

  testAllCases() {
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
      "Should return the correct keys and values."
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
    store2.tempTsBackend()

    this.store.addSlave(store2)
    this.store.set("alpha", { info: "first" })
    this.store.set("abeta", { info: "second" })
    this.store.set("gamma", { info: "third" })
    this.store.delete("abeta")

    console.assert(
      JSON.stringify(JSON.parse(this.store.dumps()).gamma) ===
        JSON.stringify(JSON.parse(store2.dumps()).gamma),
      "Should return the correct keys and values."
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
      const smallPayload = makeBigPayload(90) // ~0.09 MiB
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
