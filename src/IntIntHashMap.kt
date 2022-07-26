import kotlinx.atomicfu.*

/**
 * Int-to-Int hash map with open addressing and linear probes.
 *
 * This class is thread-safe.
 */
class IntIntHashMap {
    private val core : AtomicRef<Core> = atomic(Core(INITIAL_CAPACITY))

    /**
     * Returns value for the corresponding key or zero if this key is not present.
     *
     * @param key a positive key.
     * @return value for the corresponding or zero if this key is not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    operator fun get(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(core.value.getInternal(key))
    }

    /**
     * Changes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key   a positive key.
     * @param value a positive value.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key or value are not positive, or value is equal to
     * [Integer.MAX_VALUE] which is reserved.
     */
    fun put(key: Int, value: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        require(isValue(value)) { "Invalid value: $value" }
        return toValue(putAndRehashWhileNeeded(key, value))
    }

    /**
     * Removes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key a positive key.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    fun remove(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(putAndRehashWhileNeeded(key, DEL_VALUE))
    }

    private fun putAndRehashWhileNeeded(key: Int, value: Int): Int {
        while (true) {
            val c = core.value
            val oldValue = c.putInternal(key, value)
            if (oldValue != NEEDS_REHASH) return oldValue
            core.compareAndSet(c, c.rehash())
        }
    }

    private class Core constructor(capacity: Int) {
        val map: AtomicIntArray = AtomicIntArray(2 * capacity)
        val next: AtomicRef<Core?> = atomic(null)
        val shift: Int
        val rehashIndex : AtomicInt = atomic(2 * capacity - 2)
        val rehashCount : AtomicInt = atomic(0)

        init {
            val mask = capacity - 1
            assert(mask > 0 && mask and capacity == 0) { "Capacity must be power of 2: $capacity" }
            shift = 32 - Integer.bitCount(mask)
        }

        fun getInternal(key: Int): Int {
            var index = index(key)
            var probes = 0

            while (true) {
                val keyValue = map[index].value
                if (keyValue == key) break
                if (keyValue == NULL_KEY) return NULL_VALUE
                if (++probes >= MAX_PROBES) return NULL_VALUE
                index = nextIndex(index)
            }

            val value = map[index + 1].value
            if (value == S) return next.value!!.getInternal(key)
            return if (value < 0) defrostV(value) else value
        }

        fun putInternal(key: Int, value: Int): Int {
            var index = index(key)
            var probes = 0

            while (true) {
                val keyValue = map[index].value
                if (keyValue == key) break
                if (keyValue == NULL_KEY) {
                    if (map[index].compareAndSet(NULL_KEY, key)) break
                    continue
                }
                if (++probes >= MAX_PROBES) return NEEDS_REHASH
                index = nextIndex(index)
            }

            while (true) {
                val oldValue = map[index + 1].value
                if (oldValue == S) return next.value!!.putInternal(key, value)
                if (oldValue < 0) return NEEDS_REHASH
                if (map[index + 1].compareAndSet(oldValue, value)) return oldValue
            }
        }

        fun rehash(): Core {
            next.compareAndSet(null, Core(map.size))
            val newCore = next.value!!

            while (rehashIndex.value >= 0) {
                val index = rehashIndex.getAndAdd(-2)
                if (index >= 0) {
                    moveCell(newCore, index)
                    rehashCount.getAndAdd(2)
                }
            }
            while (rehashCount.value < map.size);

            return newCore
        }


        private fun moveCell(core : Core, index : Int) {
            while (true) {
                val value = map[index + 1].value
                if (map[index + 1].compareAndSet(value, freezeV(value))) {
                    if (value == DEL_VALUE || value <= 0) break

                    val key = map[index].value
                    var newIndex = core.index(key)
                    while (true) {
                        if (core.map[newIndex].compareAndSet(NULL_KEY, key)) {
                            core.map[newIndex + 1].value = value
                            map[index + 1].value = S
                            break
                        }
                        newIndex = core.nextIndex(newIndex)
                    }
                    break
                }
            }
        }

        private fun index(key: Int): Int = (key * MAGIC ushr shift) * 2
        private fun nextIndex(index: Int) : Int = (index + 2) and (map.size - 1)
        private fun freezeV(V : Int) : Int = V or Int.MIN_VALUE
        private fun defrostV(V : Int) : Int = V and Int.MAX_VALUE
    }
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val INITIAL_CAPACITY = 2 // !!! DO NOT CHANGE INITIAL CAPACITY !!!
private const val MAX_PROBES = 8 // max number of probes to find an item
private const val NULL_KEY = 0 // missing key (initial value)
private const val NULL_VALUE = 0 // missing value (initial value)
private const val DEL_VALUE = Int.MAX_VALUE // mark for removed value
private const val NEEDS_REHASH = -1 // returned by `putInternal` to indicate that rehash is needed
private const val S = Int.MIN_VALUE // note about the transferred value

private fun isValue(value: Int): Boolean = value in (1 until DEL_VALUE)

private fun toValue(value: Int): Int = if (isValue(value)) value else 0
