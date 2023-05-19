class Utilities {
    /**
     * Sorting JSON array and returning sorted clone with array of Objects
     * @param data JSON array
     * @param key string with key for sorting
     * @param direction direction of sorting (1 means ASC, -1 means DESC)
     * @returns array of Objects
     */
    static sort(data, key, direction) {
        return structuredClone(data.sort((a, b) => {
            /** Order index */
            if (a[key] < b[key]) {
                return -1 * direction;
            } else if (a[key] > b[key]) {
                return direction;
            } else {
                return 0;
            }
        }))
    }

    static sum(data, key) {
        return data.reduce((partialSum, a) => partialSum + a[key], 0);
    }
    /** Filter by boolean key */
    static filter(data, key) {
        if (data.every(obj => key in obj)) {
            return structuredClone(data.filter(obj => obj[key]));
        }
        return data;
    }
    static find(data, key, value) {
        return structuredClone(data.filter(obj => obj[key] === value));
    }
    /** Limit array of Objects */
    static limit(data, num) {
        if (num > 0) {
            return structuredClone(data.slice(0, num));
        }
        return data;
    }
}