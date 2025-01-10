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

    /** Advanced filter */
    static filter(data, key) {
        if (key.type === "exists") {
            if (data.every(obj => key["field"] in obj)) {
                return structuredClone(data.filter(obj => obj[key["field"]]));
            }
        } else if (key.type === "equal") {
            if (data.every(obj => key["field"] in obj)) {
                return structuredClone(data.filter(obj => obj[key["field"]] === key["value"]));
            }
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

    static getInputField() {
        return document.getElementById('inputField');
    }

    static cancelSearchResults(rowsForSearch) {
        rowsForSearch.forEach(row => {
            row.style.display = '';
        })
        this.getInputField().value = '';
    }

    static searchQueryWithStatistics(rowsForSearch, keyword) {
        let foundQueries = Utilities.searchQueryText(keyword);

        rowsForSearch.forEach(row => {
            if (row.dataset["hexqueryid"]
                && foundQueries[row.dataset["hexqueryid"]]) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
                if (row.nextSibling && row.nextSibling.classList.contains('queryRow')) {
                    row.nextSibling.style.display = 'none';
                }
            }
        })
    }

    static searchQueryText(keyword) {
        let foundQueries = {};
        data.datasets.queries.forEach(query => {
            /** Search in query texts */
            Object.keys(query).forEach(key => {
                query.query_texts.forEach(query_text => {
                    if (query_text && query_text.toLowerCase().includes(keyword) && !foundQueries[query["hexqueryid"]]) {
                        foundQueries[query["hexqueryid"]] = true;
                    }
                })
            })
            /** Search in plan texts */
            if (query.plans) {
                query.plans.forEach(plan => {
                    if (plan.plan_text.toLowerCase().includes(keyword) && !foundQueries[query["hexqueryid"]]) {
                        foundQueries[query["hexqueryid"]] = true;
                    }
                })
            }
        })
        return foundQueries;
    }

    static searchWithParam(rowsForSearch, keyword, searchParam) {
        let foundQueries = {};
        /** if we search everywhere, then first we need to 
         * find the keyword in the query texts, and then 
         * we display all the lines related to this query
         */
        if (searchParam === 'all') {
            foundQueries = Utilities.searchQueryText(keyword)
        }

        rowsForSearch.forEach(row => {
            /** If dataset[searchParam] exists and has substring with keyword */
            if (row.dataset[searchParam] && row.dataset[searchParam].toLowerCase().includes(keyword)) {
                row.style.display = '';
                /** If dataset[searchParam] has hexqueryid, then put it into foundQueries collection */
                if (row.dataset["hexqueryid"]) {
                    foundQueries[row.dataset["hexqueryid"]] = true;
                }
            } else {
                row.style.display = 'none';
                if (row.nextSibling && row.nextSibling.classList.contains('queryRow')) {
                    row.nextSibling.style.display = 'none';
                }
            }
        })

        rowsForSearch.forEach(row => {
            /** If a row from a table with query texts or a search parameter data-all */
            if (row.parentNode.id === 'sqllist_t' || searchParam === 'all') {
                /** Check foundQueries, if such index exists, then */
                if (foundQueries[row.dataset["hexqueryid"]]) {
                    row.style.display = '';
                }
            } else {
                /** Otherwise, we check for a match between data-hexqueryid and the presence of a key phrase in dataset[searchParam]*/
                if (foundQueries[row.dataset["hexqueryid"]] && row.dataset[searchParam].toLowerCase().includes(keyword)) {
                    row.style.display = '';
                }
            }
        })
    }
    
    static search(rowsForSearch, searchParam, keyword) {
        keyword = keyword.toLowerCase();

        if (!keyword) {
            Utilities.cancelSearchResults(rowsForSearch);
        } else if (searchParam === 'querytext') {
            Utilities.searchQueryWithStatistics(rowsForSearch, keyword);
        } else {
            Utilities.searchWithParam(rowsForSearch, keyword, searchParam);
        }
    }
}