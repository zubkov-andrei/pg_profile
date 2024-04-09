/**
 * The class is designed to highlight the selected row and other rows
 * with matching date attributes. When the user selects any row in the
 * report, the Highlighter class determines the set of date attributes
 * for the selected row and looks for matches in other rows in the
 * report. If the set of attributes completely match, then the lines
 * are highlighted.
 */

class Highlighter {
    static transition = 'background-color 70ms';

    /**
     * Method compares each data-attribute in target and in each row
     * and add active class if they fit.
     * @param tr
     * @param allRows
     */
    static toggleClass(tr, allRows) {
        /** Firstly, clean all active rows */
        Highlighter.cleanAllActiveClasses(allRows);

        /** If row has dataset and is not active (highlighted) */
        if (Object.keys(tr.dataset).length && !tr.classList.contains('active')) {
            allRows.forEach((row) => {
                /** Removing data-search attr from dataset */
                let isEqual = Highlighter.isDatasetEqual(tr, row);
                if (isEqual) {
                    /** Remove smart hover and highlight row */
                    Highlighter.setBackgroundColorToRow(row, '', this.transition);
                    row.classList.add('active');

                    /** Highlight navigator item */
                    let navId = this.getClosestTag(row, 0, 'div').firstChild.id;
                    if (navId) {
                        let navLi = document.getElementById(`navigator_${navId}`);
                        if (navLi && !navLi.classList.contains('active')) {
                            navLi.classList.add('active');
                        }
                    }
                }
            });
        }
    }

    static getAllRows() {
        return document.querySelectorAll('tr');
    }

    static getHighlightableRows() {
        return document.querySelectorAll('table.highlight tr:not(.queryRow)');
    }

    /**
     * The method is designed to determine the parent tag <tr> from the target tag
     * on which the user clicked. The method returns the parent tag <tr> or false if
     * not found.
     * Works like method htmlNode.closest('tag') but with additional logic
     * @param target - the tag the user clicked on.
     * @param curDeep - initial depth (to determine the depth of the recursion)
     * @param targetTag
     * @returns {*|boolean}
     */
    static getClosestTag(target, curDeep, targetTag) {
        let tooDeep = curDeep >= 5;
        let headOfTable = target.tagName.toLowerCase() === 'th';
        let stillNotRow = target.tagName.toLowerCase() !== targetTag;

        if (tooDeep) {
            return false;
        } else if (headOfTable) {
            return false;
        } else if (stillNotRow) {
            curDeep++;
            return Highlighter.getClosestTag(target.parentNode, curDeep, targetTag);
        } else {
            return target;
        }
    }

    static cleanAllActiveClasses(rows) {
        rows.forEach(elem => {
            if (elem.classList.contains('active')) {
                elem.classList.remove('active');
            }
        })
        let navigator = document.getElementById('navigator');
        if (navigator) {
            let allItems = document.querySelectorAll('li');
            allItems.forEach(item => {
                if (item.classList.contains('active')) {
                    item.classList.remove('active');
                }
            })
        }
    }

    static getTargetDS(tr) {
        let fieldsList = JSON.parse(tr.closest('table').dataset.highlight);
        let targetDS = {};
        fieldsList.forEach(field => {
            targetDS[field.id] = tr.dataset[field.id]
        })
        return targetDS;
    }
    /**
     * If datasets in target and in row are the same - highlight the row.
     * @param tr
     * @param row
     * @returns boolean
     */
    static isDatasetEqual(tr, row) {
        let targetDataset = Highlighter.getTargetDS(tr);
        let rowDataset = row.dataset;

        /** Highlighting statements texts. If data-queryid and (data-planid) in statement list match */
        let trIsSqlList = Highlighter.getClosestTag(tr, 0, 'table').id === 'sqllist_t';
        let rowIsSqlList = Highlighter.getClosestTag(row, 0, 'table').id === 'sqllist_t';

        let isSameQuery = targetDataset.hexqueryid !== undefined
            && targetDataset.hexqueryid === rowDataset.hexqueryid
            && targetDataset.planid === rowDataset.planid;

        if ((trIsSqlList || rowIsSqlList) && isSameQuery) {
            return true;
        }

        /** If at least one data in datasets doesn't match */
        for (let data in targetDataset) {
            if (targetDataset[data] === '*' && rowDataset[data] !== undefined) {
                continue;
            }
            if (data === 'all') {
                continue;
            }
            if (targetDataset[data] !== rowDataset[data]) {
                return false;
            }
        }

        return true;
    }

    static setBackgroundColorToRow(tr, hoverColor, transition) {
        tr.querySelectorAll('td').forEach(td => {
            td.style.backgroundColor = hoverColor;
            td.style.transition = transition;
        })

        let siblings = null;
        if (tr.classList.contains('int1')) {
            siblings = tr.nextSibling.querySelectorAll('td');
        } else if (tr.classList.contains('int2')) {
            siblings = tr.previousSibling.querySelectorAll('td');
        }
        if (siblings) {
            siblings.forEach(elem => {
                elem.style.backgroundColor = hoverColor;
                elem.style.transition = transition;
            })
        }
    }

    static highlight(event, allRows) {
        /** If user clicked not on link */
        if (event.target.tagName.toLowerCase() !== 'a') {
            let tr = Highlighter.getClosestTag(event.target, 0, 'tr');
            if (tr && Object.keys(tr.dataset).length) {
                Highlighter.toggleClass(tr, allRows);
            }
        }
    }

    static smartHover(eventType, event, transition) {
        let hoverColor = '#D9FFCC';
        let tr = Highlighter.getClosestTag(event.target, 0, 'tr');

        if (tr && !tr.classList.contains('active') && eventType === 'mouseover') {
            Highlighter.setBackgroundColorToRow(tr, hoverColor, transition);
        } else if (tr && eventType === 'mouseout') {
            Highlighter.setBackgroundColorToRow(tr, '', transition);
        }
    }

    static init() {
        const ALL_ROWS = Highlighter.getAllRows();
        const HIGHLIGHTABLE_ROWS = Highlighter.getHighlightableRows();

        /** Highlighting chosen (and related) row */
        HIGHLIGHTABLE_ROWS.forEach((elem) => {
            elem.addEventListener('click', (event) => {
                Highlighter.highlight(event, HIGHLIGHTABLE_ROWS);
            });
        })

        /** Smart hover */
        ALL_ROWS.forEach((elem) => {
            ['mouseover', 'mouseout'].forEach(eventType => {
                elem.addEventListener(eventType, (event) => {
                    Highlighter.smartHover(eventType, event, this.transition);
                });
            })
        })
    }
}
