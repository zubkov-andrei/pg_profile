/**
 * The class is designed to highlight the selected row and other rows
 * with matching date attributes. When the user selects any row in the
 * report, the Highlighter class determines the set of date attributes
 * for the selected row and looks for matches in other rows in the
 * report. If the set of attributes completely match, then the lines
 * are highlighted.
 */

class Highlighter {
    /**
     * Method compares each data-attribute in target and in each row
     * and add active class if they fit.
     * @param tr
     * @param allRows
     */
    static toggleClass(tr, allRows) {
        if (!tr.classList.contains('active')) {
            Highlighter.cleanAllActiveClasses(allRows);

            /** Create message and send message If popup feature is active */
            if (Popup.getInstance()) {
                const notice = document.createElement('div');
                const p = document.createElement('p');
                p.textContent = 'Highlighted row attributes:'
                notice.appendChild(p);

                const table = document.createElement('table');
                Object.keys(tr.dataset).forEach(key => {
                    let _tr = table.insertRow(-1);
                    let tdKey = _tr.insertCell(-1);
                    tdKey.innerHTML = key;
                    let tdVal = _tr.insertCell(-1);
                    tdVal.innerHTML = tr.dataset[key];
                });
                notice.appendChild(table);

                Popup.sendNotice(Popup.STYLE.BANNER, notice);
            }
            allRows.forEach((elem) => {
                let isEqual = Highlighter.isDatasetEqual(tr.dataset, elem.dataset, elem);
                if (isEqual) {
                    elem.classList.add('active');
                }
            });
        } else {
            Highlighter.cleanAllActiveClasses(allRows);
            /** Hide popup if popup feature is active */
            if (Popup.getInstance()) {
                Popup.popupDisappearing();
            }
        }
    }

    static getAllRows() {
        return document.querySelectorAll('tr');
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
    }

    /**
     * If datasets in target and in row are the same - highlight the row.
     * TODO: Продумать как подсвечивать отдельные ячейки при неполном совпадении дата-атрибутов
     * @param targetDataset
     * @param rowDataset
     * @param elem
     * @returns boolean
     */
    static isDatasetEqual(targetDataset, rowDataset, elem) {
        /** If no data in dataset then return */
        if (!Object.keys(targetDataset).length) {
            return false;
        }
        /** Highlighting statements texts. If data-queryid and (data-planid) in statement list match */
        let tableIsSqlList = Highlighter.getClosestTag(elem, 0, 'table').id === 'sqllist_t';
        let isSameQuery = targetDataset.queryid !== undefined
            && targetDataset.hexqueryid === rowDataset.hexqueryid
            && targetDataset.planid === rowDataset.planid;

        if (tableIsSqlList && isSameQuery) {
            return true;
        }

        /** If at least one data in datasets doesn't match */
        for (let data in targetDataset) {
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

    static smartHover(eventType, event) {
        let hoverColor = '#D9FFCC';
        let transition = 'background-color 300ms';
        let tr = Highlighter.getClosestTag(event.target, 0, 'tr');

        if (tr && eventType === 'mouseover') {
            Highlighter.setBackgroundColorToRow(tr, hoverColor, transition);
        } else if (tr && eventType === 'mouseout') {
            Highlighter.setBackgroundColorToRow(tr, '', transition);
        }
    }

    static init() {
        const ALL_ROWS = Highlighter.getAllRows();
        ALL_ROWS.forEach((elem) => {

            /** Highlighting chosen (and related) row */
            elem.addEventListener('click', (event) => {
                Highlighter.highlight(event, ALL_ROWS);
            });

            /** Smart hover */
            ['mouseover', 'mouseout'].forEach(eventType => {
                elem.addEventListener(eventType, (event) => {
                    Highlighter.smartHover(eventType, event);
                });
            })
        })
    }
}
