class Sorter {
    /**
     * Get all <th> tags with id
     * @returns {NodeListOf<Element>}
     */
    static getAllHeaders() {
        return document.querySelectorAll('th[id]');
    }
    static getClosestTag(target, curDeep, targetTag) {
        let tooDeep = curDeep >= 5;
        let stillNotRow = target.tagName.toLowerCase() !== targetTag;

        if (tooDeep) {
            return false;
        } else if (stillNotRow) {
            curDeep++;
            return Highlighter.getClosestTag(target.parentNode, curDeep, targetTag);
        } else {
            return target;
        }
    }
    static drawTriangle(elem) {
        let div = document.createElement('div');
        div.setAttribute('class', 'triangle-down');
        elem.appendChild(div);
    }
    static sort(tableId, sortingKey, sortingDirection) {
        console.log(tableId);
        console.log(sortingKey);
        console.log(sortingDirection);
    }
    static onClick(event, elem) {
        let triangle = elem.querySelector('div[class*="triangle"]');
        let sortingDirection;
        if (triangle) {
            if (triangle.classList.contains('triangle-down')) {
                triangle.setAttribute('class', 'triangle-up');
                sortingDirection = 1;
            } else {
                triangle.setAttribute('class', 'triangle-down');
                sortingDirection = -1;
            }
            let table = Sorter.getClosestTag(elem, 0, 'table');
            Sorter.sort(table.id, elem.id, sortingDirection);
        }
    }

    static init() {
        const ALL_HEADERS = Sorter.getAllHeaders();
        ALL_HEADERS.forEach(elem => {
            Sorter.drawTriangle(elem);
            elem.addEventListener('click', (event) => {
                Sorter.onClick(event, elem);
            });
        })
    }
}