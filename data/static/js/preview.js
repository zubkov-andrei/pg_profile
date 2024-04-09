/**
 * The class is designed to instantly preview the query text referenced by the selected row
 */
class Previewer {
    static getParentRows() {
        return document.querySelectorAll("table.preview tr:not(.header)");
    }

    static preprocessQueryString(queryString) {
        let etc = '';
        queryString = queryString.split(',').join(', ');
        queryString = queryString.split('+').join(' + ');
        queryString = queryString.split('/').join(' / ');

        /** Max length = 1000 chars */
        if (queryString.length > 1000) {
            queryString = queryString.substring(0, 1000);
            etc = ' ...'
        }

        return `${queryString}${etc}`
    }

    static queryTextPreviewer(queryCell, queryRow, newRow, queryString) {
        queryCell.style.width = `${Math.floor(newRow.offsetWidth * 0.95)}px`;
        queryCell.style.fontFamily = 'Monospace';
        queryRow.style.display = '';
        queryRow.setAttribute("data-hexqueryid", newRow.dataset.hexqueryid);

        /** Query text preview */
        if (queryCell.firstChild && queryCell.firstChild.tagName.toLowerCase() !== 'p') {
            let preprocessedText = Previewer.preprocessQueryString(queryString);
            queryCell.insertAdjacentHTML('afterbegin', `<p><i>${preprocessedText}</i></p>`);
        }
    }

    static findQuery(datasetName, dataID, hexQueryId) {
        for (let i = 0; i < data.datasets[datasetName].length; i++) {
            if (data.datasets[datasetName][i][dataID] === hexQueryId) {
                return i
            }
        }
        return -1
    }

    static init() {
        const PARENT_ROWS = Previewer.getParentRows();

        PARENT_ROWS.forEach(parentRow => {

            /** Determine row and cell with query text */
            let queryCell = document.createElement("td");
            queryCell.setAttribute("colspan", "100");
            let queryRow = document.createElement("tr");
            queryRow.classList.add("queryRow");
            queryRow.style.display = "none";

            let preview = JSON.parse(parentRow.closest('table').dataset["preview"])[0]
            let datasetName = preview.dataset;
            let dataID = preview.id;

            /** Copy query text into clipboard button */
            let copyQueryTextButton = Copier.drawButton();
            copyQueryTextButton.setAttribute("class", "copyQueryTextButton");
            queryCell.appendChild(copyQueryTextButton);
            queryRow.appendChild(queryCell);

            if (!parentRow.classList.contains("int1")) {
                parentRow.insertAdjacentElement("afterend", queryRow);
            }

            parentRow.addEventListener("click", event => {
                if (parentRow.classList.contains('int1')) {
                    queryRow = parentRow.nextSibling.nextSibling;
                    queryCell = queryRow.firstChild;
                }
                /** Trigger event only if user clicked not on rect and link*/
                if (event.target.tagName.toLowerCase() !== 'a' && event.target.tagName.toLowerCase() !== 'rect') {
                    if (queryRow.style.display === 'none') {
                        let queryIndex = Previewer.findQuery(datasetName, dataID, parentRow.dataset[dataID]);
                        if (queryIndex >= 0) {
                            let queryText = data.datasets[datasetName][queryIndex].query_texts[0];
                            Previewer.queryTextPreviewer(queryCell, queryRow, parentRow, queryText);
                        }
                    } else {
                        queryRow.style.display = 'none';
                    }
                }
            })
        })
    }
}