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

        /** Query text preview */
        if (queryCell.firstChild && queryCell.firstChild.tagName.toLowerCase() !== 'p') {
            let preprocessedText = Previewer.preprocessQueryString(queryString);
            queryCell.insertAdjacentHTML('afterbegin', `<p><i>${preprocessedText}</i></p>`);
        }
    }

    static findQuery(queryRaw) {
        // datasetName, dataID, parentRow.dataset[dataID]
        let datasetName = queryRaw.dataset["dataset_name"];
        let dataID = queryRaw.dataset["dataset_col_id"];
        let querySet = data.datasets[datasetName];
        let queryId = queryRaw.dataset["dataset_id"]
        
        for (let i = 0; i < querySet.length; i++) {
            if (querySet[i][dataID] === queryId) {
                return i
            }
        }
        return -1
    }

    static drawCopyButton() {
        let button = document.createElement('a');
        button.setAttribute('class', 'copyButton');
        button.setAttribute('title', 'Copy to clipboard');

        let svg = `
            <svg height="14px" width="12px" style="margin-left: 10px;">
                <rect x="2" y="2" height="12px" width="10px" rx="4" stroke="grey" fill="transparent"></rect>
                <rect x="0" y="0" height="12px" width="10px" rx="4" stroke="grey" fill="transparent"></rect>
            </svg>
        `

        button.insertAdjacentHTML('afterbegin', svg);

        return button;
    }

    static init() {
        const PARENT_ROWS = Previewer.getParentRows();

        PARENT_ROWS.forEach(parentRow => {

            /** Determine row and cell with query text */
            let queryCell = document.createElement("td");
            queryCell.setAttribute("colspan", "100");
            let queryRow = document.createElement("tr");
            queryRow.classList.add("queryRow");

            let preview = JSON.parse(parentRow.closest('table').dataset["preview"])[0]
            queryRow.setAttribute("data-dataset_name", preview.dataset);
            queryRow.setAttribute("data-dataset_col_id", preview.id);
            queryRow.setAttribute("data-dataset_id", parentRow.dataset[preview.id]);
            queryRow.style.display = "none";
            queryRow.appendChild(queryCell);

            if (!parentRow.classList.contains("int1")) {
                parentRow.insertAdjacentElement("afterend", queryRow);
            }

            /** Copy query text into clipboard button */
            let copyQueryTextButton = Previewer.drawCopyButton();
            copyQueryTextButton.setAttribute("class", "copyQueryTextButton");
            queryCell.appendChild(copyQueryTextButton);

            parentRow.addEventListener("click", event => {
                if (parentRow.classList.contains('int1')) {
                    queryRow = parentRow.nextSibling.nextSibling;
                    queryCell = queryRow.firstChild;
                }

                /** Trigger event only if user clicked not on rect and link*/
                if (event.target.tagName.toLowerCase() !== 'a' && event.target.tagName.toLowerCase() !== 'rect') {
                    if (queryRow.style.display === 'none') {
                        let queryIndex = Previewer.findQuery(queryRow);
                        if (queryIndex >= 0) {
                            let queryText = data.datasets[preview.dataset][queryIndex].query_texts[0];
                            Previewer.queryTextPreviewer(queryCell, queryRow, parentRow, queryText);
                            copyQueryTextButton.addEventListener("click", event => {
                                navigator.clipboard.writeText(queryText).then(r => console.log(queryText));
                            });
                        }
                    } else {
                        queryRow.style.display = 'none';
                    }
                }
            })
        })
    }
}