/**
 * The class is designed to instantly preview the query text referenced by the selected row
 */
class Previewer {
    static getParentRows() {
        return document.querySelectorAll("table.preview tr:not(.header)");
    }

    static queryTextPreviewer(queryCell, queryRow, newRow, queryString) {
        queryCell.style.width = `${Math.floor(newRow.offsetWidth * 0.95)}px`;
        queryCell.style.fontFamily = 'Monospace';
        queryRow.style.display = '';

        /** Query text preview */
        if (!queryCell.hasChildNodes()) {
            let preprocessedText = Utilities.preprocessQueryString(queryString);
            queryCell.insertAdjacentHTML('afterbegin', `<p><i>${preprocessedText}</i></p>`);
        }
    }

    static storageParamsPreviewer(previewCell, previewRow, newRow, previewData) {
        previewCell.style.width = `${Math.floor(newRow.offsetWidth * 0.95)}px`;
        previewCell.style.fontFamily = 'Monospace';
        previewRow.style.display = '';

        /** Query text preview */
        if (!previewCell.hasChildNodes()) {
            let topn = data.properties.topn || 20;
            previewData.slice(-topn).reverse().forEach(item => {
                let preprocessedText = Utilities.preprocessQueryString(`${item['first_seen']}: ${item['reloptions']}`);
                previewCell.insertAdjacentHTML('afterbegin', `<p><i>${preprocessedText}</i></p>`);
            })
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
            <svg width="14" height="14" viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path fill-rule="evenodd" clip-rule="evenodd" d="M8 0C5.79086 0 4 1.79086 4 4V10C4 12.2091 5.79086 14 8 14H14C16.2091 14 18 12.2091 18 10V4C18 1.79086 16.2091 0 14 0H8ZM6 4C6 2.89543 6.89543 2 8 2H14C15.1046 2 16 2.89543 16 4V10C16 11.1046 15.1046 12 14 12H8C6.89543 12 6 11.1046 6 10V4ZM0 7C0 5.69378 0.834808 4.58254 2 4.17071V14C2 15.1046 2.89543 16 4 16H13.8293C13.4175 17.1652 12.3062 18 11 18H3C1.34315 18 0 16.6569 0 15V7Z" fill="#14B0FF"/>
            </svg>
        `

        button.insertAdjacentHTML('afterbegin', svg);

        return button;
    }

    static init() {
        const PARENT_ROWS = Previewer.getParentRows();

        PARENT_ROWS.forEach(parentRow => {

            /** Determine row and cell with query text */
            let previewCell = document.createElement("td");
            previewCell.setAttribute("colspan", "100");
            let previewRow = document.createElement("tr");
            previewRow.classList.add("previewRow");

            let preview = JSON.parse(parentRow.closest('table').dataset["preview"])[0]
            let sourceDatasetName = preview.dataset;
            let sourceDatasetKey = preview.id;

            previewRow.setAttribute("data-dataset_name", sourceDatasetName);
            previewRow.setAttribute("data-dataset_col_id", sourceDatasetKey);
            previewRow.setAttribute("data-dataset_id", parentRow.dataset[sourceDatasetKey]);
            previewRow.style.display = "none";
            previewRow.appendChild(previewCell);

            if (!parentRow.classList.contains("int1")) {
                parentRow.insertAdjacentElement("afterend", previewRow);
            }

            parentRow.addEventListener("click", event => {
                if (parentRow.classList.contains('int1')) {
                    previewRow = parentRow.nextSibling.nextSibling;
                    previewCell = previewRow.firstChild;
                }

                /** Trigger event only if user clicked not on rect and link*/
                if (
                    event.target.tagName.toLowerCase() !== 'a' &&
                    event.target.tagName.toLowerCase() !== 'rect' &&
                    event.target.tagName.toLowerCase() !== 'svg' &&
                    event.target.tagName.toLowerCase() !== 'path'
                ) {
                    if (previewRow.style.display === 'none') {

                        /** Preview SQL query text */
                        if (sourceDatasetName === "queries" || sourceDatasetName === "act_queries") {
                            let queryIndex = Previewer.findQuery(previewRow);
                            if (queryIndex >= 0) {

                                let queryText = data.datasets[sourceDatasetName][queryIndex].query_texts[0];
                                Previewer.queryTextPreviewer(previewCell, previewRow, parentRow, queryText);

                                /** Copy query text into clipboard button */
                                if (!previewCell.querySelector('.copyQueryTextButton')) {
                                    let copyQueryTextButton = Previewer.drawCopyButton();
                                    copyQueryTextButton.setAttribute("class", "copyQueryTextButton");
                                    previewCell.appendChild(copyQueryTextButton);

                                    copyQueryTextButton.addEventListener("click", event => {
                                        navigator.clipboard.writeText(queryText).then(r => console.log(queryText));
                                    });
                                }
                            }
                        }

                        /** Preview Table storage parameters */
                        if (sourceDatasetName === "table_storage_parameters" || sourceDatasetName === "index_storage_parameters") {
                            let sourceDataset = data.datasets[sourceDatasetName]; 
                            let targetDatasetValue = parentRow.dataset[sourceDatasetKey];

                            let previewData = Utilities.find(sourceDataset, sourceDatasetKey, targetDatasetValue);

                            if (previewData.length) {
                                let previewDataJSON = JSON.stringify(previewData);
                                Previewer.storageParamsPreviewer(previewCell, previewRow, parentRow, previewData);
                            }
                        }
                    } else {
                        previewRow.style.display = 'none';
                    }
                }
            })
        })
    }
}