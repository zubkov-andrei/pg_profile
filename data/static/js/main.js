/**
 * Recursive function for building report. Function accepts report data in JSON and parent node (html tag) in which
 * report should be inserted
 * @param data jsonb object with report data
 * @param parentNode node in html-page
 * @returns {*}
 */
function buildReport(data, parentNode, deep) {
    data.sections.forEach(section => {
        let sectionHasNestedSections = ('sections' in section);
        let newSection = new BaseSection(section, deep).init();

        /** Recursive call for building nested sections if exists */
        if (sectionHasNestedSections) {
            deep++;
            buildReport(section, newSection, deep);
            deep--;
        }

        parentNode.appendChild(newSection);
    })

    return parentNode;
}

function addDescription(data, parentNode) {
    if (data.properties.description) {
        let description = `
            <h3>Description:</h3>
            <p>${data.properties.description}</p>
        `;
        parentNode.insertAdjacentHTML('beforeend', description);
    }
    if (data.properties.server_description) {
        let server_description = `
            <h3>Server description:</h3>
            <p>${data.properties.server_description}</p>
        `;
        parentNode.insertAdjacentHTML('beforeend', server_description);
    }
}

function main() {
    /** Build report sections */
    const CONTAINER = document.getElementById('container');
    addDescription(data, CONTAINER);
    buildReport(data, CONTAINER, 1);

    /** Add highlight feature */
    Highlighter.init();

    /** Add query text and plan feature */
    Previewer.init();

    /** Add menu feature */
    Menu.init();
}

main();