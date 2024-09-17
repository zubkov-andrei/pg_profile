/**
 * Recursive function for building report. Function accepts report data in JSON and parent node (html tag) in which
 * report should be inserted
 * @param data jsonb object with report data
 * @param parentNode node in html-page
 * @returns {*}
 */
function buildReport(data, parentNode) {
    data.sections.forEach(section => {
        let sectionHasNestedSections = ('sections' in section);
        let newSection = new BaseSection(section).init();

        /** Recursive call for building nested sections if exists */
        if (sectionHasNestedSections) {
            buildReport(section, newSection);
        }

        parentNode.appendChild(newSection);
    })

    return parentNode;
}

function main() {

    /** Build report sections */
    const CONTAINER = document.getElementById('container');
    buildReport(data, CONTAINER);

    /** Add highlight feature */
    Highlighter.init();

    /** Add query text and plan feature */
    Previewer.init();

    /** Add copy feature */
    Copier.init();

    /** Add menu feature */
    Menu.init();
}

main();