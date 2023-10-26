/**
 * Recursive function to build content of report. Function receives data as JSON object and html node in which content
 * information should be inserted
 *
 * @param data jsonb object with report data
 * @param parentNode node in html-page
 * @returns {*} - parent node with data
 */
function buildPageContent(data, parentNode) {
    data.sections.forEach(section => {
        let hasTableCap = ('tbl_cap' in section);
        let hasNestedSections = ('sections' in section);
        let ul = document.createElement('ul');
        let li = document.createElement('li')

        /** Creating <li> and <a> tags inside <ul> */
        if (hasTableCap) {

            let a = document.createElement('a');

            a.innerHTML = section.tbl_cap;
            a.href = `#${section.href}`;

            li.appendChild(a);
            parentNode.appendChild(li);
        } else {
            ul = li;
        }
        /** Recursive call for building nested content */
        if (hasNestedSections) {
            parentNode.appendChild(buildPageContent(section, ul));
        }
    })

    return parentNode;
}

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

    /** Build report content */
    const CONTENT = document.getElementById('content');
    buildPageContent(data, CONTENT);

    /** Build report sections */
    const CONTAINER = document.getElementById('container');
    buildReport(data, CONTAINER);

    /** Add popup feature */
    Popup.init();

    /** Add highlight feature */
    Highlighter.init();

    /** Add copy feature */
    Copier.init();

    /** Add sort feature */
    Sorter.init();

    /** Add navigator feature */
    ReportNavigator.init();
}

main();