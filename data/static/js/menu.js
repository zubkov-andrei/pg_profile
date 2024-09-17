/**
 * Class 
 */
class Menu {
    static buildPageContent(data, parentNode, visibility) {
        data.sections.forEach(section => {
            let hasTableCap = ('toc_cap' in section);
            let hasNestedSections = ('sections' in section);
            let ul = document.createElement('ul');
            let li = document.createElement('li');
            li.classList.add(visibility);

            /** Creating <li> and <a> tags inside <ul> */
            if (hasTableCap) {

                let a = document.createElement('a');

                a.innerHTML = section.toc_cap;
                a.href = `#${section.sect_id}`;
                a.classList.add('anchor');

                li.setAttribute('id', `menu_${section.sect_id}`);
                li.appendChild(a);

                parentNode.appendChild(li);
            }
            /** Recursive call for building nested content */
            if (hasNestedSections) {
                parentNode.appendChild(this.buildPageContent(section, ul, "hidden"));
            }
        })
        return parentNode;
    }

    static addToggleMenuEvent() {
        let menuButton = document.getElementById("menuButton");
        let reportContent = document.getElementById('pageContent');
        let container = document.getElementById('container');
        
        menuButton.addEventListener('click', function() {
            let isMenuActive = menuButton.classList.contains('active');

            if (isMenuActive) {
                menuButton.setAttribute('class', '');
                burger.setAttribute('class', 'horizontal');
                container.style.left = "0";
                reportContent.style.width = "0";
            } else {
                menuButton.setAttribute('class', 'active');
                burger.setAttribute('class', 'vertical');
                container.style.left = "25%";
                reportContent.style.width = "25%";
            }
        })
    }

    static addSearchFieldEvent() {
        let rowsForSearch = document.querySelectorAll('tr[data-all]');
        let input = document.getElementById("inputField");
        let searchParam = document.getElementById('searchParam');

        input.addEventListener('input', ev => {
            let keyword = ev.target.value.trim();
            let searchParam = document.getElementById('searchParam').value;

            /** Calling search only for rows that have data-search attr */
            Utilities.search(rowsForSearch, searchParam, keyword);
        });
        
        /** Add event for changing searchParam */
        searchParam.addEventListener('change', ev => {
            let searchParam = ev.target.value;
            let keyword = document.getElementById("inputField").value;

            if (keyword) {
                /** Calling search only for rows that have data-search attr */
                Utilities.search(rowsForSearch, searchParam, keyword);
            }
        })
    }

    static addToggleSectionsEvent() {
        document.querySelectorAll("#sections li").forEach(section => {
            section.addEventListener("click", ev => {
                if (ev.target.parentNode.nextSibling && ev.target.parentNode.nextSibling.tagName === "UL") {
                    ev.target.parentNode.nextSibling.childNodes.forEach(el => {
                        if (el.classList.contains("hidden")) {
                            el.classList.remove("hidden");
                            el.classList.add("visible");
                        }
                    })
                }
            })
        });
    }

    static buildHtml() {
        let body = document.querySelector('body');

        /** Main Menu Button */
        let menuButton = `
            <a id="menuButton">
                <div id="burger" class="horizontal">
                    <div></div>
                    <div></div>
                    <div></div>
                </div>
            </a>
        `
        /** Search Field */
        let searchField = `
            <div>
                <input id="inputField" type="search" placeholder="Filter...">
                <select id="searchParam">
                    <option value="all">Everywhere</option>
                    <option value="dbname">Database</option>
                    <option value="username">User</option>
                    <option value="relname">Table</option>
                    <option value="indexrelname">Index</option>
                    <option value="querytext">Query</option>
                <select>
            </div>
        `
        /** Top Navigation */
        let topNavigation = `
            <div id="topnav">
                ${menuButton}
                ${searchField}
            </div>
        `
        /** Page Content */
        let pageContent = `
            <div id="pageContent">
                <ul id="sections" class="active"></ul>
            </div>
        `
        body.insertAdjacentHTML('beforeend', pageContent);
        body.insertAdjacentHTML('afterbegin', topNavigation);
        let sections = document.getElementById("sections");
        Menu.buildPageContent(data, sections, "visible");
    }

    static init() {
        this.buildHtml();
        this.addToggleMenuEvent();
        this.addSearchFieldEvent();
        this.addToggleSectionsEvent();
    }
}