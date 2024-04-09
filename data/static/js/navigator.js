class ReportNavigator {
    static buildReportNavigator(NAVIGATOR) {
        window.onscroll = function () {
            document.querySelectorAll('h3').forEach(title => {
                let position = title.getBoundingClientRect().top;
                if (position >= 0 && position < 100) {
                    let li = `[href*=${title.id}]`;
                    let elem = NAVIGATOR.querySelector(li).closest('li');
                    if (!elem.classList.contains('current')) {
                        NAVIGATOR.querySelectorAll('li').forEach(item => {
                            item.classList.remove('current');
                        });
                        elem.classList.add('current');
                    }
                }
            })
        }
    }

    static buildPageContent(data, parentNode) {
        data.sections.forEach(section => {
            let hasTableCap = ('tbl_cap' in section);
            let hasNestedSections = ('sections' in section);
            let ul = document.createElement('ul');
            let li = document.createElement('li');

            /** Creating <li> and <a> tags inside <ul> */
            if (hasTableCap) {

                let a = document.createElement('a');

                a.innerHTML = section.tbl_cap;
                a.href = `#${section.href}`;
                a.classList.add('anchor');

                li.setAttribute('id', `navigator_${section.href}`);
                li.appendChild(a);
                parentNode.appendChild(li);
            } else {
                ul = li;
            }
            /** Recursive call for building nested content */
            if (hasNestedSections) {
                parentNode.appendChild(this.buildPageContent(section, ul));
            }
        })

        return parentNode;
    }

    static init() {
        const TOPNAV = document.getElementById('topnav');
        const MAIN_REPORT = document.getElementById('container');
        MAIN_REPORT.setAttribute('class', 'with-menu')

        let menu = document.createElement('a');
        menu.setAttribute('class', 'active');
        for (let i = 0; i < 3; i++) {
            let burger = document.createElement('div');
            burger.style.width = "10px";
            burger.style.height = "2px";
            burger.style.background = "white";
            burger.style.marginBottom = "2px";
            menu.appendChild(burger);
        }

        /** Add input field for searching substrings over report */
        let input = document.createElement('input');
        input.setAttribute('id', 'searchField');
        input.setAttribute('type', 'search');
        input.setAttribute('placeholder', 'Filter...');

        let select = document.createElement('select');
        select.setAttribute('id', 'searchParam');

        Object.entries(Utilities.searchParams).forEach(([key, value]) => {
            let choiceElement = document.createElement('option');
            choiceElement.setAttribute('value', key);
            choiceElement.innerHTML = value;
            select.appendChild(choiceElement);
        })

        /** Add event listener for searching over report */
        /** TODO: Добавить в схему JSON класс с явным указанием полей по которым работает поиск */
        let rowsForSearch = document.querySelectorAll('tr[data-all]');
        input.addEventListener('input', ev => {
            let keyword = ev.target.value.trim();
            let searchParam = document.getElementById('searchParam').value;

            /** Calling search only for rows that have data-search attr */
            Utilities.search(rowsForSearch, searchParam, keyword);
        })
        /** Add event for changing searchParam */
        select.addEventListener('change', ev => {
            let searchParam = ev.target.value;
            let keyword = Utilities.getInputField().value;
            if (keyword) {
                /** Calling search only for rows that have data-search attr */
                Utilities.search(rowsForSearch, searchParam, keyword);
            }
        })
        /** Add event listener for cancelling search results */
        document.addEventListener('keydown', evt => {
            if (evt.key === 'Escape') {
                Utilities.cancelSearchResults(rowsForSearch);
            }
        });

        TOPNAV.appendChild(menu);
        TOPNAV.appendChild(input);
        TOPNAV.appendChild(select);

        /** Create navigator, append it to the body and fill it with content */
        const NAVIGATOR = document.createElement('div');
        NAVIGATOR.setAttribute('id', 'navigator');

        let ul = document.createElement('ul');
        ul.setAttribute('class', 'active');
        NAVIGATOR.appendChild(ul);

        /** Add event listener to hide and show navigator */
        menu.addEventListener('click', event => {
            if (menu.classList.contains('active')) {
                menu.setAttribute('class', '')
                MAIN_REPORT.style.left = "0";
                NAVIGATOR.style.width = "0";
            } else {
                menu.setAttribute('class', 'active')
                MAIN_REPORT.style.left = "25%";
                NAVIGATOR.style.width = "25%";
            }
        })
        document.querySelector('body').appendChild(NAVIGATOR);
        ReportNavigator.buildPageContent(data, ul);
        /** Add some useful events */
        ReportNavigator.buildReportNavigator(NAVIGATOR);
    }
}
