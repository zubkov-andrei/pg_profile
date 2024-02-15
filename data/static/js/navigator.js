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

        let topContent = document.createElement('a');
        topContent.setAttribute('class', 'active');
        topContent.innerHTML = 'Contents';

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

        TOPNAV.appendChild(topContent);
        TOPNAV.appendChild(input);
        TOPNAV.appendChild(select);

        /** Create navigator, append it to the body and fill it with content */
        const NAVIGATOR = document.createElement('div');
        NAVIGATOR.setAttribute('id', 'navigator');

        let ul = document.createElement('ul');
        ul.setAttribute('class', 'active');
        NAVIGATOR.appendChild(ul);

        /** Add event listener to hide and show navigator */
        topContent.addEventListener('click', event => {
            if (ul.classList.contains('hidden')) {
                ul.setAttribute('class', 'active');
                topContent.setAttribute('class', 'active');
            } else {
                ul.setAttribute('class', 'hidden');
                topContent.removeAttribute('class');
            }
        })
        document.querySelector('body').appendChild(NAVIGATOR);
        ReportNavigator.buildPageContent(data, ul);
        /** Add some useful events */
        ReportNavigator.buildReportNavigator(NAVIGATOR);
    }
}
