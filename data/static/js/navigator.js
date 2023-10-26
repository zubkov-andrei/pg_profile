class ReportNavigator {
    static buildReportNavigator(CONTENT, NAVIGATOR) {
        window.onscroll = function() {
            if (CONTENT.getBoundingClientRect().bottom <= 0) {
                NAVIGATOR.style.visibility = 'visible';
            } else {
                NAVIGATOR.style.visibility = 'hidden';
            }
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
        /** Create navigator, append it to the body and fill it with content */
        const CONTENT = document.getElementById('content');
        const NAVIGATOR = document.createElement('div');
        NAVIGATOR.setAttribute('id', 'navigator');

        let button = document.createElement('div');
        button.innerHTML = 'hide menu';
        button.setAttribute('class', 'active');
        button.setAttribute('title', 'Show / hide content');
        button.innerHTML = 'content';
        NAVIGATOR.appendChild(button);
        let ul = document.createElement('ul');
        ul.setAttribute('class', 'active');
        NAVIGATOR.appendChild(ul);
        button.addEventListener('click', event => {
            if (ul.classList.contains('hidden')) {
                ul.setAttribute('class', 'active');
                button.setAttribute('class', 'active');
            } else {
                ul.setAttribute('class', 'hidden');
                button.setAttribute('class', 'hidden');
            }
        })
        document.querySelector('body').appendChild(NAVIGATOR);
        ReportNavigator.buildPageContent(data, ul);
        /** Add some useful events */
        ReportNavigator.buildReportNavigator(CONTENT, NAVIGATOR);
    }
}
