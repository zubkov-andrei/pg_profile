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
                if (position >= 0 && position < 50) {
                    let li = `[href*=${title.id}]`;
                    let elem = NAVIGATOR.querySelector(li).closest('li');
                    if (!elem.classList.contains('active')) {
                        NAVIGATOR.querySelectorAll('li').forEach(item => {
                            item.removeAttribute('class');
                        });
                        elem.setAttribute('class', 'active');
                    }
                }
            })
        }
    }

    static init() {
        /** Create navigator, append it to the body and fill it with content */
        const CONTENT = document.getElementById('content');
        const NAVIGATOR = document.createElement('ul');
        NAVIGATOR.setAttribute('id', 'navigator');
        document.querySelector('body').appendChild(NAVIGATOR);
        buildPageContent(data, NAVIGATOR);

        /** Add some useful events */
        ReportNavigator.buildReportNavigator(CONTENT, NAVIGATOR);
    }
}