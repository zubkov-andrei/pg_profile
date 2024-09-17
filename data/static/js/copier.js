class Copier {
    static drawButton() {
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

    static copyQueryId(ev) {
        let text = ev.target.closest('tr').dataset.queryid;
        navigator.clipboard.writeText(text).then(r => console.log('Copy: ', text));
    }

    static init() {
        document.querySelectorAll('.copyQueryId').forEach(button => {
            button.addEventListener('click', function () {
                let text = button.closest('tr').dataset.queryid;
                navigator.clipboard.writeText(text).then(r => console.log('Copy query ID'));
            })
        })

        document.querySelectorAll('.copyQueryTextButton').forEach(button => {
            button.addEventListener('click', function () {
                let hexQueryId = button.closest('tr').dataset.hexqueryid;
                data.datasets.queries.forEach(query => {
                    if (query.hexqueryid === hexQueryId) {
                        let text = query.query_texts[0];
                        navigator.clipboard.writeText(text).then(r => console.log('Copy query text'));
                    }
                })
            })
        })
        document.querySelectorAll('.copyPlanTextButton').forEach(button => {
            button.addEventListener('click', function () {
                let hexQueryId = button.closest('tr').dataset.hexqueryid;
                let hexPlanId = button.closest('tr').dataset.hexplanid;
                data.datasets.queries.forEach(query => {
                    if (query.hexqueryid === hexQueryId) {
                        query.plans.forEach(plan => {
                            if (plan.hexplanid === hexPlanId) {
                                let text = plan.plan_text;
                                navigator.clipboard.writeText(text).then(r => console.log('Copy plan text'));
                            }
                        })
                    }
                })
            })
        })
    }
}