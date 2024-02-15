class Copier {
    static drawButton() {
        let button = document.createElement('a');
        button.setAttribute('class', 'copyButton');
        button.setAttribute('title', 'Copy to clipboard');
        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.style.marginLeft = '10px';
        svg.setAttribute('height', '14px');
        svg.setAttribute('width', '12px');

        let rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.setAttribute('x', '2');
        rect.setAttribute('y', '2');
        rect.setAttribute('height', '12px');
        rect.setAttribute('width', '10px');
        rect.setAttribute('rx', '4');
        rect.setAttribute('stroke', 'grey');
        rect.setAttribute('fill', 'transparent');

        let replica = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        replica.setAttribute('x', '0');
        replica.setAttribute('y', '0');
        replica.setAttribute('height', '12px');
        replica.setAttribute('width', '10px');
        replica.setAttribute('rx', '4');
        replica.setAttribute('stroke', 'grey');
        replica.setAttribute('fill', 'transparent');

        svg.appendChild(rect);
        svg.appendChild(replica);
        button.appendChild(svg);

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