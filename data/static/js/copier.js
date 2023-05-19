class Copier {
    static getAllQueryCells() {
        return document.querySelectorAll('.queryId, .jitCellId');
    }
    static drawButton() {
        let button = document.createElement('a');
        button.setAttribute('class', 'copyButton');
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
    static sendNotice(text) {
        if (Popup.getInstance()) {
            const notice = document.createElement('p');
            notice.textContent = `queryid value copied to clipboard: ${text}`;
            Popup.sendNotice(Popup.STYLE.MESSAGE, notice);
        }
    }
    static copyQueryId(ev) {
        let text = ev.target.closest('tr').dataset.queryid;
        navigator.clipboard.writeText(text).then(
            function() {
                Copier.sendNotice(text);
            }, function(err) {
            console.error('Async: Could not copy text: ', err);
        });
    }
    static init() {
        const ALL_ID_CELLS = Copier.getAllQueryCells();

        ALL_ID_CELLS.forEach(elem => {
            elem = elem.querySelector('p');

            let button = Copier.drawButton();
            button.addEventListener('click', ev => Copier.copyQueryId(ev));
            elem.appendChild(button);
        })
    }
}