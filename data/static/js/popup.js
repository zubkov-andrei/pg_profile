class Popup {
    static hidden = '-20%';
    static appear = '2%';
    static transitionDelay = 250;
    static klass = 'popup';
    static id = 'popup';

    static STYLE = {
        MESSAGE: {
            'bgColor': '#D9FFCC',
            'duration': 3000,
            'fontColor': 'black'
        },
        BANNER: {
            'bgColor': '#CCF1FF',
            'fontColor': 'black'
        },
        ERROR: {
            'color': 'red',
            'duration': 3000
        },
    }

    static #createPopupTag() {
        const POPUP = document.createElement('div');
        POPUP.setAttribute('id', Popup.id);
        POPUP.setAttribute('class', Popup.klass);
        document.getElementById('container').appendChild(POPUP);

        return POPUP;
    }
    static getInstance() {
        return document.getElementById(Popup.id) ? document.getElementById(Popup.id) : false;
    }
    static popupIsHidden() {
        const POPUP = Popup.getInstance();

        return getComputedStyle(POPUP).getPropertyValue('--main-bottom').trim() === Popup.hidden.trim();
    }
    static popupAppearing(noticeProperties, notice) {
        const POPUP = Popup.getInstance();

        return new Promise(result => {
            POPUP.style.setProperty('--main-bottom', Popup.appear);
            POPUP.style.setProperty('--main-bg-color', noticeProperties.bgColor);
            POPUP.style.setProperty('--main-font-color', noticeProperties.fontColor);
            POPUP.innerHTML = ''; /** Cleare all inside */

            /** Add close link to popup */
            let close_link = document.createElement('a');
            close_link.innerHTML = 'x';
            close_link.onclick = function () {
                POPUP.style.display = 'none';
            }
            close_link.style.cursor = 'pointer';
            close_link.style.color = 'gray';

            POPUP.appendChild(close_link);
            POPUP.appendChild(notice);

            if (noticeProperties.duration) {
                setTimeout(result, noticeProperties.duration);
            }
        })
    }
    static popupDisappearing(delay) {
        const POPUP = Popup.getInstance();

        return new Promise(result => {
            POPUP.style.setProperty('--main-bottom', Popup.hidden);
            setTimeout(result, delay);
        })
    }
    static async sendNotice(noticeProperties, message) {
        const POPUP = Popup.getInstance();

        if (!Popup.popupIsHidden(POPUP)) {
            await Popup.popupDisappearing(Popup.transitionDelay);
        }
        await Popup.popupAppearing(noticeProperties, message);
        if (noticeProperties.duration) {
            await Popup.popupDisappearing(noticeProperties.duration + Popup.transitionDelay);
        }

    }
    static init() {
        /** Setting div attributes */
        Popup.#createPopupTag();
    }
}