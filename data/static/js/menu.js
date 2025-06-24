/**
 * Class
 */
class Menu {
    /** Creates content with arrows */
    static buildNavigator(section, deep) {
        let hasNestedSections = ('sections' in section);
        let hasTocCap = ('toc_cap' in section);
        let navigator = document.getElementById('sections');
        /** Creating the main container for the current section */
        let div = document.createElement('div');
        div.classList.add(`level${deep}`);

        /** If the section is missing a subsection */
        if (hasTocCap) {
            /** Creating a section header */
            let title = document.createElement('div');
            /** Creating a link for the title */
            let a = document.createElement('a');
            a.innerHTML = section.toc_cap;
            a.href = `#${section.sect_id}`;
            a.classList.add('anchor');
            a.setAttribute('id', `menu_${section.sect_id}`);
            /** Building tag div */
            title.appendChild(a);
            div.appendChild(title);

            /** If there are no nested sections */
            if (!hasNestedSections) {
                navigator.appendChild(div);
            } else {
                /** Creating a container for nested sections */
                let nestedDiv = document.createElement('div');
                nestedDiv.classList.add('nested-sections', 'hidden');

                let hasNestedContent = false;

                /** Processing of each nested section */
                section.sections.forEach(nestedSection => {
                    let nestedSectionDiv = Menu.buildNavigator(nestedSection, deep + 1);
                    if (nestedSectionDiv) {
                        /** Adding a nested section */
                        nestedDiv.appendChild(nestedSectionDiv);
                        hasNestedContent = true;
                    }
                });

                const arrowHTML = `
                    <svg viewBox="0 0 16 16" width="16" height="16" class="arrow">
                        <path fill-rule="evenodd" clip-rule="evenodd" d="M4.9417 5.5L8 8.54753L11.0583 5.5L12.5 6.93662L8.72085 10.7025C8.32273 11.0992 7.67726 11.0992 7.27915 10.7025L3.5 6.93662L4.9417 5.5Z" fill="#A6B5C7"/>
                    </svg>
                `;

                if (hasNestedContent) {
                    title.insertAdjacentHTML('beforeend', arrowHTML);
                    let arrowSvg = title.querySelector('.arrow');

                    /** Adding a container of nested sections after the parent */
                    div.appendChild(nestedDiv);
                    /** Adding the main container to the navigator */
                    navigator.appendChild(div);

                    /** A header click handler for switching the visibility of nested sections */
                    title.addEventListener('click', () => {
                        nestedDiv.classList.toggle('hidden');
                        arrowSvg.classList.toggle('up');
                    });
                } else {
                    /** If there are no nested sections, just add the current section. */
                    navigator.appendChild(div);
                }
            }
            return div;
        }
    }


    /** implementation of dynamic page content construction */
    static buildPageContent(data, parentNode, deep = 1) {

        /** Drawing Navigation */
        let initialDeep = 1;
        data.sections.forEach(section => {
            Menu.buildNavigator(section, initialDeep);
        })

        /** Adding the "chapter" class to links */
        let allLinks = parentNode.querySelectorAll('div[class^="level"] a');
        allLinks.forEach(link => {
            let parentDiv = link.closest('div');
            if (parentDiv) {
                parentDiv.classList.add('chapter');
            }
        });
        return parentNode;
    }

    /** Create a logo */
    static drawLogo() {
        let reportContent = document.getElementById('pageContent');
        let logo = logoFile;
        let logoMini = logoMiniFile;

        reportContent.insertAdjacentHTML('afterbegin', logo);
        reportContent.insertAdjacentHTML('afterbegin', logoMini);
    }

    static buildMenu() {
        let body = document.querySelector('body');

        /** Page Content */
        let pageContent = `
            <div id="pageContent">
                <div id="sections" class="active"></div>
            </div>
        `
        body.insertAdjacentHTML('afterbegin', pageContent);
        let sections = document.getElementById("sections");
        Menu.buildPageContent(data, sections, 1);

        /** Draw Logo */
        this.drawLogo();

        /** Draw Search */
        this.createSearchAndDropdown();
    }

    /** Creating an input and selection field */
    static createSearchAndDropdown() {
        let body = document.querySelector('body');
        let container = document.createElement('div');
        container.id = 'searchDropdownContainer';
        /** A block is being created for the search field and its buttons*/
        let searchWrapper = document.createElement('div');
        searchWrapper.id = 'searchWrap';
        searchWrapper.style.display = 'flex';
        /** Creating a search field */
        let input = document.createElement('input');
        input.type = 'text';
        input.id = 'searchField';
        input.placeholder = 'Search';
        /** Creating a search button */
        let searchButton = document.createElement('button');
        searchButton.type = 'button';
        searchButton.id = 'searchButton';

        /** Inserting SVG into the search field */
        searchButton.innerHTML = `
            <svg width="15" height="15" viewBox="0 0 15 15" fill="none" xmlns="http://www.w3.org/2000/svg">
                <g clip-path="url(#clip0_371_5886)">
                    <path 
                        fill-rule="evenodd" 
                        clip-rule="evenodd" 
                        d="M7 14C10.866 14 14 10.866 14 7C14 3.13401 10.866 0 7 0C3.13401 0 0 3.13401 0 7C0 10.866 
                            3.13401 14 7 14ZM7 12C9.76142 12 12 9.76142 12 7C12 4.23858 9.76142 2 7 2C4.23858 2 2 
                            4.23858 2 7C2 9.76142 4.23858 12 7 12ZM14 12.5858L15.7071 14.2929L14.2929 15.7071L12.5858 
                            14L14 12.5858ZM6.24974 6.33882C6.43444 6.12956 6.70147 6 7 6V4C6.10384 4 5.2985 4.3942 
                            4.75026 5.01535L6.24974 6.33882Z" 
                        fill="#14B0FF"
                    />
                </g>
                <defs>
                    <clipPath id="clip0_371_5886">
                        <rect width="16" height="16" fill="white"/>
                    </clipPath>
                </defs>
            </svg>
        `;
        searchWrapper.appendChild(input);
        searchWrapper.appendChild(searchButton);

        /** Creating a field with a drop-down list */
        let select = document.createElement('select');
        select.id = 'dropdownSelect';

        /** Adding options */
        let defaultOption = document.createElement('option');
        defaultOption.value = 'all';
        defaultOption.textContent = 'Everywhere';
        select.appendChild(defaultOption);

        /** Adding options */
        const options = [
            { value: 'dbname', text: 'Database' },
            { value: 'username', text: 'User' },
            { value: 'relname', text: 'Table' },
            { value: 'indexrelname', text: 'Index' },
            { value: 'querytext', text: 'Query' }
        ];

        options.forEach(option => {
            let optElement = document.createElement('option');
            optElement.value = option.value;
            optElement.textContent = option.text;
            select.appendChild(optElement);
        });

        /** Insert the svg arrow into the drop-down list box */
        const arrowBlue = `
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path 
                    fill-rule="evenodd" 
                    clip-rule="evenodd" 
                    d="M4.9417 5.5L8 8.54753L11.0583 5.5L12.5 6.93662L8.72085 10.7025C8.32273 11.0992 7.67726 11.0992 
                        7.27915 10.7025L3.5 6.93662L4.9417 5.5Z" 
                    fill="#14B0FF"
                />
            </svg>
        `;
        let DropDownArrow = encodeURIComponent(arrowBlue);
        select.style.backgroundImage = `url("data:image/svg+xml;utf8,${DropDownArrow}")`;

        /** Inserting the fields into the container */
        container.appendChild(searchWrapper);
        container.appendChild(select);

        /** We insert it after the logos */
        let logoContainer = document.getElementById('logo');
        if (logoContainer && logoContainer.parentNode) {
            logoContainer.parentNode.insertBefore(container, logoContainer.nextSibling);
        } else {
            document.body.insertBefore(container, document.body.firstChild);
        }

        /** Add Event on change input Field */
        let rowsForSearch = document.querySelectorAll('tr:not(.header, .previewRow)');
        input.addEventListener("keyup", event => {
            if (event.key === "Escape") {
                event.target.value = "";
                Utilities.search(rowsForSearch, defaultOption.value, event.target.value);
            } else {
                Utilities.search(rowsForSearch, select.value, event.target.value);
            }
        })
    }

    /** Track the scroll position and highlight the relevant sections in the menu */
    static navigateBorder() {
        function updateHighlight() {
            let width = document.getElementById('pageContent').offsetWidth + 30;
            let height = 0;
            let element = document.elementFromPoint(width, height);

            if (element) {
                /** Detecting section id */
                let sectId;
                let targetHeader = element.closest('h3, p, table');
                if (targetHeader) {
                    if (targetHeader.tagName === 'H3') {
                        sectId = targetHeader.getAttribute('id');
                    } else if (targetHeader.tagName === 'P') {
                        sectId = targetHeader.getAttribute('id');
                    } else if (targetHeader.tagName === 'TABLE') {
                        sectId = targetHeader.parentNode.firstChild.getAttribute('id');
                    }
                }

                if (sectId) {
                    /** We are looking for a link corresponding to the current ID. */
                    let targetLink = document.querySelector(`.chapter a[href="#${sectId}"]`);
                    let currentChapter = targetLink?.closest('.chapter');

                    if (targetLink && currentChapter) {
                        /** Removing the 'activeSection' from everyone */
                        document.querySelectorAll('.chapter.activeSection').forEach(ch => ch.classList.remove('activeSection'));
                        /** A function for moving up the hierarchy and highlighting parent sections with hidden subsections */
                        function highlightParentSections(chapter) {
                            let parentContainer = chapter.closest('[class^="level"]');
                            while (parentContainer) {
                                let parentChapter = parentContainer.querySelector('.chapter:first-child');
                                let nestedSections = parentContainer.querySelector('.nested-sections');

                                if (parentChapter && nestedSections && nestedSections.classList.contains('hidden')) {
                                    parentChapter.classList.add('activeSection');
                                }

                                parentContainer = parentContainer.parentElement.closest('[class^="level"]');
                            }
                        }
                        highlightParentSections(currentChapter);
                        currentChapter.classList.add('activeSection');
                    }
                }
            }
        }

        /** Scroll Handler */
        document.addEventListener('scroll', updateHighlight);

        /** Enabling the class change monitor */
        let observer = new MutationObserver((mutations) => {
            mutations.forEach(mutation => {
                if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
                    updateHighlight();
                }
            });
        });

        /** Launching surveillance */
        document.querySelectorAll('.nested-sections').forEach(section => {
            observer.observe(section, { attributes: true });
        });

        /** The first call to updateHighlight is to highlight when loading. */
        updateHighlight();
    }

    /** Opening and closing menus on a page, and switching logos */
    static toggleMenu() {
        let menu = document.getElementById('pageContent');
        let logo = document.getElementById('logo');
        let logoMini = document.getElementById('logoMini');
        let mainContainer = document.getElementById('container');
        let searchDropdownContainer = document.getElementById('searchDropdownContainer');

        /** Declaring sizes and margins */
        let paddingHorizontal = 40;
        let minMenuWidthPx = 100;
        let collapsedWidthPx = 35;

        /** Calculating the width of the menu when expanding. */
        function getExpandedWidthPx() {
            let maxWidthVw = 19;
            let widthFromVw = window.innerWidth * maxWidthVw / 100;
            /** protection against too narrow a menu when the browser window is greatly reduced */
            return Math.max(widthFromVw, minMenuWidthPx + paddingHorizontal);
        }

        /** Unified menu toggle function */
        function toggleMenuState(shouldExpand) {
            if (shouldExpand) {
                const totalWidth = getExpandedWidthPx();
                menu.style.width = (totalWidth - paddingHorizontal) + 'px';
                mainContainer.style.left = totalWidth + 'px';
                menu.classList.remove('hidden');
                logo.classList.remove('hidden');
                logoMini.classList.add('hidden');
                searchDropdownContainer.classList.remove('hidden');
            } else {
                menu.style.width = collapsedWidthPx + 'px';
                mainContainer.style.left = (collapsedWidthPx + paddingHorizontal) + 'px';
                menu.classList.add('hidden');
                logo.classList.add('hidden');
                logoMini.classList.remove('hidden');
                searchDropdownContainer.classList.add('hidden');
            }
        }

        /** Set initial state */
        toggleMenuState(!menu.classList.contains('hidden'));

        /** Logo Click Handlers */
        [logo, logoMini].forEach(elem =>
            elem.addEventListener('click', function() {
                toggleMenuState(menu.classList.contains('hidden'));
            })
        );

        /** Window resize handler */
        window.addEventListener('resize', function() {
            if (!menu.classList.contains('hidden')) {
                toggleMenuState(true);
            }
        });
    }

    static init() {
        this.buildMenu();
        this.navigateBorder();
        this.toggleMenu();
    }
}