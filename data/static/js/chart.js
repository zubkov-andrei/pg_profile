class BaseChart {
    static drawIntoTable(cls, newRow, column, data) {
        /** Order data */
        let key = column.ordering[0] === '-' ? column.ordering.substring(1) : column.ordering;
        let value = column.id;
        let direction = column.ordering[0] === '-' ? -1 : 1;
        let newCell = newRow.insertCell(-1);
        newCell.setAttribute('class', column.class);
        if (Utilities.sum(data, value) > 0) {
            let orderedData = Utilities.sort(data, key, direction);

            /** Draw SVG */
            let svg = cls.drawSVG(orderedData, value, key);

            /** Append SVG to table */
            newCell.insertAdjacentHTML('beforeend', svg);
        }
    }
}

class PipeChart extends BaseChart {
    static drawSVG(orderedData, value, key) {
        let x = 0; // Start position of nested svg
        let nestedSvg = '';
        let totalWidth = 0;

        orderedData.forEach(elem => {
            let width = Math.floor(elem[value]);
            if (width > 0) {
                totalWidth += width;
            }
        });

        orderedData.forEach(elem => {
            let width = Math.floor(elem[value]);
            /** all lines with a value of 'width' > 0 */
            if (width > 0) {
                let textContent = `${elem.objname}: ${elem[key]}`;
                let lineColor;
                if (elem[value] > 50) {         // >50%
                    lineColor = "FA1900";
                } else if (elem[value] > 25) {  // >25% <=50%
                    lineColor = "FF9500";
                } else if (elem[value] > 10) {  // >10% <=25%
                    lineColor = "FFC83B";
                } else {                        // <=10%
                    lineColor = "00B896";
                }

                /** If there is only one parameter, it takes up the entire width without any padding */
                if (width !== 100) {
                    width = width - 0.3;
                }

                nestedSvg += `
                    <svg class="lineSvg" height="2em" width="${width}%" x="${x}%">
                        <title>${elem.objname}: ${elem[value]}%</title>
                        <line opacity="0.4" x1="0" y1="80%" x2="100%" y2="80%" stroke="#${lineColor}" stroke-width="4px"></line>
                        <text x="0.1em" y="45%" dominant-baseline="middle" fill="#${lineColor}" class="textSvg">
                            ${textContent}
                        </text>
                    </svg>
                `;

                x += width + 0.3;
            }
        });

        if (totalWidth < 100) {
            let remainingWidth = 100 - totalWidth;
            nestedSvg += `
                <svg class="lineSvg" height="2em" width="${remainingWidth}%" x="${x}%">
                    <title>Others (${remainingWidth}%)</title>
                    <line opacity="0.4" x1="0" y1="80%" x2="100%" y2="80%" stroke="#8898AE" stroke-width="4px"></line>
                </svg>
            `;
        }

        return `<svg height="2em" width="100%" class="pipe-chart-container">${nestedSvg}</svg>`;
    }

    static drawIntoTable(newRow, column, data) {
        BaseChart.drawIntoTable(PipeChart, newRow, column, data);
        setTimeout(() => {
            /** We are looking for all fields of the chart */
            let containerSvg = newRow.querySelectorAll('.pipe-chart-container');

            containerSvg.forEach(containerSvg => {
                if (containerSvg) {
                    PipeChart.checkTextOverflow(containerSvg); /** first check */
                    /** tracking window size changes */
                    let resizeObserver = new ResizeObserver((entries) => {
                        entries.forEach(entry => {
                            PipeChart.checkTextOverflow(entry.target);
                        });
                    });
                    resizeObserver.observe(containerSvg);
                    /** Saving the observer */
                    containerSvg._resizeObserver = resizeObserver;
                }
            });
        }, 100); /** delay for reliability */

        return true;
    }

    /** A function for comparing the width of text and line */
    static checkTextOverflow(containerSvg) {
        let lineSvgs = containerSvg.querySelectorAll('.lineSvg');
        if (lineSvgs) {
            lineSvgs.forEach(svg => {
                let textElement = svg.querySelector('.textSvg');
                let lineElement = svg.querySelector('line');

                if (textElement && lineElement && textElement.getBBox && lineElement.getBBox) {
                    /** getting the dimensions */
                    let textBBox = textElement.getBBox();
                    let lineBBox = lineElement.getBBox();

                    textElement.style.visibility = textBBox?.width > lineBBox?.width ? 'hidden' : 'visible';
                }
            });
        }
    }
}

class SessionChart extends BaseChart {
    colours = [
        "#FFD700", "#FFA500", "#AAAFEE", "#00FF00", "#00FFFF",
        "#FFFF00", "#FFA0ED", "#FF00FF", "#FF1493", "#F7DDD2",
        "#8A2BE2", "#ADFF2F", "#20B2AA", "#9932CC", "#FF6347",
        "#40E0D0", "#9370DB", "#FFFFFF"
    ]

    drawRect(dataObj, reportStartUT, proportion, num, strokeColour, fillColour, title, klass, target) {
        let startPoint = Math.ceil(proportion * (dataObj.start_ut - reportStartUT));

        return `
            <rect
                class="${klass}"
                data-target="${target}"
                x="${startPoint}px"
                y="${num * 20}px"
                height="10px"
                width="${Math.ceil(proportion * (dataObj.duration))}"
                stroke="${strokeColour}"
                fill="${fillColour}">
                <title>${title}</title>
            </rect>
        `;
    }

    drawPID(dataObj, reportStartUT, proportion, num) {
        let startPoint = Math.ceil(proportion * (dataObj.start_ut - reportStartUT));
        let charCount = dataObj.pid.toString().length ? dataObj.pid.toString().length : 4;

        let pid = `
            <text
                x="${startPoint - charCount * 8}px"
                y="${(num * 20) + 10}px"
                fill="black"
                font-style="italic"
                font-size="12px"
            >
            ${dataObj.pid}
            </text>
        `
        return pid
    }

    drawLegend(clr, com) {
        let div = document.createElement('div');

        let legendSVG = `
            <svg height="12px">
                <rect x="0px" y="0px" height="10px" width="10px" fill="${clr}"></rect>
                <text x="20px" y="10px" fill="black" font-style="italic" font-size="12px">- ${com}</text>
            </svg>
        `

        div.insertAdjacentHTML('beforeend', legendSVG);
        div.style.display = 'inline-block';
        div.style.height = '14px';

        return div;
    }

    static initTreshold(newBlock) {
         /** Create main HTML node*/
        let div = document.createElement('div');
        div.setAttribute('id', 'tresholdDimmer');

        let inputFields = document.createElement('div');
        let inputFieldsHeader = document.createElement('p');
        let inputFieldsRanges = document.createElement('div');
        inputFieldsHeader.innerHTML = 'Duration threshold (sec.):';
        inputFieldsRanges.style.display = 'flex';
        inputFieldsRanges.style.width = `${document.documentElement.offsetWidth ? 0.85 * document.documentElement.offsetWidth : 1520}px`;
        inputFieldsRanges.style.justifyContent = 'space-around';
        inputFieldsRanges.style.flexDirection = 'row';
        inputFieldsRanges.style.fontStyle = 'italic';
        inputFieldsRanges.style.fontSize = '15px';

        /** Getting max durations */
        let maxBackendDuration = 0;
        let maxXactDuration = 0;
        let maxStateDuration = 0;

        newBlock.data.forEach(elem => {
            let backend_duration = elem.backend_duration_ut;
            let xact_duration = elem.xact_duration_ut;
            let state_duration = elem.xact_duration_ut;

            /** If backend duration less then threshold then pass this elem */
            if (backend_duration > maxBackendDuration) {
                maxBackendDuration = backend_duration;
            }
            if (xact_duration > maxXactDuration) {
                maxXactDuration = xact_duration;
            }
            if (state_duration > maxStateDuration) {
                maxStateDuration = state_duration
            }
        })

        /** Backend duration treshold */
        let thresholdBackendField = document.createElement('div');
        thresholdBackendField.id = 'thresholdBackendField';

        let thresholdBackendInput = document.createElement('input');
        thresholdBackendInput.type = 'range';
        thresholdBackendInput.min = 0;
        thresholdBackendInput.max = Math.floor(maxBackendDuration);
        thresholdBackendInput.defaultValue = 0;
        thresholdBackendInput.style.width = '200px';

        let thresholdBackendName = document.createElement('span');
        thresholdBackendName.innerHTML = `Backend: ${thresholdBackendInput.value}`;
        thresholdBackendField.appendChild(thresholdBackendInput);
        thresholdBackendField.appendChild(thresholdBackendName);

        /** Xact duration treshold */
        let thresholdXactField = document.createElement('div');
        thresholdXactField.id = 'thresholdXactField';

        let thresholdXactInput = document.createElement('input');
        thresholdXactInput.type = 'range';
        thresholdXactInput.min = 0;
        thresholdXactInput.max = Math.floor(maxXactDuration);
        thresholdXactInput.defaultValue = 0;
        thresholdXactInput.style.width = '200px';

        let thresholdXactName = document.createElement('span');
        thresholdXactName.innerHTML = `Xact: ${thresholdXactInput.value}`;
        thresholdXactField.appendChild(thresholdXactInput);
        thresholdXactField.appendChild(thresholdXactName);

        /** State duration treshold */
        let thresholdStateField = document.createElement('div');
        thresholdStateField.id = 'thresholdStateField';

        let thresholdStateInput = document.createElement('input');
        thresholdStateInput.type = 'range';
        thresholdStateInput.min = 0;
        thresholdStateInput.max = Math.floor(maxStateDuration);
        thresholdStateInput.defaultValue = 0;
        thresholdStateInput.style.width = '200px';

        let thresholdStateName = document.createElement('span');
        thresholdStateName.innerHTML = `State: ${thresholdStateInput.value}`;
        thresholdStateField.appendChild(thresholdStateInput);
        thresholdStateField.appendChild(thresholdStateName);

        thresholdBackendField.style.display = 'flex';
        thresholdXactField.style.display = 'flex';
        thresholdStateField.style.display = 'flex';
        [thresholdBackendInput, thresholdXactInput, thresholdStateInput].forEach(elem => {
            elem.addEventListener('change', function() {
                let mainNode = document.getElementById('stateChangeSVG');
                let thresholdBackendValue = document.getElementById('thresholdBackendField').firstChild.value;
                let thresholdXactValue = document.getElementById('thresholdXactField').firstChild.value;
                let thresholdStateValue = document.getElementById('thresholdStateField').firstChild.value;

                document.getElementById('thresholdBackendField').childNodes[1].innerHTML = `Backend: ${thresholdBackendValue}`;
                document.getElementById('thresholdXactField').childNodes[1].innerHTML = `Xact: ${thresholdXactValue}`;
                document.getElementById('thresholdStateField').childNodes[1].innerHTML = `State: ${thresholdStateValue}`;

                mainNode.remove();
                div.insertAdjacentElement('afterend', new SessionChart().init(newBlock, thresholdBackendValue, thresholdXactValue, thresholdStateValue))
            });
        })
        inputFieldsRanges.appendChild(thresholdBackendField);
        inputFieldsRanges.appendChild(thresholdXactField);
        inputFieldsRanges.appendChild(thresholdStateField);

        inputFields.appendChild(inputFieldsHeader);
        inputFields.appendChild(inputFieldsRanges);

        div.appendChild(inputFields);

        return div;
    }

    init(newBlock, threshold_backend=null, threshold_xact=null, threshold_state=null) {
        /** Create main HTML node*/
        let div = document.createElement('div');
        div.setAttribute('id', 'stateChangeSVG');

        /** Declaring variables */
        let stateChanging = {};
        let reportStartUT = data.properties.report_start1_ut;
        let reportEndUT = data.properties.report_end2_ut ? data.properties.report_end2_ut : data.properties.report_end1_ut;
        let reportDuration = reportEndUT - reportStartUT;
        let totalSVGWidth = 1520;
        if (document.documentElement.offsetWidth) {
            totalSVGWidth = Math.ceil(0.85 * document.documentElement.offsetWidth / 12) * 12;
        }

        let timeLineWidth = 4;
        let proportion = totalSVGWidth / reportDuration;
        let linesCount = Math.ceil(totalSVGWidth / timeLineWidth);

        /** Building "Backend->Xact->State" tree */

        newBlock.data.forEach(elem => {
            let backend_duration = elem.backend_duration_ut;
            let xact_duration = elem.xact_duration_ut;
            let state_duration = elem.xact_duration_ut;

            /** If backend duration less then threshold then pass this elem */
            if (backend_duration < threshold_backend) {
                return;
            }
            if (xact_duration < threshold_xact) {
                return;
            }
            if (state_duration < threshold_state) {
                return;
            }

            let state_change = {};

            state_change[elem.state_change_ut] = {
                'pid': elem.pid,
                'state': elem.state,
                'start_ut': elem.state_change_ut,
                'start': elem.state_change,
                'duration': elem.state_duration_ut,
                'state_code': elem.state_code,
                'queryid': elem.queryid
            };
            if (stateChanging[`${elem.pid}_${elem.backend_start_ut}`] === undefined) {
                /** Backend statistics */
                let xact = {}
                xact[elem.xact_start_ut] = {
                    'pid': elem.pid,
                    'start_ut': elem.xact_start_ut,
                    'start': elem.xact_start,
                    'duration': elem.xact_duration_ut,
                    'state_changes': state_change
                }

                stateChanging[`${elem.pid}_${elem.backend_start_ut}`] = {
                    'pid': elem.pid,
                    'start': elem.backend_start,
                    'start_ut': elem.backend_start_ut,
                    'duration': elem.backend_duration_ut,
                    'count': 1,
                    'xacts': xact
                }
            } else {
                if (stateChanging[`${elem.pid}_${elem.backend_start_ut}`].xacts[elem.xact_start_ut] === undefined) {

                    stateChanging[`${elem.pid}_${elem.backend_start_ut}`].xacts[elem.xact_start_ut] = {
                        'pid': elem.pid,
                        'start_ut': elem.xact_start_ut,
                        'start': elem.xact_start,
                        'duration': elem.xact_duration_ut,
                        'state_changes': state_change
                    };
                } else {
                    stateChanging[`${elem.pid}_${elem.backend_start_ut}`]
                        .xacts[elem.xact_start_ut]
                        .state_changes[elem.state_change_ut] = state_change[elem.state_change_ut];
                }
                stateChanging[`${elem.pid}_${elem.backend_start_ut}`].count += 1;
            }
        })

        /** Compute backend's Best position in chart */

        let backends = Object.keys(stateChanging).map((key) => stateChanging[key]);

        let statesByStart = structuredClone(backends.sort((a,b) => a.start_ut - b.start_ut));
        let statesByEnd = structuredClone(backends.sort((a,b) => (a.start_ut + a.duration) - (b.start_ut + b.duration)));

        let statesByEndPointer = 0;
        let s = 0;

        for (let statesByStartPointer = 0; statesByStartPointer < statesByStart.length; statesByStartPointer++) {
            let firstBackend = statesByStart[statesByStartPointer];
            let firstBackendID = `${firstBackend.pid}_${firstBackend.start_ut}`
            let secondBackend = statesByEnd[statesByEndPointer];
            let secondBackendID = `${secondBackend.pid}_${secondBackend.start_ut}`
            let charCount = firstBackend.pid.toString().length ? firstBackend.pid.toString().length : 4;
            let charToTime = charCount * 8 * 1.2 / proportion;

            let isLater = (firstBackend.start_ut - charToTime) > secondBackend.start_ut + secondBackend.duration;

            if (isLater) {
                stateChanging[firstBackendID]['chart_line'] = stateChanging[secondBackendID]['chart_line'];
                statesByEndPointer += 1;
            } else {
                s += 1;
                stateChanging[firstBackendID]['chart_line'] = s;
            }
        }

        /** Drawing state change svg */
        let StateChangeSVGNested = '';

        Object.keys(stateChanging).forEach(backend => {
            let colour;
            let backendObj = stateChanging[backend];
            /** Drawing rectangle with backend */
            let title = '';
            title += `PID: ${backendObj.pid}\n`;
            title += `Backend start: ${new Date(backendObj.start_ut * 1000)}\n`;
            title += `Backend duration: ${Math.round(backendObj.duration * 100) / 100} sec`;

            let backendSVG = this.drawRect(backendObj, reportStartUT, proportion, backendObj['chart_line'], this.colours[2], this.colours[17], title, 'backend');
            /** Drawing PID */
            let pidSVG = this.drawPID(backendObj, reportStartUT, proportion, backendObj['chart_line']);

            StateChangeSVGNested += backendSVG;
            StateChangeSVGNested += pidSVG;

            Object.keys(backendObj.xacts).forEach(xact => {
                let xactObj = backendObj.xacts[xact];
                let title = '';
                title += `PID: ${xactObj.pid}\n`;
                title += `Xact start: ${new Date(xactObj.start_ut * 1000)} \n`;
                title += `Xact duration: ${Math.round(xactObj.duration * 100) / 100} sec \n`;
                title += `Backend start: ${new Date(backendObj.start_ut * 1000)} \n`;
                title += `Backend duration: ${Math.round(backendObj.duration * 100) / 100} sec`;

                let xactSVG = this.drawRect(xactObj, reportStartUT, proportion, backendObj['chart_line'], this.colours[2], this.colours[17], title, 'xact');

                StateChangeSVGNested += xactSVG;

                Object.keys(xactObj.state_changes).forEach(state_change => {
                    let stateChangeObj = xactObj.state_changes[state_change];
                    if (stateChangeObj.state_code === 1) {
                        /** idle in transaction */
                        colour = this.colours[6]
                    } else if (stateChangeObj.state_code === 3) {
                        /** active */
                        colour = this.colours[9]
                    }
                    let title = '';
                    title += `PID: ${stateChangeObj.pid} \n`;
                    title += `State: ${stateChangeObj.state} \n`;
                    title += `Query ID: ${stateChangeObj.queryid}\n`;
                    title += `State start: ${new Date(stateChangeObj.start_ut * 1000)} \n`;
                    title += `State duration: ${Math.round(stateChangeObj.duration * 100) / 100} sec \n`;
                    title += `Xact start: ${new Date(xactObj.start_ut * 1000)} \n`;
                    title += `Xact duration: ${Math.round(xactObj.duration * 100) / 100} sec \n`;
                    title += `Backend start: ${new Date(backendObj.start_ut * 1000)} \n`;
                    title += `Backend duration: ${Math.round(backendObj.duration * 100) / 100} sec`;

                    /** dataObj, reportStartUT, proportion, num, strokeColour, fillColour, title, klass */
                    let stateChangeSVG = this.drawRect(
                        stateChangeObj,
                        reportStartUT,
                        proportion,
                        backendObj['chart_line'],
                        colour,
                        colour,
                        title,
                        'stateChange',
                        `${backendObj.pid}_${xactObj.start_ut}_${stateChangeObj.start_ut}`
                    );

                    StateChangeSVGNested += stateChangeSVG;
                })
            })
        })

        /** Drawing timeline grid */
        let timeGridNestedSVG = '';

        for (let i = 0; i <= linesCount; i++) {
            let pointTime = new Date((i * timeLineWidth / proportion + reportStartUT) * 1000);

            let timeLineSVG = `
                <path
                    id="line${i}"
                    d="M ${i * timeLineWidth} 0 L ${i * timeLineWidth} ${(s + 1) * 20 }"
                    stroke="lightgrey"
                    stroke-dasharray="1">
                </path>
            `;
            let hiddenLineSVG = `
                <path
                    d="M ${i * timeLineWidth} 0 L ${i * timeLineWidth} ${(s + 1) * 20}"
                    stroke="white"
                    stroke-width="${timeLineWidth}"
                    >
                    <title>${pointTime}</title>
                </path>
            `;

            timeGridNestedSVG += hiddenLineSVG;
            timeGridNestedSVG += timeLineSVG;
        }

        let StateChangeSVG = `<svg>${StateChangeSVGNested}</svg>`
        let timeGridSVG = `<svg>${timeGridNestedSVG}</svg>`;

        let svg = `
            <svg
                width="${totalSVGWidth}px"
                height="${(s * 20) + 20}px"
            >
                ${timeGridSVG}
                ${StateChangeSVG}
            </svg>
        `;

        div.insertAdjacentHTML('beforeend', svg);

        div.style.display = 'flex';
        div.style.flexDirection = 'column';

        /** Add eventListeners on SVG objects */
        div.querySelectorAll('rect.stateChange').forEach(elem => {
            elem.addEventListener('click', e => {
                let target = e.target.dataset.target;
                window.location.replace(`#${target}`);
                let targetNode = document.getElementById(target);
                targetNode.click();
            })
        })

        return div;
    }

    drawSessionChartLegend() {
        /** Create legend */
        let legend = document.createElement('div');
        legend.style.display = 'flex';
        legend.style.flexDirection = 'column';

        legend.appendChild(this.drawLegend(this.colours[6], 'idle in transaction'));
        legend.appendChild(this.drawLegend(this.colours[9], 'active'));
        legend.appendChild(this.drawLegend(this.colours[2], 'backend with pid'));

        return legend;
    }
}