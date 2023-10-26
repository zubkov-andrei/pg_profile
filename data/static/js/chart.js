class BaseChart {
    static drawIntoTable(cls, newRow, column, data) {
        /** Order data */
        let key = column.ordering[0] === '-' ? column.ordering.substring(1) : column.ordering;
        let value = column.id;
        let direction = column.ordering[0] === '-' ? -1 : 1;
        let newCell = newRow.insertCell(-1);

        if (Utilities.sum(data, value) > 0) {
            let orderedData = Utilities.sort(data, key, direction);

            /** Draw SVG */
            let svg = cls.drawSVG(orderedData, value, key);

            /** Append SVG to table */
            newCell.appendChild(svg);
        }
    }
}
class PipeChart extends BaseChart {
    static drawSVG(orderedData, value, key) {
        let x = 0; // Start position of nested svg

        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '2em');

        orderedData.forEach(elem => {
            let width = Math.floor(elem[value]);
            let nestedSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            nestedSvg.setAttribute('x', `${x}%`);
            nestedSvg.setAttribute('height', '2em');
            nestedSvg.setAttribute('width', `${width}%`);

            let title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            title.innerHTML = `${elem.objname}: ${elem[value]}`;

            let text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            text.setAttribute('y', '70%');
            text.setAttribute('x', '0.3em');
            text.innerHTML = `${elem.objname}: ${elem[key]}`;

            let rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            rect.setAttribute('height', '90%');
            rect.setAttribute('x', '0%');
            rect.setAttribute('y', '10%');
            rect.setAttribute('ry', '15%');
            rect.setAttribute('stroke', 'black');
            rect.setAttribute('stroke-width', '1px');
            rect.setAttribute('width', '100%');
            rect.setAttribute('fill', `#${elem.objcolor}`);

            nestedSvg.appendChild(title);
            nestedSvg.appendChild(rect);
            svg.appendChild(nestedSvg);
            nestedSvg.appendChild(text);

            x += width;
        })

        return svg;
    }
    static drawIntoTable(newRow, column, data) {
        BaseChart.drawIntoTable(PipeChart, newRow, column, data);
        return true;
    }
}

class PieChart extends BaseChart {
    static drawPieSlice(startRad, diffRad, radius, center, elem, key) {
        /** Build title */
        let nestedSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        let title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        title.innerHTML = `${elem.objname}: ${elem[key]}`;

        /** If the Pie is just a circle */
        if (diffRad >= Math.PI * 1.999) {
            let circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', center[0]);
            circle.setAttribute('cy', center[1]);
            circle.setAttribute('r', radius);
            circle.setAttribute('fill', `#${elem.objcolor}`);
            circle.setAttribute('stroke', 'black');
            circle.setAttribute('stroke-width', '1px');

            nestedSvg.appendChild(title);
            nestedSvg.appendChild(circle);
            return nestedSvg;
        }
        /** Building M attr */
        const hoverOffset = 5;
        let startPointX = center[0] + radius * Math.cos(startRad);
        let startPointY = center[1] + radius * Math.sin(startRad);
        let startPoint = `M ${startPointX} ${startPointY}`;
        let arcFinishX = center[0] + radius * Math.cos(startRad + diffRad);// 150;
        let arcFinishY = center[1] + radius * Math.sin(startRad + diffRad); // 0
        let arcAngle = 0;
        let arcType = diffRad <= Math.PI ? 0 : 1;
        let arcClockwise = 1;
        let arc = `A ${radius} ${radius} ${arcAngle} ${arcType} ${arcClockwise} ${arcFinishX} ${arcFinishY}`;
        let lineOne = `L ${center[0]} ${center[1]}`;
        let lineTwo = `L ${startPointX} ${startPointY}`;
        let d = `${startPoint} ${arc} ${lineOne} ${lineTwo} Z`;
        let slice = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        slice.setAttribute('d', d);
        slice.setAttribute('fill', `#${elem.objcolor}`);
        slice.setAttribute('stroke', 'black');
        slice.setAttribute('stroke-width', '1px');

        slice.addEventListener('mouseover', function (){
            let centerOne = center[0] + hoverOffset * Math.cos(startRad + diffRad / 2);
            let centerTwo = center[1] + hoverOffset * Math.sin(startRad + diffRad / 2);

            startPointX = centerOne + radius * Math.cos(startRad);
            startPointY = centerTwo + radius * Math.sin(startRad);
            startPoint = `M ${startPointX} ${startPointY}`;

            arcFinishX = centerOne + radius * Math.cos(startRad + diffRad);// 150;
            arcFinishY = centerTwo + radius * Math.sin(startRad + diffRad); // 0

            arc = `A ${radius} ${radius} ${arcAngle} ${arcType} ${arcClockwise} ${arcFinishX} ${arcFinishY}`;
            lineOne = `L ${centerOne} ${centerTwo}`;
            lineTwo = `L ${startPointX} ${startPointY}`;

            let dHover = `${startPoint} ${arc} ${lineOne} ${lineTwo} Z`;
            slice.setAttribute('d', dHover);
        })
        slice.addEventListener('mouseout', function () {
            slice.setAttribute('d', d);
        })

        nestedSvg.appendChild(title);
        nestedSvg.appendChild(slice);
        return nestedSvg;
    }
    static drawLegendItem(num, elem, key) {
        let legendItem = document.createElementNS('http://www.w3.org/2000/svg', 'svg');

        let square = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        let y = num*20 + 20;
        square.setAttribute('x', '350');
        square.setAttribute('y', `${y}`);

        square.setAttribute('height', '15');
        square.setAttribute('width', '15');
        square.setAttribute('stroke', 'black');
        square.setAttribute('stroke-width', '1px');
        square.setAttribute('fill', `#${elem.objcolor}`);

        let text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('y', `${y+12}`);
        text.setAttribute('x', '370');
        text.setAttribute('font-size', '10');
        text.innerHTML = `${elem.objname}: ${elem[key]}`;

        legendItem.appendChild(square);
        legendItem.appendChild(text);

        return legendItem;
    }
    static drawSVG(orderedData, value, key) {
        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        let legend = document.createElementNS('http://www.w3.org/2000/svg', 'svg');

        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '400');

        const center = [150, 150];
        const radius = 145;

        let startRad = 0;

        orderedData.forEach((elem, num) => {
            let diffRad = (elem[value] / 100) * Math.PI * 2;

            let slice = PieChart.drawPieSlice(startRad, diffRad, radius, center, elem, key);
            let legendItem = PieChart.drawLegendItem(num, elem, key);

            svg.appendChild(slice);
            legend.appendChild(legendItem);

            startRad += diffRad;
        })

        svg.appendChild(legend);

        return svg;
    }
    static drawIntoTable(newRow, column, data) {
        BaseChart.drawIntoTable(PieChart, newRow, column, data);
        return true;
    }
}