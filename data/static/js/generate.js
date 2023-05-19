class BaseSection {

    constructor(section) {
        this.section = section;
        this.sectionHasContent = ('content' in section);
        this.sectionHasBlocks = ('header' in section);
        this.sectionHasTitle = ('tbl_cap' in section);
    }

    /**
     * Method builds and returns html tag with title of section
     * @returns {HTMLHeadingElement}
     */
    static buildTitle(section) {
        let title = document.createElement('h3');
        title.innerHTML = section.tbl_cap;
        if (section.href) {
            title.id = section.href;
        }
        return title;
    }

    init() {
        let div = document.createElement('div');

        if (this.section.sect_id) {
            div.setAttribute('id', this.section.sect_id);
        }
        if (this.sectionHasTitle) {
            div.appendChild(BaseSection.buildTitle(this.section));
        }
        if (this.sectionHasBlocks) {
            let table = document.createElement('table');

            /** ID of table determined as sect_id + '_t' literal */
            table.setAttribute('id', `${this.section.sect_id}_t`);

            for (let i = 0; i < this.section.header.length; i++) {

                /** Creating blocks from header and data in JSON one by one */
                let newBlock = structuredClone(this.section);
                newBlock.header = this.section.header[i];

                if (newBlock.header.source) {
                    newBlock.data = data.datasets[newBlock.header.source];

                    /** Filtering data if expected */
                    if (newBlock.header.filter) {
                        newBlock.data = Utilities.filter(newBlock.data, newBlock.header.filter);
                    }
                    /** Ordering data if expected */
                    if (newBlock.header.ordering) {
                        let direction = 1;
                        let key = newBlock.header.ordering;

                        if (newBlock.header.ordering.startsWith('-')) {
                            direction = -1;
                            key = newBlock.header.ordering.slice(1);
                        }
                        newBlock.data = Utilities.sort(newBlock.data, key, direction);
                    }
                    /** Limiting data if expected */
                    if (newBlock.header.limit) {
                        newBlock.data = Utilities.limit(newBlock.data, Number.parseInt(data.properties[newBlock.header.limit]));
                    }
                } else {
                    newBlock.data = this.section.data[i];
                }

                /** Class of table determined by parameter 'class' in header (section_structure).
                 * If there are multiple objects in header, then class of table will be overriden */
                table.setAttribute('class', newBlock.header.class);

                if (newBlock.header.type === 'row_table') {
                    div.appendChild(new HorizontalTable(newBlock, table).init());
                } else if (newBlock.header.type === 'column_table') {
                    div.appendChild(new VerticalTable(newBlock, table).init());
                }
            }
        }
        if (this.sectionHasContent) {
            const divider = '{func_output}';
            let contentOne = this.section.content.split(divider)[0];
            let contentTwo = this.section.content.split(divider)[1];
            let table = div.querySelector('table');

            if (table) {
                if (contentOne) {
                    table.insertAdjacentHTML('beforebegin', contentOne);
                }
                if (contentTwo) {
                    table.insertAdjacentHTML('afterend', contentTwo);
                }
            }
        }

        return div;
    }
}

class BaseTable extends BaseSection {
    static uniqueHeaders = {
        'waitEventsBlock': BaseTable.buildWaitEventsBlockHeader
    }
    static uniqueCells = {
        'queryId': BaseTable.buildQueryIdCell,
        'jitCellId': BaseTable.buildJitCellId,
        'jitTimeCell': BaseTable.buildJitTimeCell,
        'interval': BaseTable.buildIntervalCell,
        'queryTextId': BaseTable.buildQueryTextIdCell,
        'planId': BaseTable.buildPlanIdCell,
        'queryText': BaseTable.buildQueryTextCell,
        'waitEvent': BaseTable.buildWaitEventDetailsCell,
        'pipeChart': PipeChart.drawIntoTable,
        'pieChart': PieChart.drawIntoTable
    };
    static properties = {
        'topn': data.properties.topn,
        'end1_id': data.properties.end1_id,
        'end2_id': data.properties.end2_id,
        'start1_id': data.properties.start1_id,
        'start2_id': data.properties.start2_id,
        'description': data.properties.description,
        'report_end1': data.properties.report_end1,
        'report_end2': data.properties.report_end2,
        'server_name': data.properties.server_name,
        'report_start1': data.properties.report_start1,
        'report_start2': data.properties.report_start2,
        'max_query_length': data.properties.max_query_length,
        'pgprofile_version': data.properties.pgprofile_version,
        'server_description': data.properties.server_description,
        'checksum_fail_detected': data.properties.checksum_fail_detected,
        'interval1_duration_sec': data.properties.interval1_duration_sec,
        'interval2_duration_sec': data.properties.interval2_duration_sec
    }

    constructor(section, table) {
        super(section);
        this.table = table;
    }

    static buildWaitEventsBlockHeader(table, header) {
        let tr0 = document.createElement('tr');
        let tr1 = document.createElement('tr');
        let tr2 = document.createElement('tr');

        let th0 = document.createElement('th');
        let mainColumn = header.columns[0];

        th0.innerHTML = mainColumn.caption;
        th0.setAttribute('colspan', '100%');
        tr0.appendChild(th0);

        mainColumn.columns.forEach(column => {
            let th1 = document.createElement('th');
            th1.innerHTML = column.caption;
            if (column.columns) {
                th1.setAttribute('colspan', column.columns.length);
                column.columns.forEach(column => {
                    let th2 = document.createElement('th');
                    th2.innerHTML = column.caption;
                    if (column.title) {
                        th2.setAttribute('title', column.title);
                    }
                    tr2.appendChild(th2);
                })
            } else {
                th1.setAttribute('rowspan', '2');
            }
            tr1.appendChild(th1);

        })
        table.appendChild(tr0);
        table.appendChild(tr1);
        table.appendChild(tr2);
    }

    /**
     * Build cell with total_jit_time data and with link to JIT statistics table
     * @param newRow row into which the cell need to be build
     * @param column cell meta-data
     * @param row object data
     */
    static buildJitTimeCell(newRow, column, row) {

        let newCell = newRow.insertCell(-1);
        if (row[column.id]) {
            newCell.setAttribute('class', 'table_obj_value');
            let p = document.createElement('p');
            let a = document.createElement('a');
            a.href = `#jit_${row.hexqueryid}_${row.datid}_${row.userid}_${row.toplevel}`;
            a.innerHTML = row[column.id];
            p.appendChild(a);
            newCell.appendChild(p);
        }

        return !!row[column.id];
    }

    /**
     * Build cell with queryId data and with link to query text table
     * @param newRow row into which the cell need to be build
     * @param column cell meta-data
     * @param row object data
     */
    static buildQueryIdCell(newRow, column, row) {

        let newCell = newRow.insertCell(-1);

        newCell.setAttribute('class', column.class);

        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        /** Tag with hex(queryid) and link */
        let p1 = document.createElement('p');
        let a1 = document.createElement('a');
        a1.href = `#${row.hexqueryid}`;
        a1.innerHTML = row.hexqueryid;
        p1.appendChild(a1);

        /** Tag with md5 of (userid::text || datid::text || queryid::text) */
        let p2 = document.createElement('p');
        let small = document.createElement('small');
        p2.appendChild(small);
        small.innerHTML = `[${row.hashed_ids}]`;

        /** If toplevel is false then add special tag*/
        if (!row.toplevel) {
            small = document.createElement('small');
            p2.appendChild(small);
            small.innerHTML = '(N)';
            small.title = 'Nested level';
        }
        newCell.appendChild(p1);
        newCell.appendChild(p2);

        return !!row.hexqueryid;
    }

    static buildJitCellId(newRow, column, row) {
        BaseTable.buildQueryIdCell(newRow, column, row);
        newRow.firstChild.setAttribute('id', `jit_${row.hexqueryid}_${row.datid}_${row.userid}_${row.toplevel}`);
    }
    static buildPlanIdCell(newRow, column, row) {

        let newCell = newRow.insertCell(-1);

        newCell.setAttribute('class', column.class);

        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        /** Tag with hex(planid) and link */
        let p1 = document.createElement('p');
        let a1 = document.createElement('a');
        a1.setAttribute('href', `#${row.hexqueryid}_${row.hexplanid}`);
        a1.innerHTML = row.hexplanid;
        p1.appendChild(a1);

        newCell.appendChild(p1);

        return !!row.hexplanid;
    }

    static buildQueryTextIdCell(newRow, column, row) {
        let cell = newRow.insertCell(-1);
        let columnId;

        if (row['hexplanid']) {
            columnId = row['hexplanid'];
            newRow.classList.add('plantext');
            cell.setAttribute('id', `${row[column.id]}_${columnId}`);
        } else {
            columnId = row[column.id];
            newRow.classList.add('statement');
            cell.setAttribute('id', columnId);
        }

        let newText = document.createTextNode(columnId);

        /** Setting attributes to new cell */
        cell.setAttribute('class', column.class);

        /** Count query_text* keys in row and set it as rowspan */
        const countMatches = [...Object.keys(row).join(' ').matchAll(new RegExp('query_text*', 'g'))].length;
        cell.setAttribute('rowspan', `${countMatches}`);

        cell.appendChild(newText);

        return !!columnId;
    }

    static buildQueryTextCell(newRow, column, row) {
        if (row['hexplanid'] || row[column.id]) {
            let newCell = newRow.insertCell(-1);
            let newText = null;

            /** Setting attributes to new cell */
            newCell.setAttribute('class', column.class);

            if (row['hexplanid'] && row[column.id]) {
                newText = `<pre>${row[column.id]}</pre>`;
                newCell.insertAdjacentHTML('afterbegin', newText);
            } else if (row[column.id]) {
                newText = document.createTextNode(row[column.id]);
                newCell.appendChild(newText);
            }
        }
        return !!row[column.id];
    }

    static buildIntervalCell(newRow, column, row) {
        let cell = newRow.insertCell(-1);
        let newText = document.createTextNode(column.id);

        /** Setting attributes to new cell */
        if (column.class) {
            cell.setAttribute('class', 'table_obj_value');
        }
        if (column.rowspan) {
            cell.setAttribute('rowspan', '2');
        }
        if (column.title) {
            cell.setAttribute('title', BaseTable.getTagTitle(column.title));
        }

        cell.appendChild(newText);

        return true;
    }

    static buildWaitEventDetailsCell(newRow, column, row) {
        let newCell = newRow.insertCell(-1);

        /** Setting id on tr */
        newRow.setAttribute('id', `${row.wait_event_type}_${row.hexqueryid}_${row.hexplanid}`);

        /** Setting rowspan attributes to new cell */
        if (column.rowspan) {
            newCell.setAttribute('rowspan', row.rowspan);
        }
        /** Setting class attribute to new cell */
        if (column.class) {
            newCell.setAttribute('class', column.class);
            newCell.classList.add('table_obj_value');
        }
        /** If data exists only */
        let dataExists = Boolean(row[column.id]);
        if (dataExists) {
            for (let i = 0; i < row[column.id].length; i++) {
                let details = row[column.id][i];

                if (details.wait_event) {
                    let p = document.createElement('p');
                    let a = document.createElement('a');
                    if (row.wait_event_type === 'Total') {
                        a.href = `#${details.wait_event}_${details.hexqueryid}_${details.hexplanid}`;
                    }
                    a.innerHTML = details.wait_event;
                    p.appendChild(a);
                    let colons = document.createTextNode(': ');
                    p.appendChild(colons);
                    let strong = document.createElement('strong');
                    strong.innerHTML = details.waited;
                    p.appendChild(strong);
                    newCell.appendChild(p);
                } else if (details.wait_event) {
                    let p = document.createElement('p');
                    p.insertAdjacentHTML('afterbegin', `${details.wait_event}: <strong>${details.waited}</strong>`);
                    newCell.appendChild(p);
                }
            }
        }

        return true;
    }

    /**
     * Split object into several objects with unique fields
     *
     * @param column, {json object}, structure of column
     * @returns objects, {json[]}, array with json objects */
    static bifurcateObject(column) {

        let objects = [];
        let keys = Object.keys(column);

        for (let i = 0; i < column.id.length; i++) {
            let newObj = structuredClone(column);

            for (let j = 0; j < keys.length; j++) {
                if (typeof newObj[keys[j]] === 'object') {
                    newObj[keys[j]] = column[keys[j]][i];
                }
            }
            objects.push(newObj);
        }
        return objects;
    }

    static getTagTitle(title) {
        const TITLES = {
            'properties.timePeriod1': `(${BaseTable.properties.report_start1} - ${BaseTable.properties.report_end1})`,
            'properties.timePeriod2': `(${BaseTable.properties.report_start2} - ${BaseTable.properties.report_end2})`,
            'properties.timePeriod1,properties.timePeriod2': 'Sample\'s time period'
        }
        if (TITLES[title] !== undefined) {
            return TITLES[title];
        }
        return title;
    }

    static hasSpecialClass(column) {
        if (column.class) {
            let classList = column.class.split(' ');

            for (let i = 0; i < classList.length; i++) {
                let klass = classList[i].trim();
                if (BaseTable.uniqueCells[klass]) {
                    return klass;
                }
            }
        }

        return false;
    }

    static collectHeader(header, deep, resultMatrix) {
        if (resultMatrix === null) {
            resultMatrix = [];
        }

        if (header.caption) {

            if (!resultMatrix[deep]) {
                resultMatrix.push([]);
            }
            let sumCols = 0;

            if (header.columns) {
                deep++;
                header.columns.forEach(column => {
                    BaseTable.collectHeader(column, deep, resultMatrix);
                    if (column.columns) {
                        sumCols += column.columns.length - 1;
                    }
                })
                deep--;
            }

            /** Create header Object */
            let th = {
                'caption': header.caption,
                'colspan': header.columns ? header.columns.length + sumCols : 1,
                'rowspan': deep === 0 && !('columns' in header) ? 2 : 1
            };
            if (header.id) {th['id'] = header.id}
            if (header.title) {th['title'] = BaseTable.getTagTitle(header.title)}

            resultMatrix[deep].push(th);
        } else {
            if (header.columns) {
                header.columns.forEach(column => {
                    BaseTable.collectHeader(column, deep, resultMatrix);
                })
            }
        }
        return resultMatrix;
    }

    /**
     * Method initiates a html-table and builds its header
     * @returns {HTMLTableElement} tag wit table
     */
    buildHeader() {

        /** Checking if the header has unique class */
        let classList = this.section.header.class.split(' ');
        for (let i = 0; i < classList.length; i++) {

            /** If true, build header by special method */
            let klass = classList[i].trim();
            if (BaseTable.uniqueHeaders[klass]) {
                return BaseTable.uniqueHeaders[klass](this.table, this.section.header);
            }
        }

        /** Collecting header into matrix */
        let headerMatrix = BaseTable.collectHeader(this.section.header, 0, null);

        /** If header is not unique build it like regular */
        headerMatrix.forEach(row => {
            let tr = document.createElement('tr');
            row.forEach(column => {
                let th = document.createElement('th');

                /** Setting demanded attrs */
                th.innerHTML = column.caption;
                th.setAttribute('rowspan', headerMatrix.length === 1 ? 1 : column.rowspan);
                th.setAttribute('colspan', column.colspan);

                /** Setting option attrs */
                if (column.title) {
                    th.setAttribute('title', column.title);
                }
                if (column.id) {
                    th.setAttribute('id', column.id);
                }
                tr.appendChild(th);
            });
            this.table.appendChild(tr);
        });
        if (headerMatrix.length === 1) {
            let emptyRow = document.createElement('tr');
            this.table.appendChild(emptyRow);
        }
    }

    init() {
        this.buildHeader();
        this.insertRows();
        return this.table;
    }
}


class HorizontalTable extends BaseTable {
    /**
     *  Build cell
     *
     * @param newRow, {html node}, row into which the cell need to be build
     * @param column, {json obj}, cell meta-data
     * @param row, {json obj}, object data
     *
     * @returns boolean, return true if cell is empty, otherwise return false  */
    static buildCell(newRow, column, row) {
        /** Applying special creation method if the cell is a special one.
         *  Checking if any from columns classes includes in special classes list */
        let specialClass = BaseTable.hasSpecialClass(column);

        if (specialClass) {

            let notEmpty = BaseTable.uniqueCells[specialClass](newRow, column, row);

            /** return isEmpty (boolean) */
            return !notEmpty;
        }

        let newCell = newRow.insertCell(-1);

        /** Setting rowspan attributes to new cell */
        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        /** Setting attributes to new cell */
        if (column.class) {
            newCell.setAttribute('class', column.class);
        }
        /** If data exists only */
        if (row[column.id]) {
            let newText = document.createTextNode(row[column.id]);
            newCell.appendChild(newText);

            return false;
        }
        return true;
    }

    /**
     * Recursive collection of last-leave columns
     * @param header - section structure in JSON
     * @param array - array with columns that not divided in sub-rows
     * @param matrix - 2D array with sub-rows, where each sub-row represented as vector
     * @returns {*[][]}
     */
    static collectColumns(header, array, matrix) {
        if (array === null) {
            array = [];
        }
        if (matrix === null) {
            matrix = [];
        }

        if ('columns' in header) {
            for (let i = 0; i < header.columns.length; i++) {
                let column = header.columns[i];
                HorizontalTable.collectColumns(column, array, matrix);
            }
        } else {
            if (typeof header.id === 'string') {
                array.push(header);
            } else if (typeof header.id === 'object') {
                matrix.push(BaseTable.bifurcateObject(header));
            }
        }
        if (matrix.length) {
            /** Getting columns by matrix transposition */
            matrix = matrix[0].map((_, colIndex) => matrix.map(row => row[colIndex]));
        }
        return [array, matrix];
    }

    /** Method returns collection of columns (last leaves in tree) in table.
     * if some objects are divided into sub-rows then the method returns
     * each row in vector of matrix. In case when there is only one row
     * in object, method returns single-vector matrix.
     *
     * @returns matrix, {2D array} */
    static getColumns(columns) {
        let matrixCollection = HorizontalTable.collectColumns(columns, null, null);
        let singeRowColumns = matrixCollection[0];
        let matrixWithVectors = matrixCollection[1];
        let newMatrix = Array.from(matrixCollection[1]);

        if (matrixWithVectors[0]) {
            newMatrix[0] = singeRowColumns.concat(matrixWithVectors[0]);
        } else {
            newMatrix[0] = singeRowColumns;
        }
        return newMatrix;
    }

    /**
     * Setting data attributes to row for highlighting
     * @param newRow: html tag with table row
     * @param row: Object with data for populating html table
     */
    static setDataAttrs(newRow, row) {
        let attributesMap = {
            'dbname': 'data-dbname',
            'hexqueryid': 'data-hexqueryid',
            'queryid': 'data-queryid',
            'hexplanid': 'data-hexplanid',
            'toplevel': 'data-toplevel',
            'userid': 'data-userid',
            'wait_event_type': 'data-waiteventtype',
            'wait_event': 'data-waitevent',
            'tablespacename': 'data-tablespacename',
            'schemaname': 'data-schemaname',
            'relname': 'data-relname',
            'indexrelname': 'data-indexrelname',
            'funcname': 'data-funcname'
        }
        for (const [key, value] of Object.entries(attributesMap)) {
            if (row[key]) {
                newRow.setAttribute(value, row[key]);
            }
        }
    }

    /**
     * Inserting rows into html-table
     */
    insertRows() {

        let columns = HorizontalTable.getColumns(this.section.header);  // Getting columns matrix
        let rows = this.section.data;  // Getting json array with data
        let isParent = true;

        /** Iterate over the data */
        for (let i = 0; i < rows.length; i++) {

            let row = rows[i];

            /** Iterate over columns matrix */
            for (let j = 0; j < columns.length; j++) {

                /** Build <tr> tag for each row in matrix */
                let newRow = this.table.insertRow(-1);
                if (this.table.classList.contains('diff') || this.table.classList.contains('toast')) {
                    newRow.classList.add(isParent ? 'int1' : 'int2');
                    isParent = !isParent;
                }
                if (row.klass) {
                    newRow.setAttribute('class', row.klass);
                }
                /** Set classes for highlighting rows */
                HorizontalTable.setDataAttrs(newRow, row);

                /** Array to collect empty cells */
                let isEmpty = [];

                /** Iterate over all unique columns */
                for (let k = 0; k < columns[j].length; k++) {

                    /** Build cells inside <tr> */
                    isEmpty.push(HorizontalTable.buildCell(newRow, columns[j][k], row));
                }

                /** Remove row, if all cells in row are empty */
                if (isEmpty.every(Boolean)) {
                    newRow.style.visibility = 'collapse';
                    let prevSib = newRow.previousSibling;
                    prevSib.style.verticalAlign = 'baseline';
                }
            }
        }
    }
}

class VerticalTable extends BaseTable {
    static getColumns(section) {

        let columns = [];
        let rows = section.header.rows;

        for (let i = 0; i < rows.length; i++) {
            if (typeof rows[i].id === 'object') {
                columns.push(BaseTable.bifurcateObject(rows[i]));
            } else {
                columns.push(rows[i]);
            }
        }
        return columns;
    }

    static buildCell(newRow, column, row) {
        let specialClass = BaseTable.hasSpecialClass(column);
        if (specialClass) {
            BaseTable.uniqueCells[specialClass](newRow, column, row);
            return;
        }
        /** Insert column with metric */
        let newCell = newRow.insertCell(-1);
        if ('caption' in column) {
            let newText = document.createTextNode(column['caption']);
            newCell.appendChild(newText);
        }
        if ('title' in column) {
            newCell.setAttribute('title', column.title);
        }
        if ('id' in column) {
            newCell.innerHTML = row[0][column.id];
        }
        if ('class' in column) {
            newCell.setAttribute('class', column.class);
        }
    }

    insertRows() {
        let columns = VerticalTable.getColumns(this.section);
        let rows = this.section.data;
        for (let i = 0; i < columns.length; i++) {
            let newRow = this.table.insertRow(-1);
            VerticalTable.buildCell(newRow, columns[i], rows);
            columns[i].cells.forEach(cell => {
                VerticalTable.buildCell(newRow, cell, rows);
            });
        }
    }
}