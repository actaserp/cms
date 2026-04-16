class FlexGridContextMenu {
    constructor(grid) {
        let host = grid.hostElement, menu = this._buildMenu(grid);
        host.addEventListener('contextmenu', (e) => {
            // select the cell/column that was clicked
            let sel = grid.selection, ht = grid.hitTest(e), row = ht.getRow();
            switch (ht.panel) {
                case grid.cells:
                    let colIndex = ht.col;
                    // if this is a group header, select the group column
                    if (row instanceof wijmo.grid.GroupRow && row.dataItem instanceof wijmo.collections.CollectionViewGroup) {
                        let gd = row.dataItem.groupDescription;
                        if (gd instanceof wijmo.collections.PropertyGroupDescription) {
                            let col = grid.getColumn(gd.propertyName);
                            if (col && col.index > -1) {
                                colIndex = col.index;
                            }
                        }
                    }
                    grid.select(ht.row, colIndex);
                    break;
                case grid.columnHeaders:
                    grid.select(sel.row, ht.col);
                    break;
                case grid.rowHeaders:
                    grid.select(ht.row, sel.col);
                    break;
                default:
                    return; // invalid panel
            }
            // show the menu for the current column
            if (grid.selection.col > -1) {
                e.preventDefault(); // cancel the browser's default menu
                menu.show(e); // and show ours
            }
        }, true);
    }

    _buildMenu(grid) {
        let menu = new wijmo.input.Menu(document.createElement('div'), {
            owner: grid.hostElement,
            displayMemberPath: 'header',
            subItemsPath: 'items',
            commandParameterPath: 'cmd',
            dropDownCssClass: 'ctx-menu',
            openOnHover: true,
            closeOnLeave: true,
            itemsSource: [
                {
                    header: 'Sort', items: [
                        {header: 'Ascending', cmd: 'SRT_ASC'},
                        {header: 'Descending', cmd: 'SRT_DESC'},
                        {header: 'No Sort', cmd: 'SRT_NONE'},
                        {header: '-'},
                        {header: 'Clear All Sorts', cmd: 'SRT_CLR'}
                    ]
                },
                {header: '-'},
                {header: 'Pin/Unpin', cmd: 'PIN'},
                {header: '-'},
                {header: 'AutoSize', cmd: 'ASZ'},
                {header: 'AutoSize All', cmd: 'ASZ_ALL'},
                {header: '-'},
                {header: 'Group/Ungroup', cmd: 'GRP'},
                {header: 'Clear All Groups', cmd: 'GRP_CLR'},
                {header: '-'},
                {
                    header: 'Export', items: [
                        {header: 'CSV', cmd: 'X_CSV'},
                        {header: 'XLSX', cmd: 'X_XLSX'},
                        {header: 'PDF', cmd: 'X_PDF'},
                    ]
                }
            ],
            command: {
                // enable/disable menu commands
                canExecuteCommand: (cmd) => {
                    let view = grid.collectionView, col = grid.columns[grid.selection.col];
                    switch (cmd) {
                        case 'SRT_ASC':
                            return col.currentSort != '+';
                        case 'SRT_DESC':
                            return col.currentSort != '-';
                        case 'SRT_NONE':
                            return col.currentSort != null;
                        case 'SRT_CLR':
                            return view.sortDescriptions.length > 0;
                        case 'PIN':
                            return true; // toggle pin
                        case 'ASZ':
                        case 'ASZ_ALL':
                            return true;
                        case 'GRP':
                            return col.dataType != wijmo.DataType.Number; // don't group numbers
                        case 'GRP_CLR':
                            return view.groupDescriptions.length > 0;
                    }
                    return true;
                },
                // execute menu commands
                executeCommand: (cmd) => {
                    let view = grid.collectionView, cols = grid.columns, col = cols[grid.selection.col],
                        sd = view.sortDescriptions, gd = view.groupDescriptions;
                    let downloadFileName = grid.downloadFileName || 'FlexGrid';

                    switch (cmd) {
                        case 'SRT_ASC':
                        case 'SRT_DESC':
                        case 'SRT_NONE':
                            if (grid.allowSorting != wijmo.grid.AllowSorting.MultiColumn) {
                                sd.clear();
                            } else {
                                for (let i = 0; i < sd.length; i++) {
                                    if (sd[i].property == col.binding) {
                                        sd.removeAt(i);
                                        break;
                                    }
                                }
                            }
                            if (cmd != 'SRT_NONE') {
                                sd.push(new wijmo.collections.SortDescription(col.binding, cmd == 'SRT_ASC'));
                            }
                            break;
                        case 'SRT_CLR':
                            sd.clear();
                            break;
                        case 'PIN':
                            let fCols = grid.frozenColumns;
                            if (col.index >= fCols) { // pinning
                                cols.moveElement(col.index, fCols, false);
                                cols.frozen++;
                            } else { // unpinning
                                cols.moveElement(col.index, fCols - 1, false);
                                cols.frozen--;
                            }
                            break;
                        case 'ASZ':
                            grid.autoSizeColumn(col.index);
                            break;
                        case 'ASZ_ALL':
                            grid.autoSizeColumns(0, grid.columns.length - 1);
                            break;
                        case 'GRP':
                            // remove group
                            for (let i = 0; i < gd.length; i++) {
                                if (gd[i].propertyName == col.binding) {
                                    gd.removeAt(i);
                                    return; // we're done
                                }
                            }
                            // add group
                            gd.push(new wijmo.collections.PropertyGroupDescription(col.binding));
                            break;
                        case 'GRP_CLR':
                            gd.clear();
                            break;
                        // export
                        case 'X_CSV':
                            let rng = new wijmo.grid.CellRange(0, 0, grid.rows.length - 1, grid.columns.length - 1),
                                csv = grid.getClipString(rng, wijmo.grid.ClipStringOptions.CSV, true, false);
                            wijmo.saveFile(csv, downloadFileName + '.csv');
                            break;
                        case 'X_XLSX':
                            wijmo.grid.xlsx.FlexGridXlsxConverter.saveAsync(grid, null, downloadFileName + '.xlsx').catch(err => console.error('Error exporting to Excel:', err));
                            break;
                        case 'X_PDF':
                            wijmo.grid.pdf.FlexGridPdfConverter.export(grid, downloadFileName + '.pdf', {
                                maxPages: 10,
                                scaleMode: wijmo.grid.pdf.ScaleMode.PageWidth,
                                documentOptions: {
                                    compress: true,
                                    header: { declarative: { text: '\t&[Page] of &[Pages]' } },
                                    footer: { declarative: { text: '\t&[Page] of &[Pages]' } },
                                    info: { author: 'MESCIUS', title: downloadFileName }
                                },
                                embeddedFonts: [
                                    {
                                        source: '/font/NotoSansKR-Regular.ttf',  // 폰트 파일 경로
                                        name: 'NotoSansKR',
                                        style: 'normal',
                                        weight: 'normal',
                                        sansSerif: true
                                    }
                                ],
                                styles: {
                                    cellStyle: {
                                        backgroundColor: '#ffffff',
                                        borderColor: '#c6c6c6',
                                        font: { family: 'NotoSansKR' }  // 스타일에서 폰트 지정
                                    },
                                    altCellStyle: { backgroundColor: '#f9f9f9' },
                                    groupCellStyle: { backgroundColor: '#dddddd' },
                                    headerCellStyle: { backgroundColor: '#eaeaea' }
                                }
                            });
                            break;
                    }
                    // restore focus to active grid cell
                    grid.refresh();
                    let sel = grid.selection, cell = grid.cells.getCellElement(sel.row, sel.col);
                    if (cell) {
                        cell.focus();
                    }
                }
            }
        });
        // done
        return menu;
    }
}
