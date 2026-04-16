class WijmoFlexGrid {

    constructor(grid) {

        this.gridEle = grid;

    }

    addNewRow(data, callback){

        // let cv = this.approverGrid.collectionView;
        // console.log('data', addData)
        // let newRow = {
        //     ShiftName: addData['ShiftName'],
        //     DepartName: addData['DepartName'],
        //     Name: addData['Name'],
        //     Shift: addData['Shift'],
        //     Depart_id: addData['Depart_id'],
        //     User_id: addData['User_id']
        // };
        // cv.addNew(newRow);
        // cv.commitNew();
        //
        // // 그리드의 마지막 행 인덱스 가져오기
        // let newRowIndex = cv.items.length - 1;
        //
        // // 그리드 업데이트 후 행 선택
        // setTimeout(() => {
        //     this.approverGrid.select(newRowIndex, 0); // 마지막 행 선택
        //     this.approverGrid.rows[newRowIndex].isSelected = true; // 해당 행을 선택 상태로 설정
        // }, 0);

        let cv = this.gridEle.collectionView;
        cv.addNew(data);
        cv.commitNew();

        if(typeof callback === 'function')
        {
            callback();
        }

    }
    //바로 삭제
    deleteSelectedRow(){
        let cv = this.gridEle.collectionView;
        console.log(this.gridEle);

        let rowsToDelete = this.gridEle.rows.filter(row => row.isSelected);

        if(rowsToDelete.length > 0){
            rowsToDelete.forEach(row => {
                cv.remove(row.dataItem);
            });
            //cv.refresh();
        }
    }

    //근데 이거 순수한 리스트만 얻으려면
    /**
     * _this.grid.collectionView.sourceCollection.map(item => ({ ...item }));
     *
     * **/
    getGridList(){

        return this.gridEle.collectionView.items;
    }

    getSelected(){
        return this.gridEle.selectedItems;
    }

    grid_reset_clear(){
        console.log('호출됨');

        this.gridEle.itemsSource = new wijmo.collections.CollectionView([]);

    }

    grid_binding(data){

        this.gridEle.itemsSource = data;
    }

    restoreGrid_rollback(){

    }
}
function createFlexGrid(gridId, options = {}){
    let flexGrid = new wijmo.grid.FlexGrid(gridId, {
        autoGenerateColumns: options.autoGenerateColumns || false,
        showMarquee: options.showMarquee || true,
        allowSorting: options.allowSorting || 'MultiColumn',
        selectionMode: options.selectionMode || wijmo.grid.SelectionMode.Row,
        isReadOnly: options.isReadOnly || false,
        columns: options.columns || [],
        itemsSource: options.itemsSource || [],
        formatItem: (s, e) => {
            if (e.panel.cellType === wijmo.grid.CellType.ColumnHeader) {
                e.cell.style.textAlign = 'center'; // 헤더 텍스트 가운데 정렬
            }
        },
    });

    return flexGrid;
}