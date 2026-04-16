window.selectedGridId = null;
window.savedScrollPosition = null;

let GridUtil = {
    changeOrder: function (val, grid) {
        if (!grid || !grid.selectedItems || grid.selectedItems.length === 0) {
            Alert.alert('', '순서를 변경할 Row 를 선택하세요.');
            return;
        }

        let collectionView = grid.collectionView;
        if (!collectionView || !collectionView.items) {
            console.error("CollectionView가 정의되지 않았습니다.");
            return;
        }

        if (val === "U") {
            let items = grid.selectedItems;
            items.forEach((item) => {
                let self_index = collectionView.items.indexOf(item);
                let upper_index = self_index - 1;
                if (upper_index < 0) {
                    Alert.alert('', "첫번째 Row가 선택되었습니다.");
                    return;
                }

                // ✅ 배열에서 항목 위치 변경
                [collectionView.items[self_index], collectionView.items[upper_index]] =
                    [collectionView.items[upper_index], collectionView.items[self_index]];

                collectionView.refresh();
                grid.select(new wijmo.grid.CellRange(upper_index, 0)); // ✅ 수정
            });
        } else {
            let items = grid.selectedItems;
            items.reverse(); // 아래로 내릴 때는 역순으로 루프를 돌려야 한다.
            items.forEach((item) => {
                let self_index = collectionView.items.indexOf(item);
                let under_index = self_index + 1;
                if (under_index >= collectionView.items.length) {
                    Alert.alert('', "마지막 Row가 선택되었습니다.");
                    return;
                }

                // ✅ 배열에서 항목 위치 변경
                [collectionView.items[self_index], collectionView.items[under_index]] =
                    [collectionView.items[under_index], collectionView.items[self_index]];

                collectionView.refresh();
                grid.select(new wijmo.grid.CellRange(under_index, 0)); // ✅ 수정
            });
        }
    },

    adjustHeight: function (grid, rows_len) {
        if (!grid || !grid.hostElement) {
            console.error("Grid가 정의되지 않았습니다.");
            return;
        }
        let rowHeight = grid.rows.defaultSize;
        let headerHeight = grid.columnHeaders.rows.defaultSize;
        let height = headerHeight + (rowHeight * (rows_len + 2));
        if (height < 150) height = 150;
        grid.hostElement.style.height = height + 'px';
    },

    getSelectedId: function (grid, idField = 'id') {
        const it = grid && grid.selectedItems && grid.selectedItems[0];
        return it ? it[idField] : null;
    },

    // === 추가: id로 행 다시 선택(바인딩 타이밍 대응) ===
    // options: { idField='id', focus=true, scroll=true, lastColumnSelect=true }
    selectRowById: function (grid, id, options = {}) {
        const opts = Object.assign({
            idField: 'id',
            focus: true,
            scroll: true,
            lastColumnSelect: true
        }, options);

        if (!grid || id == null || id === '') return;

        const cv = grid.collectionView;

        // 즉시 시도 후 실패하면 collectionChanged 한 번 대기
        if (!this._trySelectRowById(grid, id, opts)) {
            if (cv && cv.collectionChanged) {
                const handler = () => {
                    // 데이터가 바뀐 뒤에 한 번 더 시도
                    setTimeout(() => {
                        if (this._trySelectRowById(grid, id, opts)) {
                            cv.collectionChanged.removeHandler(handler);
                        }
                    }, 0);
                };
                cv.collectionChanged.addHandler(handler);
            } else {
                // collectionView가 없거나 rows가 비어있을 때의 fallback
                setTimeout(() => this._trySelectRowById(grid, id, opts), 0);
            }
        }
    },

    // === 내부: 실제 선택 로직(그룹행 건너뜀) ===
    _trySelectRowById: function (grid, id, opts) {
        if (!grid || !grid.rows || grid.rows.length === 0) return false;

        const lastCol = opts.lastColumnSelect ? Math.max(0, grid.columns.length - 1) : 0;

        for (let r = 0; r < grid.rows.length; r++) {
            const row = grid.rows[r];
            if (row instanceof wijmo.grid.GroupRow) continue;

            const item = row.dataItem;
            if (item && item[opts.idField] == id) {
                // 현재 스크롤 위치 저장
                const prevScroll = grid.scrollPosition;

                // 행 선택 (이때 FlexGrid가 자동 스크롤)
                grid.select(new wijmo.grid.CellRange(r, 0, r, lastCol), true);
                if (opts.focus) grid.focus();

                // scroll 옵션이 false면 이전 스크롤 위치로 복원
                if (opts.scroll === false) {
                    setTimeout(() => {
                        grid.scrollPosition = prevScroll;
                    }, 0);
                }
                return true;
            }
        }
        return false;
    },
    saveView: function (grid, itemOrId, options = {}) {
        const idField = options.idField || 'id';
        let id = null;

        if (itemOrId && typeof itemOrId === 'object') {
            id = itemOrId[idField];
        } else {
            id = itemOrId;
        }

        if (id != null && id !== '') {
            window.selectedGridId = id;
        }
        if (grid && grid.scrollPosition) {
            // 객체 복사(참조깨짐 방지)
            window.savedScrollPosition = new wijmo.Point(grid.scrollPosition.x, grid.scrollPosition.y);
        }
    },
    restoreView: function (grid, options = {}) {
        const idField = options.idField || 'id';
        const clear = options.clear !== false; // 기본 true

        const id = window.selectedGridId;
        const scroll = window.savedScrollPosition;

        // 1) 선택 복원 (스크롤은 건드리지 않음)
        if (id) {
            // selectRowById 내부에서 자동 스크롤 안 하도록 scroll:false
            this.selectRowById(grid, id, { idField, scroll: false });
        }

        // 2) 스크롤 복원 (그리드 내부 자동 스크롤 이후 한 프레임 뒤)
        if (scroll) {
            (window.requestAnimationFrame
                    ? requestAnimationFrame(() => { grid.scrollPosition = scroll; })
                    : setTimeout(() => { grid.scrollPosition = scroll; }, 0)
            );
        }

        if (clear) {
            window.selectedGridId = null;
            window.savedScrollPosition = null;
        }
    }

};
