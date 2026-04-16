function submitTextarea(event, func){
    let key = event.key || event.keyCode;

    if(key === 'Enter' || key == 13){
        func();
    }
}

//오늘낧짜
function getNowDate(){
    const today = new Date();

    const formattedDate = (date) => {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`
    }
    return formattedDate(today);
}
// 7일전 날짜
function getNowDateMinus7(){

    const day = new Date();

    const sevenDaysAgo = new Date();

    sevenDaysAgo.setDate(day.getDate() - 7);

    const formattedDate = (date) => {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0')
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    };
    return formattedDate(sevenDaysAgo);
}

//현재 시간, HH:mm
function getNowTime(){
    const now = new Date();
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    return `${hours}:${minutes}`
}

function getFirstDayOfThreeMonthsAgo(){
    const today = new Date();

    const threeMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 3, 1);

    const formattedDate = (date) => {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    };
    return formattedDate(threeMonthsAgo);
}

// 3달이상이면 false
function calculateDay(fd, td){



    const frdate = new Date(fd);
    const todate = new Date(td);

    const diffInMs = Math.abs(frdate - todate);
    const diffDays = diffInMs / (1000 * 60 * 60 * 24);

    return diffDays <= 90;
}

// 빈 값 항목 체크해줌
function validationCheck(FormData, checklist){

    for(let key in checklist){
        const value = FormData[key];



        if(value === null || value === ''){
            const text = checklist[key];

            Alert.alert('', `${text} 항목이 빈 값입니다.`);
            return false;
        }
    }
    return true
}

// api 호출후 응답메세지 반환
function ApiSuccessMessage(res){
    if(res === null || res === undefined){
        Alert.alert('', '에러가 발생하였습니다.');
    }
    Alert.alert('', res.message);
}

//셀렉트 박스 바인딩 해줌
function selectBoxBinding(id, arr){

    const selectedBox = document.getElementById(id);
    selectedBox.innerHTML = '';

    arr.forEach(item => {
        const option = document.createElement('option');
        option.value = item.value;
        option.textContent = item.text;
        selectedBox.appendChild(option);
    })
}

//입출금 구분에 따라 거래구분을 필터링 후 콤보박스에 바인딩
//inout : 입금(0)이냐 출금(1)이냐
//arr : 서버에서 조회환 항목들
//id : 바인딩하려는 콤보박스 아이디
function filterAndBindTradeTypes(inout, arr, id){
    const filterItems = arr.filter(item => item.ioflag === inout);

    const selectedBox = document.getElementById(id);
    selectedBox.innerHTML = '';

    filterItems.forEach(item => {
        const option = document.createElement('option');
        option.value = item.value;
        option.textContent = item.text;
        selectedBox.appendChild(option);
    })
}

function getSpjangcdFromSession(){
    return sessionStorage.getItem('spjangcd');
}

//거래처 팝업 오픈해주는 함수
function companyPopupOpen(intputId, hiddenid){

    let poppage = new PopCompComponent();
    let $poppage = $(poppage);
    let searchcallback = function (items) {
        // $content.find('#cboCompany').val(items.id);
        // $content.find('#CompanyName').val(items.compname);

        document.getElementById(intputId).value = items.compname;
        document.getElementById(hiddenid).value = items.id;
    };

    poppage.show(searchcallback);
}

function ProjectPopupOpen(intputId, hiddenid){
    const value = document.getElementById(intputId).value;

    let poppage = new PopProjectComponent(value);

    let result = AjaxUtil.getSyncData('/api/popup/search_project', { srchName : value, spjangcd : getSpjangcdFromSession() });

    let searchcallback = function (items) {


        document.getElementById(intputId).value = items.projnm;
        document.getElementById(hiddenid).value = items.projno;

        document.getElementById(intputId).focus();
    };
    if(result && result.data.length === 1){
        searchcallback(result.data[0]);
        return;
    }

    poppage.show(searchcallback);
}



//숫자 금액으로 변환 , 숫자 아니면 0으로 리턴
function formatMoney(val){
    const num = Number(val);
    if(isNaN(num)) return '0';
    return num.toLocaleString('en-US')
}

//input필드 중 숫자만 입력되게
function formatToCommaNumberInput(e) {
    let val = e.target.value;
    val = val.replace(/[^0-9]/g, '');                         // 숫자만 남기기
    val = val.replace(/\B(?=(\d{3})+(?!\d))/g, ",");          // 천단위 콤마
    e.target.value = val;
}

class Trie{
    constructor() {
        this.wordList = [];
    }

    insert(word){

        this.wordList.push(word);
    }

    searchPrefix(prefix) {
        return this.wordList.filter(word => word.includes(prefix)); // 부분 일치 검색
    }

}




function createSuggestionBox(searchInput) {
    let box = document.getElementById("autocomplete-list");
    if (!box) {
        box = document.createElement("div");
        box.id = "autocomplete-list";
        box.style.position = "absolute";
        box.style.background = "white";
        box.style.border = "1px solid #ccc";
        box.style.zIndex = "1000";
        box.style.maxHeight = "200px";
        box.style.overflowY = "auto";
        box.style.display = "none";
        document.body.appendChild(box);
    }
    box.style.width = searchInput.offsetWidth + "px";
    return box;
}

function attachInputListener(searchInput, suggestionBox, trie, dataArrList) {
    let currentIndex = -1;

    searchInput.addEventListener("input", () => {
        const query = searchInput.value.trim();
        suggestionBox.innerHTML = "";
        currentIndex = -1;

        if (!query) return suggestionBox.style.display = "none";

        const matches = trie.searchPrefix(query);
        if (matches.length === 0) return suggestionBox.style.display = "none";

        matches.forEach(name => {
            const item = document.createElement("div");
            item.textContent = name;
            item.classList.add("autocomplete-item");
            Object.assign(item.style, {
                padding: "10px",
                cursor: "pointer",
                borderBottom: "1px solid #eee",
                fontSize: "14px"
            });

            item.addEventListener("click", () => {
                searchInput.value = name;
                suggestionBox.style.display = "none";
            });

            suggestionBox.appendChild(item);
        });

        const rect = searchInput.getBoundingClientRect();
        suggestionBox.style.top = `${window.scrollY + rect.bottom}px`;
        suggestionBox.style.left = `${window.scrollX + rect.left}px`;
        suggestionBox.style.width = `${rect.width}px`;
        suggestionBox.style.display = "block";
    });

    searchInput.addEventListener("keydown", (e) => {
        const items = suggestionBox.getElementsByClassName("autocomplete-item");
        if (e.key === "ArrowDown") {
            e.preventDefault();
            if (currentIndex < items.length - 1) currentIndex++;
            updateSelection(items, currentIndex);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            if (currentIndex > 0) currentIndex--;
            updateSelection(items, currentIndex);
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (currentIndex >= 0 && currentIndex < items.length) {
                searchInput.value = items[currentIndex].textContent;
                suggestionBox.style.display = "none";

                const inputValue = searchInput.value.trim();

                const matchedItem = dataArrList.find(obj => {
                    const key = Object.keys(obj)[0];
                    return key.trim() === inputValue;
                });

                if(matchedItem) {
                    const url = matchedItem[inputValue];
                    const target = url.substring(url.lastIndexOf("/") + 1);
                    document.querySelector(`[data-objid="${target}"]`)?.click();
                }


            }
        }
    });
}

function updateSelection(items, currentIndex) {
    Array.from(items).forEach(item => item.style.background = "white");
    if (currentIndex >= 0 && currentIndex < items.length) {
        const selectedItem = items[currentIndex];
        selectedItem.style.background = "dodgerblue";
        selectedItem.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
}

function bindClickOutside(searchBox, suggestionBox) {
    document.addEventListener("click", (event) => {
        if (!searchBox.contains(event.target)) {
            suggestionBox.style.display = "none";
        }
    });
}

function initializeAutocomplete(inputId, divId, dataList, dataArrList) {
    const searchInput = document.getElementById(inputId);
    if (!searchInput || searchInput.dataset.autocompleteInitialized === "true") return;

    const searchBox = searchInput.closest("#" + divId);
    if (!searchBox) return;

    const suggestionBox = createSuggestionBox(searchInput);
    const trie = new Trie();
    dataList.forEach(name => trie.insert(name));

    attachInputListener(searchInput, suggestionBox, trie, dataArrList);
    bindClickOutside(searchBox, suggestionBox);

    searchInput.dataset.autocompleteInitialized = "true";
}

