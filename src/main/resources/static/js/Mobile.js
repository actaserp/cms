
// 현재 년도를 가져옴
function getCurrentYear() {
    return new Date().getFullYear();
}

// 날짜 초기화 함수
function setDefaultDates(startDateId, endDateId) {

    // 순회점검 현황에서 더블클릭해서 새창으로 띄운다음 그 날짜 설정하기 위해서
    let checkdtElement = $('#checkdtParam');
    let checkdt = checkdtElement.length > 0 && checkdtElement.val() ? checkdtElement.val().trim() : null;

    if (checkdt) {
        document.getElementById("startDate").value = checkdt;
        document.getElementById("endDate").value = checkdt;
    } else {
        const currentYear = getCurrentYear();
        document.getElementById("startDate").value = `${currentYear}-01-01`;
        document.getElementById("endDate").value = `${currentYear}-12-31`;
    }

}

// 날짜 초기화 함수
function setDefaultMonths(startDateId, endDateId) {


    const currentYear = getCurrentYear();
    document.getElementById(startDateId).value = `${currentYear}-01`;
    document.getElementById(endDateId).value = `${currentYear}-12`;


}

// 날짜를 두 자릿수로 포맷하는 함수
function padToTwoDigits(num) {
    return num.toString().padStart(2, '0');
}

// 날짜 초기화 함수
function setDefaultDateToday(startDateId, endDateId) {
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth(); // 0-based index (0 = January)

    // 현재 달의 시작일 (예: 2024-10-01)
    const startOfMonth = new Date(currentYear, currentMonth, 1);
    const formattedStartDate = `${startOfMonth.getFullYear()}-${padToTwoDigits(startOfMonth.getMonth() + 1)}-${padToTwoDigits(startOfMonth.getDate())}`;

    const formattedToday = `${today.getFullYear()}-${padToTwoDigits(today.getMonth() + 1)}-${padToTwoDigits(today.getDate())}`;

    // 날짜 설정
    document.getElementById(startDateId).value = formattedStartDate;
    document.getElementById(endDateId).value = formattedToday;
}


// 파일 다운로드
function downloadFiles(downloadList, url) {
    let xhr = new XMLHttpRequest();
    xhr.open('POST', url, true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.setRequestHeader('X-CSRF-Token', document.querySelector('[name=_csrf]').value);
    xhr.responseType = 'blob';

    xhr.onload = function () {
        if (xhr.status === 200) {
            let blob = xhr.response;
            let link = document.createElement('a');
            let downloadUrl = window.URL.createObjectURL(blob);
            let contentDisposition = xhr.getResponseHeader('Content-Disposition');
            let fileName = 'downloadedFile';

            if (contentDisposition) {
                let fileNameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
                let matches = fileNameRegex.exec(contentDisposition);
                if (matches != null && matches[1]) {
                    fileName = decodeURIComponent(matches[1].replace(/['"]/g, ''));
                }

                // filename* 속성에서 파일 이름 추출
                let fileNameStarRegex = /filename\*\=([^;]*)/;
                let matchesStar = fileNameStarRegex.exec(contentDisposition);
                if (matchesStar != null && matchesStar[1]) {
                    fileName = decodeURIComponent(matchesStar[1].split("''")[1].replace(/['"]/g, ''));
                }
            }

            link.href = downloadUrl;
            link.download = fileName;
            link.style.display = 'none';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
        } else if (xhr.status === 404) {
            Alert.alert('', '파일이 존재하지 않습니다.');
        } else {
            Alert.alert('', '파일 다운로드 중 오류가 발생했습니다.');
        }
    };

    xhr.onerror = function () {
        console.error('Network error');
    };

    xhr.send(JSON.stringify(downloadList));
}

// 파일 리스트 UI 업데이트 함수
function updateFileListUI() {
    const $fileList = $('#filelist');

    uploadedFiles.forEach(file => {
        const fileSize = (file.size / 1024).toFixed(2) + ' KiB';
        const li = $('<li>').html(`
                    <p>${file.name} <span>(${fileSize})</span></p>
                    <a href="#" title="삭제" class="btn-file-delete">
                        <img src="${withCtx('/images/icon/ico-filedelete.svg')}" alt="삭제아이콘">
                    </a>
                `);
        $fileList.append(li);
    });
}

// 파일 리스트 UI 업데이트 함수
function updateFileListUI2() {
    const $fileList = $('#filelist2');

    uploadedFiles2.forEach(file => {
        const fileSize = (file.size / 1024).toFixed(2) + ' KiB';
        const li = $('<li>').html(`
                    <p>${file.name} <span>(${fileSize})</span></p>
                    <a href="#" title="삭제" class="btn-file-delete2">
                        <img src="/images/icon/ico-filedelete.svg" alt="삭제아이콘">
                    </a>
                `);
        $fileList.append(li);
    });
}

// 날짜 형식을 변환하는 함수
function formatDate(dateString) {
    if (dateString && dateString.length === 8) {
        return dateString.substring(0, 4) + '-' + dateString.substring(4, 6) + '-' + dateString.substring(6, 8);
    }
    return dateString;
}

function formatDate2(timestamp) {
    if (!timestamp) return ""; // timestamp 값이 없을 경우 빈 문자열 반환

    // JavaScript Date 객체를 사용하여 변환
    let date = new Date(timestamp); // timestamp를 Date 객체로 변환

    // 날짜를 yyyy-MM-dd 형식으로 변환
    let year = date.getFullYear();
    let month = ('0' + (date.getMonth() + 1)).slice(-2); // 월은 0부터 시작하므로 +1, 두 자리로 맞춤
    let day = ('0' + date.getDate()).slice(-2); // 두 자리로 맞춤

    // 시간을 HH:mm 형식으로 변환
    let hours = ('0' + date.getHours()).slice(-2);
    let minutes = ('0' + date.getMinutes()).slice(-2);

    // datetime-local 형식으로 반환 (yyyy-MM-ddTHH:mm)
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}


// 파일 개수 업데이트 함수
function updateFileCount() {
    const fileCount = uploadedFiles.length;
    $('.upload-filelist .title h5').text(`Files (${fileCount})`);
    $('.btn-file span').text(`(${fileCount})`);
}

// 파일 개수 업데이트 함수
function updateFileCount2() {
    const fileCount = uploadedFiles2.length;
    $('.upload-filelist2 .title h5').text(`Files (${fileCount})`);
    $('.btn-file2 span').text(`(${fileCount})`);
}

// 파일 인풋 초기화
function resetFileInput($input) {
    $input.val('');
}

// 자동완성
let debounceTimeout;
let selectedSuggestionIndex = -1;
let suggestionsVisible = false;

function fetchSuggestions(inputId, apiUrl, suggestionId) {
    clearTimeout(debounceTimeout);
    debounceTimeout = setTimeout(() => {
        const inputField = document.getElementById(inputId);
        let suggestionsList = document.getElementById(suggestionId);

        if (!inputField || !suggestionsList) return;

        const inputClearDiv = inputField.parentElement;
        inputClearDiv.appendChild(suggestionsList);

        const query = inputField.value;
        if (query.length > 0) {
            fetch(`${apiUrl}?query=${encodeURIComponent(query)}&field=${encodeURIComponent(inputId)}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    const previousSelectedText = (selectedSuggestionIndex >= 0 && selectedSuggestionIndex < data.length) ? data[selectedSuggestionIndex] : null;
                    suggestionsList.innerHTML = '';
                    if (data.length > 0) {
                        suggestionsVisible = true;
                        suggestionsList.style.display = 'block'; // 결과가 있을 때만 표시
                        data.forEach((item, index) => {
                            let li = document.createElement('li');
                            li.textContent = item;
                            li.setAttribute('data-index', index);
                            li.onclick = function () {
                                document.getElementById(inputId).value = this.textContent;
                                suggestionsList.innerHTML = '';
                                suggestionsList.style.display = 'none';
                                suggestionsVisible = false;
                            };
                            suggestionsList.appendChild(li);
                            // 이전에 선택한 항목을 다시 선택 상태로 설정
                            if (item === previousSelectedText) {
                                selectedSuggestionIndex = index;
                                li.classList.add('selected');
                            }
                        });
                    } else {
                        suggestionsList.style.display = 'none'; // 결과가 없으면 숨김
                        suggestionsVisible = false;
                        selectedSuggestionIndex = -1;
                    }
                })
                .catch(error => {
                    console.error('Fetch error:', error);
                    suggestionsList.style.display = 'none'; // 오류 시 숨김
                    suggestionsVisible = false;
                    selectedSuggestionIndex = -1;
                });
        } else {
            suggestionsList.innerHTML = '';
            suggestionsList.style.display = 'none';
            suggestionsVisible = false;
            selectedSuggestionIndex = -1;
        }
    }, 300);  // 300ms의 디바운스 타임 적용
}

// 자동완성 기능
function initializeAutoComplete(inputId, apiUrl, suggestionId) {
    const inputField = document.getElementById(inputId);

    let preventFetchOnEnter = false;  // Enter 키로 선택했을 때 검색을 방지하기 위한 플래그

    inputField.addEventListener('keyup', (event) => {
        if (event.key !== 'Enter' && event.key !== 'ArrowDown' && event.key !== 'ArrowUp' && !preventFetchOnEnter) {
            fetchSuggestions(inputId, apiUrl, suggestionId);
        }
        // Enter 키가 눌렸을 경우에는 검색을 방지
        preventFetchOnEnter = false;  // 플래그 초기화
    });

    inputField.addEventListener('keydown', function (event) {
        const suggestionsList = document.getElementById(suggestionId);
        const items = suggestionsList.getElementsByTagName('li');

        if (suggestionsVisible) {
            if (event.key === 'ArrowDown') {
                // 아래 화살표 키를 누르면
                event.preventDefault(); // 기본 동작 방지
                selectedSuggestionIndex = (selectedSuggestionIndex + 1) % items.length;
                updateSuggestionSelection(items);
            } else if (event.key === 'ArrowUp') {
                // 위 화살표 키를 누르면
                event.preventDefault(); // 기본 동작 방지
                selectedSuggestionIndex = (selectedSuggestionIndex - 1 + items.length) % items.length;
                updateSuggestionSelection(items);
            } else if (event.key === 'Enter') {
                // Enter 키를 누르면
                if (selectedSuggestionIndex >= 0) {
                    event.preventDefault(); // 기본 동작 방지
                    items[selectedSuggestionIndex].click();
                    preventFetchOnEnter = true;  // 엔터로 선택한 후 검색 방지
                    suggestionsVisible = false; // Enter 키를 누르면 선택을 확정하고 목록을 숨김
                    // inputField.blur(); // 입력 필드 포커스 해제
                }
            } else if (event.key === 'Escape') {
                // Escape 키를 누르면
                suggestionsList.style.display = 'none';
                suggestionsVisible = false;
            }
        }
    });

    // 포커스 해제 시 자동 완성 목록 숨기기
    inputField.addEventListener('blur', function () {
        setTimeout(function () {
            const suggestionsList = document.getElementById(suggestionId);
            suggestionsList.style.display = 'none';
            suggestionsVisible = false;
        }, 300); // 300ms 지연 시간
    });

    // 마우스 클릭으로 선택 시에도 검색 방지
    document.getElementById(suggestionId).addEventListener('click', function (event) {
        if (event.target.tagName === 'LI') {
            preventFetchOnEnter = true;
        }
    });
}

function updateSuggestionSelection(items) {
    // 모든 항목의 선택 상태 초기화
    for (let i = 0; i < items.length; i++) {
        items[i].classList.remove('selected');
    }

    // 현재 선택된 항목에 선택 상태 추가
    if (selectedSuggestionIndex >= 0 && selectedSuggestionIndex < items.length) {
        items[selectedSuggestionIndex].classList.add('selected');
        items[selectedSuggestionIndex].scrollIntoView({block: 'nearest'});
    }
}

// 공통코드 리스트 가져오기
function fetchCommonCodes(parentId, selectElementId, selectedValue) {
    return fetch(`/api/common/find_parent_id?id=${parentId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok ' + response.statusText);
            }
            return response.json();
        })
        .then(data => {
            const selectElement = document.getElementById(selectElementId);

            // "전체" 옵션이 있는지 확인
            const allOption = selectElement.querySelector('option[value=""]:not([disabled]):not([hidden])');

            // "선택" 옵션이 있는지 확인
            const defaultOption = selectElement.querySelector('option[value=""][disabled][hidden]');

            // 기존 옵션 제거 (특정 옵션 제외)
            selectElement.innerHTML = ''; // 모든 옵션 제거

            // "전체" 옵션이 있으면 다시 추가 (선택 가능하게 설정)
            if (allOption) {
                allOption.selected = true; // 전체를 기본 선택으로 유지
                selectElement.appendChild(allOption);
            }

            // "선택" 옵션이 있으면 다시 추가 (선택 불가능하게 설정)
            if (defaultOption) {
                selectElement.appendChild(defaultOption);
            }

            // 데이터에서 value가 '2'인 항목은 tketflag_li일 때만 제외하고 추가
            data.forEach(item => {
                // tketflag_li일 때만 value가 '2'인 항목 제외
                const option = document.createElement('option');
                option.value = item.Code;
                option.textContent = item.Value; // item.value를 데이터베이스의 "Value" 필드로 변경

                // Code가 'fs'인 경우 옵션을 숨김
                if (selectElementId === 'tkettypecd' && item.Code === 'fs') {
                    option.hidden = true;
                }

                if (item.Code === selectedValue) {
                    option.selected = true;
                }
                selectElement.appendChild(option);
            });
        })
        .catch(error => {
            console.error('Error fetching options:', error);
        });
}


function fetchCommonCodes2(parentId, inputElementId, id) {
    fetch(`/api/common/find_parent_id2?id=${parentId}&codeId=${id}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok ' + response.statusText);
            }
            return response.json(); // JSON으로 응답 받기
        })
        .then(data => {
            const inputElement = document.getElementById(inputElementId);
            if (data && data.length > 0) {
                inputElement.value = data[0].Value;
            } else {
                inputElement.value = ''; // 값이 없을 경우 입력란 비우기
            }
        })
        .catch(error => {
            console.error('Error fetching options:', error);
        });
}


// 로딩바를 보여주는 함수
function showLoader() {
    document.getElementById('loader2').style.display = 'block';
}

// 로딩바를 숨기는 함수
function hideLoader() {
    document.getElementById('loader2').style.display = 'none';
}

// 파일업로드 변수설정
let uploadedFiles2 = [];
// 삭제할 파일 목록
let deletedFiles2 = [];
$(document).ready(function () {
    function initializeUploadComponent2(component) {

        const $fileInput = $(component).find('.fileInput2');
        const $fileList = $(component).find('.filelist2');
        const $fileCountTitle = $(component).find('.upload-filelist2 .title h5');

        $fileInput.on('change', function (event) {
            const files = event.target.files;
            handleFileSelect2(files);
        });

        $(component).find('.upload-filebox2').on('dragover', function (event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).addClass('dragging');
        });

        $(component).find('.upload-filebox2').on('dragleave', function (event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).removeClass('dragging');
        });

        $(component).find('.upload-filebox2').on('drop', function (event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).removeClass('dragging');
            // handleFileSelect(event.originalEvent.dataTransfer.files);

            const files = event.originalEvent.dataTransfer.files;

            handleFileSelect2(files);
            // 드래그 앤 드롭 후 파일 입력 요소 초기화
            resetFileInput($fileInput);
        });

        function handleFileSelect2(files) {
            $.each(files, function (index, file) {
                if (file.size > MAX_FILE_SIZE) {
                    Alert.alert('', '파일 크기는 1GB를 초과할 수 없습니다.');
                    resetFileInput($fileInput);
                } else if (!uploadedFiles2.some(f => f.name === file.name && f.size === file.size)) {
                    uploadedFiles2.push(file);
                    const fileSize = (file.size / 1024).toFixed(2) + ' KiB';
                    const li = $('<li>').html(`
                    <p>${file.name} <span>(${fileSize})</span></p>
                    <a href="#" title="삭제" class="btn-file-delete2">
                        <img src="/images/icon/ico-filedelete.svg" alt="삭제아이콘">
                    </a>
                `);
                    $fileList.append(li);
                }
            });
            updateFileCount2();
        }

        $(component).on('click', '.btn-file-delete2', function (event) {
            event.preventDefault();
            const li = $(this).closest('li');
            const fileName = li.find('p').text().split(' (')[0];

            // 파일 이름과 일치하지 않는 파일들로 필터링하여 uploadedFiles 업데이트
            const removedFile = uploadedFiles2.find(file => file.name === fileName);
            uploadedFiles2 = uploadedFiles2.filter(file => file.name !== fileName);

            // 삭제된 파일을 deletedFiles에 추가
            if (removedFile) {
                deletedFiles2.push(removedFile);
            }
            li.remove();
            updateFileCount2();

            // 파일 선택 후 파일 입력 요소 초기화
            resetFileInput2($fileInput);
        });

        $(component).find('.btn-file-deleteall2').on('click', function (event) {
            event.preventDefault();
            // 모든 업로드된 파일을 삭제된 파일 리스트에 추가
            deletedFiles2 = deletedFiles2.concat(uploadedFiles2);

            $fileList.empty();
            uploadedFiles2 = [];
            updateFileCount2();
            resetFileInput2($fileInput);
        });

        function updateFileCount2() {
            const fileCount = uploadedFiles2.length;
            $fileCountTitle.text(`Files (${fileCount})`);
            $('.btn-file2 span').text(`(${fileCount})`);
        }

        function resetFileInput2($input) {
            $input.val('');
        }
    }

    $('.upload-component2').each(function () {
        initializeUploadComponent2(this);
    });
});

function closePopup(popupId) {
    // 해당 ID의 팝업 요소를 찾음
    const popup = document.getElementById(popupId);

    if (popup) {
        // 팝업을 포함한 모달 숨기기
        const modal = popup.closest('.modal');
        if (modal) {
            modal.style.display = 'none';
            modal.classList.remove('show');
        }

        // 팝업 숨기기
        popup.style.display = 'none';
        popup.classList.remove('show');
    }
}

function showPopup(element) {
    // 클릭된 요소의 data-popup 속성 값 가져오기
    const popupId = element.getAttribute('data-popup');

    // 해당 ID의 팝업 요소를 찾음
    const popup = document.getElementById(popupId);

    if (popup) {
        // 팝업을 포함한 모달을 표시
        const modal = popup.closest('.modal');
        if (modal) {
            modal.style.display = 'block';
        }

        // 팝업을 표시
        popup.style.display = 'block';

        // 애니메이션을 위한 클래스 추가
        requestAnimationFrame(() => {
            popup.classList.add('show');
            if (modal) {
                modal.classList.add('show');
            }
        });
    }
}


// 모달로 그래프 확장
$(document).ready(function () {

    // 모든 chart-wrap 요소에 투명한 오버레이를 자동으로 추가
    $('.chart-wrap').each(function () {
        // expand-icon 아이콘 생성
        let expandIcon = $('<span class="material-symbols-outlined">\n' +
            'zoom_out_map\n' +
            '</span>');

        // expand-icon 스타일 적용
        expandIcon.css({
            'position': 'absolute',
            'top': '17px', // 차트 오른쪽 끝 위에서 10px 아래
            'right': '3px', // 차트의 오른쪽 끝에 위치
            'cursor': 'pointer',
            'z-index': '10',
            'color': '##B3B3B3'
        });

        // chart-wrap에 expand-icon 추가
        $(this).css('position', 'relative').append(expandIcon);
    });

});



//알림 서비스
function notifications_rtn(formData) {
    var app_id = "483af854-d7ff-45a8-9ce2-c6ebd87c31e5";
    var restapi_key = "NDZlMDRjMTItYWFiNS00Njk5LWE0YjAtMjAyZDlkZjEyYWYw"; //고정값 '\n'
    var push_title = formData.get("tketnm");
    var push_content = formData.get("tketrusernm");
    //개인 push id

    //push 받을 개인 배열
    var player_id_array_as_array = [formData.get("tkconuserid")];

    $.ajax({
        url: "https://api.onesignal.com/notifications?c=push",
        type: "POST",
        headers: {
            Authorization: restapi_key,
            'content-type': 'application/json'
        },
        dataType: "json",
        data: JSON.stringify({
            name: ' push event ',
            app_id: app_id,
            include_external_user_ids: player_id_array_as_array,
            headings: {en: push_title},
            contents: {en: push_content + "님이 티켓을 등록"},
            isAnyWeb: true

        }),
        success: function (data) {
            console.log("두 번째 AJAX 호출 성공", data);
        },
        error: function (e) {
            // console.log("두 번째 AJAX 호출 실패", e);
        }
    });
}

function openPopup(url) {
    // 새 창 열기
    window.open(url, '_blank',
        'width=1000,height=1000,toolbar=no,location=no,status=no,menubar=no,scrollbars=yes,resizable=yes');
}

let csrfToken;
document.addEventListener("DOMContentLoaded", function () {
    // LocalStorage에서 username과 groupname 가져오기
    const username = localStorage.getItem('username'); // 기본값: 'Guest'
    const groupname = localStorage.getItem('groupname'); // 기본값: 'Default Group'

    // HTML 요소에 값 설정
    document.getElementById('userName').textContent = username;
    document.getElementById('groupname').textContent = groupname;

    $('#logout').on('click', function (e) {
        e.preventDefault();
        Alert.confirm('', '로그아웃하시겠습니까?', function () {
            localStorage.removeItem('isLoggedIn');
            sessionStorage.removeItem('isLoggedIn');

            i18n.resetData();
            location.href = withCtx('/logout');
        });
    });

    csrfToken = $("[name=_csrf]").val();
});


//셀렉트 박스 동적 바인딩
function initializeSelect({
                              url,               // API 엔드포인트 URL
                              params = {},       // 요청 매개변수 (기본값은 빈 객체)
                              elementId,         // 셀렉트 요소의 ID
                              defaultOption = "선택하세요",  // 기본 옵션 텍스트
                              valueField = "code",    // 데이터의 값 필드 이름
                              textField = "value"     // 데이터의 표시 필드 이름
                          }) {
    $.get(url, params, function(data){
        console.log('확인: ', data);
        let selectElement = $(`#${elementId}`);
        selectElement.empty();
        selectElement.append(`<option value="">${defaultOption}</option>`);
        data.forEach(function(item) {
            selectElement.append(`<option value="${item[valueField]}">${item[textField]}</option>`);
        });
    });
}

// 파일명 축약 함수
function truncateFilename(filename, maxLength = 10) {
    if (!filename.includes('.')) {
        return filename; // 확장자가 없는 경우 그대로 반환
    }

    const lastDotIndex = filename.lastIndexOf('.');
    const namePart = filename.substring(0, lastDotIndex); // 파일 이름 부분
    const extension = filename.substring(lastDotIndex); // 확장자 부분

    // 이름이 maxLength보다 길 경우 줄임
    if (namePart.length > maxLength) {
        return `${namePart.substring(0, maxLength)}... (${extension})`;
    } else {
        return `${namePart}${extension}`;
    }
}
//당일
function getDateYYYYMMDD(){
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2,'0');

    return `${year}-${month}-${day}`
}

//당월 마지막일
function getLastDayOfCurrentMonth() {
    const date = new Date();
    const year = date.getFullYear();
    const month = date.getMonth() + 1; // 현재 달 (0-11이므로 +1)

    // 다음 달의 첫째 날을 계산한 후, 하루를 빼서 당월 마지막 날을 구함
    const lastDayDate = new Date(year, month, 0);
    const day = String(lastDayDate.getDate()).padStart(2, '0');
    const lastMonth = String(lastDayDate.getMonth() + 1).padStart(2, '0');

    return `${year}-${lastMonth}-${day}`;
}

//랜덤문자열 생성 50(랜덤문자열 42자리 + 오늘날짜 )자리로
function generateRandomStringWithDate(length = 32) {
    // 오늘 날짜를 "YYYYMMDD" 형식으로 가져오기
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, '0'); // 월은 0부터 시작하므로 +1 필요
    const dd = String(today.getDate()).padStart(2, '0');
    const formattedDate = `${yyyy}${mm}${dd}`;

    // 지정된 길이의 랜덤 문자열 생성
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let randomString = '';
    for (let i = 0; i < length; i++) {
        randomString += characters.charAt(Math.floor(Math.random() * characters.length));
    }

    // 랜덤 문자열과 날짜를 결합하여 반환
    return `${randomString}${formattedDate}`;
}

function populateSelectOptions(result, ele){
    const selectElement = document.getElementById(ele);

    result.data.forEach(item => {
        const option = document.createElement("option");
        option.value = item.id;
        option.textContent = item.first_name;

        selectElement.appendChild(option);
    });
}

let AjaxFunction = {
    PostAjaxRequest: function(url, async, contentType, processData, bodyData, successCallback, errorCallback){
        $.ajax({
            url: url,
            type: 'POST',
            contentType: contentType,
            async: async,   // 기본값: true
            data: bodyData,
            processData: processData,   //기본값: true
            headers: {
                'X-CSRF-Token': csrfToken
            },
            success: function(res){
                successCallback(res);
            },
            error: function(jqXHR, textStatus, errorThrown) {
                errorCallback(jqXHR, textStatus, errorThrown);
            }
        });
    }
};

//readonly 시키는 함수
function setElementsReadonlyById(elementIds, boolean) {
    // elementIds는 요소의 ID 값들을 포함하는 배열이어야 합니다.
    elementIds.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.readOnly = boolean; // 요소가 존재하면 readonly 속성 설정
        }
    });
}

//현재 날짜와 시간 포맷은 //2024-10-10 오전 11:00
function getFormattedDateTime() {
    const now = new Date();

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0'); // 월은 0부터 시작하므로 +1 필요
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

// 사용자 데이터를 가져오는 함수
function loadUserInfo() {
    $.ajax({
        url: '/account/myinfo',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            // console.log('응답 데이터:', response); // 응답 확인
            if (response.success) {
                $('#userName').text(response.data.name || '사용자명');
            } else {
                console.error('사용자 정보를 가져올 수 없습니다:', response.message);
            }
        },
        error: function(xhr, status, error) {
            console.error('AJAX 요청 실패:', status, error);
        }
    });
}

// DOM 로드 후 실행
$(document).ready(function() {
    loadUserInfo();
});

// 메뉴
document.addEventListener("DOMContentLoaded", function () {
    const dep1Items = document.querySelectorAll(".dep1 li");
    const dep2Menus = document.querySelectorAll(".dep2 ul");

    // Thymeleaf에서 현재 페이지 가져오기
    const currentPage = document.querySelector("nav").getAttribute("data-page");

    // 현재 페이지가 존재하는 경우 활성화 처리
    if (currentPage) {
        // 현재 페이지의 dep2 메뉴 활성화
        const activeLink = document.querySelector(`.dep2 a[href*="${currentPage}"]`);
        if (activeLink) {
            activeLink.classList.add("on"); // 현재 페이지 링크 강조

            // 해당 링크가 속한 dep2 찾기
            const activeDep2 = activeLink.closest("ul");
            if (activeDep2) {
                activeDep2.classList.add("active");

                // 해당 dep2의 dep1을 찾아 활성화
                const targetDep1 = activeDep2.getAttribute("data-menu");
                if (targetDep1) {
                    const matchingDep1 = document.querySelector(`.dep1 li[data-target="${targetDep1}"]`);
                    if (matchingDep1) {
                        matchingDep1.classList.add("active");
                    }
                }
            }
        }
    }else{
        // 메뉴 처음 진입시 dep1(전체메뉴) 하위 dep2 요소들 active활성화
        const activeLink = document.querySelector(`.dep2 a[href*="/"]`);
        if (activeLink) {
            activeLink.classList.add("on"); // 현재 페이지 링크 강조

            // 해당 링크가 속한 dep2 찾기
            const activeDep2 = activeLink.closest("ul");
            if (activeDep2) {
                activeDep2.classList.add("active");

                // 해당 dep2의 dep1을 찾아 활성화
                const targetDep1 = activeDep2.getAttribute("data-menu");
                if (targetDep1) {
                    const matchingDep1 = document.querySelector(`.dep1 li[data-target="${targetDep1}"]`);
                    if (matchingDep1) {
                        matchingDep1.classList.add("active");
                    }
                }
            }
        }
    }

    // dep1 클릭 이벤트 추가 (메뉴 선택 시 동작)
    dep1Items.forEach(item => {
        item.addEventListener("click", function () {
            // 모든 dep1에서 active 제거
            dep1Items.forEach(el => el.classList.remove("active"));
            this.classList.add("active");

            // 모든 dep2 숨김
            dep2Menus.forEach(menu => menu.classList.remove("active"));

            // 클릭한 dep1에 연결된 dep2만 표시
            const targetMenu = this.getAttribute("data-target");
            const selectedMenu = document.querySelector(`.dep2 ul[data-menu="${targetMenu}"]`);
            if (selectedMenu) {
                selectedMenu.classList.add("active");
            }
        });
    });
});
