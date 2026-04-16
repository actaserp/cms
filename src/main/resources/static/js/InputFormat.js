// inputFormat.js

// 전화번호 포맷
function attachTelFormatterAll(inputSelector) {
    const telInputs = document.querySelectorAll(inputSelector);
    if (!telInputs.length) return;

    telInputs.forEach(telInput => {
        telInput.addEventListener('input', function(e) {
            let numbers = e.target.value.replace(/[^0-9]/g, '');
            if (numbers.length > 11) numbers = numbers.slice(0, 11);

            let result = '';
            if(numbers.length < 4) {
                result = numbers;
            } else if(numbers.length < 7) {
                result = numbers.slice(0, 3) + '-' + numbers.slice(3);
            } else if(numbers.length === 10) {
                result = numbers.slice(0, 3) + '-' + numbers.slice(3, 6) + '-' + numbers.slice(6, 10);
            } else if(numbers.length === 11) {
                result = numbers.slice(0, 3) + '-' + numbers.slice(3, 7) + '-' + numbers.slice(7, 11);
            } else {
                result = numbers.slice(0, 2) + '-' + numbers.slice(2, 5) + '-' + numbers.slice(5);
            }
            e.target.value = result;
        });
    });
}
window.attachTelFormatter = attachTelFormatter;// 전역 등록

// 사업자번호 포맷 
function attachBizNumberFormatterAll(inputSelector) {
    const bizInputs = document.querySelectorAll(inputSelector);
    if (!bizInputs.length) return;

    bizInputs.forEach(bizInput => {
        bizInput.addEventListener('input', function(e) {
            let numbers = e.target.value.replace(/[^0-9]/g, '');

            if (numbers.length > 13) numbers = numbers.slice(0, 13);

            let result = '';
            if (numbers.length < 4) {
                result = numbers;
            } else if (numbers.length < 12) {
                // 4~11자리: 사업자번호 000-00-00000
                if (numbers.length < 6) {
                    result = numbers.slice(0, 3) + '-' + numbers.slice(3);
                } else {
                    result = numbers.slice(0, 3) + '-' + numbers.slice(3, 5) + '-' + numbers.slice(5);
                }
            } else {
                // 12자리 이상: 000000-0000000
                result = numbers.slice(0, 6) + '-' + numbers.slice(6, 13);
            }
            e.target.value = result;
        });
    });
}
window.attachBizNumberFormatterAll = attachBizNumberFormatterAll; // 전역등록

