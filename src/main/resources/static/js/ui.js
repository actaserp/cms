$(document).ready(function () {

});

/*=================================================================================
 * 공통제이쿼리
=================================================================================*/
/*     toggle     */
$(document).on("click", ".toggle", function () {
    $(this).toggleClass("on");
});

/*     toggle-group     */
$(document).on("click", ".toggle-group li", function () {
    $(this).closest(".toggle-group").find("li").removeClass("on");
    $(this).addClass("on");
});

/*     toggles-group     */
$(document).on("click", ".toggles-group li", function () {
    $(this).toggleClass("on");
});

/*        Acodion        */
$(document).ready(function () {
    $('.aco-hd').click(function () {
        // 클릭된 아코디언 항목의 컨텐츠 토글
        $(this).next('.aco-cont').slideToggle();
        // 다른 아코디언 항목의 컨텐츠는 접히도록 함
        $('.aco-cont').not($(this).next()).slideUp();
        // 활성 클래스 추가 및 제거
        $(this).toggleClass('active');
        $('.aco-hd').not($(this)).removeClass('active');
    });
});

/*         TAB          */
/*         TAB          */

const ExceptionUrl = [];

$(document).ready(function () {
    // 예외 URL 리스트를 상대 경로로 정의합니다.
    var exceptionUrls = [
        "/gui/wm_equipment_search/default",
        // "/gui/wm_equipment_alarm/default",
        // 추가 예외 URL을 여기에 추가할 수 있습니다.
        "/gui/another_exception"
    ];

    // 현재 URL의 경로를 가져옵니다.
    var currentPath = window.location.pathname;

    // 현재 경로가 예외 리스트에 포함되어 있는지 확인합니다.
    var isException = exceptionUrls.some(function (url) {
        return currentPath === url;
    });

    if (!isException) {
        $(".tab-links a").click(function (event) {
            event.preventDefault();

            // 클릭된 탭 링크의 href 속성 값을 가져옴
            var tabId = $(this).attr("href");

            // 해당 탭을 보여주고 활성화
            $(".tab-item").hide();
            $(tabId).show();

            // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
            $(".tab-links li").removeClass("active");
            $(this).parent().addClass("active");
        });

        // 초기에 첫 번째 탭을 활성화
        $(".tab-item:first").show();
        $(".tab-links li:first").addClass("active");
    }

    // 예외 URL 리스트를 상대 경로로 정의합니다.
    var exceptionUrls_sub = [
        "/gui/wm_equipment_alarm/default",
        "/gui/wm_sale_graph/default",
        "/gui/wm_salesmp_graph/default",
        "/gui/wm_salerec_graph/default",
        "/gui/wm_cost_graph/default",
        "/gui/wm_gasamount_list/default",
        "/gui/wm_gasbill_list/default",
        "/gui/wm_fuel_management/default",
        "/gui/wm_gasele_analysis/default",
        "/gui/wm_robot_analysis/default",
        "/gui/wm_sale_gene/default",

        // 추가 예외 URL을 여기에 추가할 수 있습니다.
        "/gui/another_exception",
        "/gui/wm_inspec_statistics/default",
        "/gui/wm_inspec_month_list/default",
        "/gui/wm_equipment_search/default"
    ];

    // 현재 URL의 경로를 가져옵니다.
    var currentPath_sub = window.location.pathname;

    // 현재 경로가 예외 리스트에 포함되어 있는지 확인합니다.
    var isException_sub = exceptionUrls_sub.some(function (url) {
        return currentPath_sub === url;
    });

    if (!isException_sub) {
        $(".tab-links-sub a").click(function (event) {
            event.preventDefault();

            // 클릭된 탭 링크의 href 속성 값을 가져옴
            var tabId = $(this).attr("href");

            // 해당 탭을 보여주고 활성화
            $(".tab-item-sub").hide();
            $(tabId).show();

            // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
            $(".tab-links-sub li").removeClass("active");
            $(this).parent().addClass("active");
        });

        // 초기에 첫 번째 탭을 활성화
        $(".tab-item-sub:first").show();
        $(".tab-links-sub li:first").addClass("active");
    }
});
//기존에는 아래인데 장비이력조회 등 복수의 섹션에서 탭을 활용해야해서 주석처리해놓았음
/*$(document).ready(function () {

    $(".tab-links a").click(function (event) {
        event.preventDefault();

        // 클릭된 탭 링크의 href 속성 값을 가져옴
        var tabId = $(this).attr("href");

        // 해당 탭을 보여주고 활성화
        $(".tab-item").hide();
        $(tabId).show();

        // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
        $(".tab-links li").removeClass("active");
        $(this).parent().addClass("active");
    });

    // 초기에 첫 번째 탭을 활성화
    $(".tab-item:first").show();
    $(".tab-links li:first").addClass("active");

});*/

/*         Popup          */
$(document).ready(function () {
    $('.btn-popup-open').on('click', function () {
        var popupId = $(this).data('popup');
        $('.popup-overlay').fadeIn(200);
        $('#' + popupId).fadeIn(200);
    });

    $('.btn-popup-close').on('click', function () {
        $('.popup-overlay').fadeOut(200);
        $('.popup-wrapper').fadeOut(200);
    });
});


/*=================================================================================
 * UI 공통
=================================================================================*/
/*        Hader : ALram TAB          */
// $(document).ready(function () {
//
//     $(".alarm-tab-links a").click(function (event) {
//         event.preventDefault();
//
//         // 클릭된 탭 링크의 href 속성 값을 가져옴
//         var tabId = $(this).attr("href");
//
//         // 해당 탭을 보여주고 활성화
//         $(".alarm-tab-item").hide();
//         $(tabId).show();
//
//         // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
//         $(".alarm-tab-links li").removeClass("active");
//         $(this).parent().addClass("active");
//     });
//
//     // 초기에 첫 번째 탭을 활성화
//     $(".alarm-tab-item:first").show();
//     $(".alarm-tab-links li:first").addClass("active");
//
// });


/*  Input Text delete  */
$(document).ready(function () {
    $('input[type="text"]').each(function () {
        var $inputWrapper = $(this).wrap('<div class="input-clear"></div>').parent();
        var $clearBtn = $('<span class="btn-clear">&times;</span>');

        $inputWrapper.append($clearBtn);

        $(this).on('input', function () {
            if ($(this).val().length > 0) {
                $clearBtn.show();
            } else {
                $clearBtn.hide();
            }
        });

        $clearBtn.on('click', function () {
            var $input = $(this).siblings('input[type="text"]');
            $input.val('');
            $(this).hide();
            // 검색된 li 목록을 숨기고 초기화
            $('#suggestions').empty().hide();
        });
    });
});
/*  Textarea  */
$(document).ready(function () {
    $('textarea').on('input', function () {
        const textLength = $(this).val().length;
        $(this).next('.text-count').text(`${textLength}/100`);
    });
});

// 파일업로드 변수설정
let uploadedFiles = [];
// 삭제할 파일 목록
let deletedFiles = [];
const MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024; // 1GB in bytes
/* 파일업로드 */


function initializeUploadComponent(component) {

    const $fileInput = $(component).find('.fileInput');
    const $fileList = $(component).find('.filelist');
    const $fileCountTitle = $(component).find('.upload-filelist .title h5');

    $fileInput.on('change', function (event) {
        // handleFileSelect(event.target.files);
        // 파일 선택 후 파일 입력 요소 초기화
        // resetFileInput($fileInput);

        const files = event.target.files;

        // 특정 모달에서만 파일이 두 개 이상 선택되었는지 확인
        if ($('#single-file-upload-page').length > 0 && (files.length > 1 || uploadedFiles.length > 0)) {
            Alert.alert('', '파일은 한 개만 첨부할 수 있습니다.');
            resetFileInput($fileInput);
            return;
        }

        handleFileSelect(files);
    });

    $(component).find('.upload-filebox').on('dragover', function (event) {
        event.preventDefault();
        event.stopPropagation();
        $(this).addClass('dragging');
    });

    $(component).find('.upload-filebox').on('dragleave', function (event) {
        event.preventDefault();
        event.stopPropagation();
        $(this).removeClass('dragging');
    });

    $(component).find('.upload-filebox').on('drop', function (event) {
        event.preventDefault();
        event.stopPropagation();
        $(this).removeClass('dragging');
        // handleFileSelect(event.originalEvent.dataTransfer.files);

        const files = event.originalEvent.dataTransfer.files;

        // 특정 페이지에서만 드래그 앤 드롭으로 추가된 파일들이 두 개 이상인지 확인
        if ($('#single-file-upload-page').length > 0 && (files.length > 1 || uploadedFiles.length > 0)) {
            Alert.alert('', '파일은 한 개만 첨부할 수 있습니다.');
            resetFileInput($fileInput);
            return;
        }

        handleFileSelect(files);
        // 드래그 앤 드롭 후 파일 입력 요소 초기화
        resetFileInput($fileInput);
    });

    function handleFileSelect(files) {
        $.each(files, function (index, file) {
            if (file.size > MAX_FILE_SIZE) {
                Alert.alert('', '파일 크기는 1GB를 초과할 수 없습니다.');
                resetFileInput($fileInput);
            } else if(!uploadedFiles.some(f => f.name === file.name && f.size === file.size)) {
                uploadedFiles.push(file);
                const fileSize = (file.size / 1024).toFixed(2) + ' KiB';
                const li = $('<li>').html(`
                <p>${file.name} <span>(${fileSize})</span></p>
                <a href="#" title="삭제" class="btn-file-delete">
                    <img src="/images/icon/ico-filedelete.svg" alt="삭제아이콘">
                </a>
            `);
                $fileList.append(li);
            }
        });
        updateFileCount();
    }

    $(component).on('click', '.btn-file-delete', function (event) {
        event.preventDefault();
        const li = $(this).closest('li');
        const fileName = li.find('p').text().split(' (')[0];

        // 파일 이름과 일치하지 않는 파일들로 필터링하여 uploadedFiles 업데이트
        const removedFile = uploadedFiles.find(file => file.name === fileName);
        uploadedFiles = uploadedFiles.filter(file => file.name !== fileName);

        // 삭제된 파일을 deletedFiles에 추가
        if (removedFile) {
            deletedFiles.push(removedFile);
        }
        li.remove();
        updateFileCount();

        // 파일 선택 후 파일 입력 요소 초기화
        resetFileInput($fileInput);
    });

    $(component).find('.btn-file-deleteall').on('click', function (event) {
        event.preventDefault();
        // 모든 업로드된 파일을 삭제된 파일 리스트에 추가
        deletedFiles = deletedFiles.concat(uploadedFiles);

        $fileList.empty();
        uploadedFiles = [];
        updateFileCount();
        resetFileInput($fileInput);
    });

    function updateFileCount() {
        const fileCount = uploadedFiles.length;
        $fileCountTitle.text(`Files (${fileCount})`);
        $('.btn-file span').text(`(${fileCount})`);
    }

    function resetFileInput($input) {
        $input.val('');
    }
}

window.initializeUploadComponent = initializeUploadComponent;

$(document).ready(function () {
    $('.upload-component').each(function () {
        initializeUploadComponent(this);
    });
});

/*     cardlist 선택  */
// $(document).ready(function () {
//     // Click event to toggle 'on' class
//     $('.card-edit-wrap section').click(function () {
//         $('.card-edit-wrap section').removeClass('on');
//         $(this).addClass('on');
//     });
//
//     // Drag and drop functionality
//     $('.card-edit-wrap').sortable({
//         items: 'section',
//         placeholder: 'ui-state-highlight',
//         helper: 'clone', // Using a clone of the dragged element for better performance
//         tolerance: 'pointer', // Improves the drop detection accuracy
//         start: function (event, ui) {
//             $(".gesture-box").hide();
//         },
//         stop: function (event, ui) {
//             // When dragging stops, remove 'on' class from all sections
//             $('.card-edit-wrap section').removeClass('on');
//             // Add 'on' class to the dragged section
//             ui.item.addClass('on');
//         }
//     });
//
//     // Click event to toggle 'card-hidden' class
//     $('.btn-hidden').click(function (e) {
//         e.stopPropagation(); // Prevent the click event from bubbling up to the section
//         $(this).closest('section').toggleClass('card-hidden');
//     });
// });

/*=================================================================================
 * Layout 공통
=================================================================================*/
/*        Header - Alram        */
$(document).ready(function () {

    /* ===============================
       🔔 알람 탭 전환
       =============================== */
    $(".alarm-tab-links a").on("click", function (event) {
        event.preventDefault();
        event.stopPropagation();

        const tabId = $(this).attr("href");

        $(".alarm-tab-item").hide();
        $(tabId).show();

        $(".alarm-tab-links li").removeClass("active");
        $(this).parent().addClass("active");
    });

    // 초기 탭 상태
    $(".alarm-tab-item").hide();
    $(".alarm-tab-item:first").show();
    $(".alarm-tab-links li:first").addClass("active");


    /* ===============================
       🔔 알람 박스 열고 닫기
       =============================== */
    $('.alarm').on('click', function (event) {
        event.preventDefault();
        event.stopPropagation();

        $('#alarmOverlay').show();
        $('.alarm-box').slideToggle('fast');
    });

    $('.alarm-box').on('click', function (event) {
        event.stopPropagation();
    });

    $('#alarmOverlay').on('click', function () {
        $('.alarm-box').slideUp('fast');
        $('#alarmOverlay').hide();
    });

    $(document).on('click', function () {
        $('.alarm-box').slideUp('fast');
        $('#alarmOverlay').hide();
    });

    $('.btn-gray').on('click', function () {
        $('.alarm-box').slideUp('fast');
        $('#alarmOverlay').hide();
    });

});

/*        Header - User        */
$(document).ready(function () {
    $('.user').on('click', function (event) {
        event.preventDefault(); // 기본 동작을 막음 (필요에 따라 사용)
        $('.user-box').slideToggle('fast'); // 서서히 펼쳐지거나 접히게 함
    });

    // 클릭 시 .user-box가 외부 클릭으로 닫히게 하려면
    $(document).on('click', function (event) {
        if (!$(event.target).closest('.user, .user-box').length) {
            $('.user-box').slideUp('fast');
        }
    });
});

/*       Sidebar - TAB MENU        */
$(document).ready(function () {

    $(".layout-sidebar-tab a").click(function (event) {
        event.preventDefault();

        // 클릭된 탭 링크의 href 속성 값을 가져옴
        var tabId = $(this).attr("href");

        // 해당 탭을 보여주고 활성화
        $(".sidebar-tabcont").hide();
        $(tabId).show();

        // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
        $(".layout-sidebar-tab li").removeClass("active");
        $(this).parent().addClass("active");
    });

    // 초기에 첫 번째 탭을 활성화
    $(".sidebar-tabcont:first").show();
    $(".layout-sidebar-tab li:first").addClass("active");

});

/*      Sidebar - LNB        */
(function ($) {
    var lnbUI = {
        click: function (target, speed) {
            var _self = this,
                $target = $(target);
            _self.speed = speed || 300;
            //alert(_self);
            $target.each(function () {
                if (findChildren($(this))) {
                    return;
                }
            });

            function findChildren(obj) {
                return obj.find('> ul').length > 0;
            }

            $target.on('click', 'a', function (e) {
                e.stopPropagation();
                var $this = $(this),
                    $depthTarget = $this.next(),
                    $siblings = $this.parent().siblings();

                $this.parent('li').find('ul li').removeClass('active');
                $siblings.removeClass('active');
                $siblings.find('ul').slideUp(250);

                if ($depthTarget.css('display') == 'none') {
                    _self.activeOn($this);
                    $depthTarget.slideDown(_self.speed);

                    var $onlyDep2 = $depthTarget.children('li');
                    if ($onlyDep2.length === 1) {
                        // dep2 클릭 시 dep3가 1개면 바로 탭 열기
                        $onlyDep2.find('a').trigger('click');
                    }
                } else {
                    $depthTarget.slideUp(_self.speed);
                    _self.activeOff($this);
                }
            });

            // $(".dep1 > li.active > a").parents('ul').show();
            // $("li.active > a").parents('.dep1-1').addClass('active');
            // $(".dep2 > li.active > a").parents("ul").show();
            // $(".dep3 > li.active > a").parents("ul").show();
            // $(".dep4 > li.active > a").parents("ul").show();
            // Add 'on' class when 'dep4' link is clicked
            // $(".dep3 > li > a").on('click', function (e) {
            //     e.preventDefault();
            //     var $parentLi = $(this).parent('li');
            //     $(".dep3 > li").removeClass('on');
            //     $parentLi.addClass('on');
            // });

        },
        activeOff: function ($target) {
            $target.parent().removeClass('active');
        },
        activeOn: function ($target) {
            $target.parent().addClass('active');
        }
    }
    $(function () {
        lnbUI.click('.layout-nav li', 300);
    });
}(jQuery));




/*      Battery       */
// document.addEventListener('DOMContentLoaded', function () {
//     const batteryItems = document.querySelectorAll('.battery-wrap');
//
//     batteryItems.forEach(item => {
//         const batteryLevelElement = item.querySelector('.battery-level');
//         const chargingIndicatorElement = item.querySelector('.charging-indicator');
//         const batteryNumElement = item.querySelector('.battery-num');
//         const batteryLevel = parseInt(batteryNumElement.textContent, 10);
//
//         function updateBatteryLevel(level, isCharging) {
//             batteryLevelElement.style.width = level + '%';
//             batteryNumElement.textContent = level + '%'; // 배터리 잔량 숫자로 표기
//
//             // 충전 중일 때 배터리 컬러 변경
//             if (chargingIndicatorElement.classList.contains('charging')) {
//                 batteryLevelElement.style.backgroundColor = '#19D7B4';
//             } else {
//                 // 배터리 상태에 따라 색상을 변경
//                 if (level > 50) {
//                     batteryLevelElement.style.backgroundColor = '#000'; // 녹색
//                 } else if (level > 20) {
//                     batteryLevelElement.style.backgroundColor = '#000'; // 노란색
//                 } else {
//                     batteryLevelElement.style.backgroundColor = '#f44336'; // 빨간색
//                 }
//             }
//
//             // 충전 중일 때 애니메이션 표시
//             if (isCharging) {
//                 chargingIndicatorElement.classList.add('charging');
//             } else {
//                 chargingIndicatorElement.classList.remove('charging');
//             }
//         }
//
//         // 초기 배터리 레벨 설정 (예: 첫 번째와 세 번째 항목은 충전 중)
//         updateBatteryLevel(batteryLevel, chargingIndicatorElement.classList.contains('charging'));
//
//     });
// });