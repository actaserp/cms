/*=================================================================================
 * 공통제이쿼리
=================================================================================*/
/*     toggle     */
$(document).on("click",".toggle",function(){
    $(this).toggleClass("on");
});

/*     toggle-group     */
$(document).on("click",".toggle-group li",function(){
    $(this).closest(".toggle-group").find("li").removeClass("on");
    $(this).addClass("on");
});

/*     toggles-group     */
$(document).on("click",".toggles-group li",function(){
    $(this).toggleClass("on");
});

/*        Acodion        */
$(document).ready(function () {
    // 초기 상태 설정: search-wrap-sp만 열고 나머지는 닫음
    $('.search-wrap-sp .aco-cont').hide(); // search-wrap-sp 열림
    $('.search-wrap-sp .aco-hd').removeClass('active'); // search-wrap-sp 헤더 active 상태

    // 나머지 아코디언 닫기
    $('.aco-cont').not('.search-wrap-sp .aco-cont').hide(); // 다른 아코디언 닫기
    $('.aco-hd').not('.search-wrap-sp .aco-hd').removeClass('active'); // 다른 헤더에서 active 제거

    // 클릭 이벤트
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
$(document).ready(function() {

    $(".tab-links a").click(function(event) {
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
    
});

/*         Popup          */
$(document).ready(function() {
    // 작업이력 팝업 열기
    $(document).on('click', '.btn-popup-open', function () {
        var popupId = $(this).data('popup');
        $(`#${popupId} .popup-overlay`).fadeIn(200);
        $(`#${popupId} .popup-wrapper`).addClass('active');
    });

    // 모든 팝업 닫기
    $(document).on('click', '.btn-popup-close', function () {
        $(this).closest('.popup-wrapper').removeClass('active');
        $(this).closest('.mobile-layout-popup').find('.popup-overlay').fadeOut(200);
    });
});

/*         GNB MENU          */
$(document).ready(function(){
    $('.btn-menu').on('click', function() {
        $('.mobile-layout-menu').addClass('active');
    });

    $('.btn-menu-close').on('click', function() {
        $('.mobile-layout-menu').removeClass('active');
    });
});

/*=================================================================================
 * UI 공통
=================================================================================*/
/*  Input Text delete  */
$(document).ready(function() {
    $('input[type="text"]').each(function() {
        var $inputWrapper = $(this).wrap('<div class="input-clear"></div>').parent();
        var $clearBtn = $('<span class="btn-clear">&times;</span>');

        $inputWrapper.append($clearBtn);

        $(this).on('input', function() {
            if ($(this).val().length > 0) {
                $clearBtn.show();
            } else {
                $clearBtn.hide();
            }
        });

        $clearBtn.on('click', function() {
            var $input = $(this).siblings('input[type="text"]');
            $input.val('');
            $(this).hide();
        });
    });
});
/*  Textarea  */
$(document).ready(function() {
    $('textarea').on('input', function() {
        const textLength = $(this).val().length;
        $(this).next('.text-count').text(`${textLength}/100`);
    });
});

/* 파일업로드 */
$(document).ready(function() {
    function initializeUploadComponent(component) {
        let uploadedFiles = [];
        const $fileInput = $(component).find('.fileInput');
        const $fileList = $(component).find('.filelist');
        const $fileCountTitle = $(component).find('.upload-filelist .title h5');

        $fileInput.on('change', function(event) {
            handleFileSelect(event.target.files);
        });

        $(component).find('.upload-filebox').on('dragover', function(event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).addClass('dragging');
        });

        $(component).find('.upload-filebox').on('dragleave', function(event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).removeClass('dragging');
        });

        $(component).find('.upload-filebox').on('drop', function(event) {
            event.preventDefault();
            event.stopPropagation();
            $(this).removeClass('dragging');
            handleFileSelect(event.originalEvent.dataTransfer.files);
        });

        function handleFileSelect(files) {
            $.each(files, function(index, file) {
                if (!uploadedFiles.some(f => f.name === file.name && f.size === file.size)) {
                    uploadedFiles.push(file);
                    const fileSize = (file.size / 1024).toFixed(2) + ' KiB';
                    const li = $('<li>').html(`
                        <p>${file.name} <span>(${fileSize})</span></p>
                        <a href="#" title="삭제" class="btn-file-delete">
                            <img src="../assets/images/icon/ico-filedelete.svg" alt="삭제아이콘">
                        </a>
                    `);
                    $fileList.append(li);
                }
            });
            updateFileCount();
        }

        $(component).on('click', '.btn-file-delete', function(event) {
            event.preventDefault();
            const li = $(this).closest('li');
            const fileName = li.find('p').text().split(' (')[0];
            uploadedFiles = uploadedFiles.filter(file => file.name !== fileName);
            li.remove();
            updateFileCount();
        });

        $(component).find('.btn-file-deleteall').on('click', function(event) {
            event.preventDefault();
            $fileList.empty();
            uploadedFiles = [];
            updateFileCount();
        });

        function updateFileCount() {
            const fileCount = uploadedFiles.length;
            $fileCountTitle.text(`Files (${fileCount})`);
        }
    }

    $('.upload-component').each(function() {
        initializeUploadComponent(this);
    });
});
