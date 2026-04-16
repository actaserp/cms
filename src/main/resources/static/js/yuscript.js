

// 기본 script (Main, default layout 사용)
// 메시지 처리

// 메뉴
// 메뉴
// 탭패널
function tabOpen($target) {
    var $wrap = $target.closest("[data-tab]");
    $wrap.find("[data-tablink]").each(function () {
        $(this).parent().removeClass("on");
        $($(this).attr("data-tablink")).addClass("hidden");
    });
    $target.parent().addClass("on");
    $($target.attr("data-tablink")).removeClass("hidden");
}

function tabSet() {
    $("[data-tab]").each(function () {
        var $tab = $(this).find("[data-tablink]");
        if ($tab.filter(".on").length) {
            tabOpen($tab.filter(".on").eq(0));
        } else {
            tabOpen($tab.eq(0));
        }
    });
}

var addTopNewTab = function (_url) { // url로 새TAB페이지 열기
    $.getJSON('/pageinfo?targeturl=' + _url, function (datas) {
        if (nthTabs.isExistsTab('#' + datas.objId)) {
            nthTabs.toggleTab('#' + datas.objId);
        } else {
            nthTabs.addTab({
                id: String(datas.objId),
                title: datas.objNm,
                url: $.trim(datas.objUrl),
                active: true,
                allowClose: true
            });
        }
        // 신규 생성 후 북마크확인
        fnCheckTabBookMark(String(datas.objId), datas.isbookmark);
    }).fail(function (e) {
        Notify.error('유효하지 않은 URL입니다');
    });
};

var dynamicLinkCss = function (srctop, srcleft) {
    localStorage.setItem('theme-top', srctop);
    localStorage.setItem('theme-left', srcleft);
    $('link[href*="/static/css/theme-top"]').remove();
    $('link[href*="/static/css/theme-left"]').remove();

    var linkTop = document.createElement('link');
    linkTop.href = srctop + '.css';
    linkTop.async = false;
    linkTop.rel = 'stylesheet';
    linkTop.type = 'text/css';
    document.head.appendChild(linkTop);
    var linkLeft = document.createElement('link');
    linkLeft.href = srcleft + '.css';
    linkLeft.async = false;
    linkLeft.rel = 'stylesheet';
    linkLeft.type = 'text/css';
    document.head.appendChild(linkLeft);
    $('label[data-load-top-css="' + srctop + '"][data-load-left-css="' + srcleft + '"]').children('input[type=radio]').prop('checked', true);
};

var dynamicLinkCssPage = function (src) {
    $('link[href*="/static/css/theme-top"]').remove();
    var linkContent = document.createElement('link');
    linkContent.href = src + '.css';
    linkContent.async = false;
    linkContent.rel = 'stylesheet';
    linkContent.type = 'text/css';
    document.head.appendChild(linkContent);
};


(function ($) {
    $(window).load(function () {

     

        // 브라우저, os 체크 클래스 추가
        var userAgentCheck = function () {
            var ua = navigator.userAgent.toString().toLowerCase();
            var agent = {
                ie: (/msie/i).test(ua) || (/trident/i).test(ua),
                edge: (/edge/i).test(ua),
                firefox: (/firefox/i).test(ua),
                webkit: (/applewebkit/i).test(ua),
                chrome: (/chrome/i).test(ua),
                opera: (/opera/i).test(ua),
                ios: (/ip(ad|hone|od)/i).test(ua),
                android: (/android/i).test(ua)
            };

            agent.safari = agent.webkit && !agent.chrome;
            agent.mobile = document.ontouchstart !== undefined && (agent.ios || agent.android);
            agent.desktop = !(agent.ios || agent.android);

            // ie 버전체크
            if (agent.ie) {
                var _ieversion = ua.match(/(msie |trident.*rv[ :])([0-9]+)/)[2];
                _ieversion = Math.floor(_ieversion);
                agent.ie = "ie" + _ieversion;
            }

            agent.os = (navigator.appVersion).match(/(mac|win|linux)/i);
            agent.os = agent.os ? agent.os[1].toLowerCase() : '';

            var _html = document.getElementsByTagName('html')[0];
            var _class = '';
            for (var value in agent) {
                if (agent[value]) {
                    if (value == 'os') {
                        _class += agent.os;
                    } else if (value == 'ie') {
                        _class += agent[value] + ' ';
                    } else {
                        _class += value + ' ';
                    }
                }
            }
            _html.className += _class;
        }();

        tabSet(); // 텝메뉴



        // 헤더 알림등 팝업
        $('[data-navpopOpen]').on('click', function (e) {
            var poptarget = $(this).attr('data-navpopOpen');
            if ($('[data-navpopCont="' + poptarget + '"]').hasClass('open')) {
                $('[data-navpopCont="' + poptarget + '"]').removeClass('open');
                $(this).removeClass('on');
            } else {
                $('[data-navpopCont]').removeClass('open');
                $('[data-navpopCont="' + poptarget + '"]').addClass('open');
                $('[data-navpopOpen]').removeClass('on');
                $(this).addClass('on');
            }

            // 내정보팝업 이름 길이
            var myInfo = $('.profile_box .navpop_cont'),
                myInfoName = $('.profile_box .navpop_cont.open .pop_head strong').outerWidth();
            myInfo.css('width', myInfoName + 158);
        });
        $('[data-navpopClose]').on('click', function (e) {
            var poptarget = $(this).attr('data-navpopClose');
            $('[data-navpopCont="' + poptarget + '"]').removeClass('open');
            $('[data-navpopOpen]').removeClass('on');
            e.preventDefault();
        });

        //tab
        $(document).on("click", "[data-tablink]", function (e) {
            e.preventDefault();
            tabOpen($(this));
        });

        jQuery('.tabs .tab-links a').on('click', function (e) {
            var currentAttrValue = jQuery(this).attr('href');

            // Show/Hide Tabs
            jQuery('.tabs ' + currentAttrValue).show().siblings().hide();

            // Change/remove current tab to active
            jQuery(this).parent('li').addClass('active').siblings().removeClass('active');

            e.preventDefault();
        });

        //treeview

        $('li:not(:has(ul))').css({ cursor: 'default', 'list-style-image': 'none' });

        $('.tree_img .tree_in:has(ul)')
            .css({ cursor: 'pointer', 'list-style-image': 'url(../img/icon/box_plus_icon.png)' })
            .children().hide();
        $('.tree_img .tree_in:has(ul)').click(function (event) {
            if (this == event.target) {
                if ($(this).children().is(':hidden')) {
                    $(this).css('list-style-image', 'url(../img/icon/box_minus_icon.png)').children().slideDown();
                }
                else {
                    $(this).css('list-style-image', 'url(../img/icon/box_plus_icon.png)').children().slideUp();
                }
            }
            return false;
        });

        $('.tree_basic .tree_in:has(ul)')
            .css({ cursor: 'pointer', 'list-style-image': 'url(../img/icon/plus_icon.gif)' })
            .children().hide();
        $('.tree_basic .tree_in:has(ul)').click(function (event) {
            if (this == event.target) {
                if ($(this).children().is(':hidden')) {
                    $(this).css('list-style-image', 'url(../img/icon/minus_icon.gif)').children().slideDown();
                }
                else {
                    $(this).css('list-style-image', 'url(../img/icon/plus_icon.gif)').children().slideUp();
                }
            }
            return false;
        });

        // iframe 관련 CSS 변경
        // 팝업
        $(".popup").parent('body').css({ "background": "#fff" });
        // 컨텐츠 - 텝메뉴
        $(".tab").find('iframe').load(function () {
            $('iframe').contents().find("body").css({ "background": "#fff" })
                .find(".content_wrap").css({ "padding": 0 + "px" })
                .find("section").css({ "padding": 0 + "px", "box-shadow": "none" })
                .find(".tabs").attr("data-tab", "tabwrap").parent(".row").css({ "border": "1px solid red" })
        });
        // 컨텐츠 - 분할레이아웃
        $(".divi_body").find('iframe').load(function () {
            $('iframe').contents().find("body").css({ "background": "#fff" })
                .find(".content_wrap").css({ "padding": 10 + "px" })
                .find("section").css({ "box-shadow": "none" })
        });

        //********************************
        $('html').click(function (e) {  // 외부요소 클릭시 div창 닫기
            if (!$(e.target).parents('.profile_box').hasClass('profile_box')) {
                $(top.document).find('.profile_mbox').hide();
            }
            if (!$(e.target).parents('.notification_box').hasClass('notification_box')) {
                $(top.document).find('.notification_mbox').hide();
            }
            if (!$(e.target).parents('#contextTabMenu').hasClass('dropdown-menu')) {
                $(top.document).find('#contextTabMenu').hide();
            }
            $('#contextTabMenu').hide();
            // 모바일일경우 다른곳 클릭시 메뉴 닫힘
            if ($(top.document).find('.menu_area').hasClass('mobile_menu_area')) {
                if (!$(e.target).hasClass('bar') && $(e.target).parents('.mobile_menu_area').length == 0) {
                    if (!$(top.document).find('body').hasClass('open')) {
                        $(top.document).find('body').addClass('open');
                        $(top.document).find('.wrap').addClass('menu_close');
                        $(top.document).find('.con_area').addClass('open');
                    }
                }
            }
        });

        $(".notification_box").click(function () {
            if ($('#msgCntHeader').text() != '0') {
                $(".notification_mbox").toggle();
            } else {
                $(".notification_mbox").hide();
            }
        });

    });
})(jQuery);

$(document).on("click", ".tab-links a", function (event) {
    event.preventDefault();

    // 클릭된 탭 링크의 href 속성 값을 가져옴
    var tabId = $(this).attr("href");

    // 해당 탭을 보여주고 활성화
    $(".tab-item").hide(); // 모든 탭 숨김
    $(tabId).show(); // 선택된 탭만 표시

    // 현재 활성화된 탭을 나타내기 위해 클래스 추가/제거
    $(".tab-links li").removeClass("active");
    $(this).parent().addClass("active");
});
