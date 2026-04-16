/**
 * nth-tabs
 * author:nethuige
 * version:2.0
 */
(function ($) {

    $.fn.nthTabs = function (options) {

        // 플러그인의 40은 기본 왼쪽 여백입니다.
        var nthTabs = this;
        var delflag = false;

        var defaults = {
            allowClose: true, // 새 탭 (종료 허용 여부), 기본적으로 활성화 됨
            active: true, // 새 탭, 활성 상태, 기본적으로 활성화 됨
            location: true, //새 탭, 자동으로 위치 여부, 기본적으로 사용 가능
            fadeIn: true, // 새 탭, 페이드 인 효과, 기본적으로 활성화 됨
            rollWidth: nthTabs.width() - 120 // 스크롤 가능 영역 너비, 120은 3 개의 조작 버튼의 너비입니다.
        };

        var settings = $.extend({}, defaults, options);

        var handler = [];

        var frameName = 0;

        var tabCloseTxt = ['現在タブを探す','現在タブを閉じる','他のタブを閉じる','すべてのタブを閉じる'];
        var template =
            '<div class="page-tabs">' +
            '<a href="#" class="roll-nav roll-nav-left"><span class="fa fa-angle-left"></span><span class="hidden">탭 왼쪽 이동버튼</span></a>' +
            '<div class="content-tabs">' +
            '<div class="content-tabs-container">' +
            '<ul class="nav nav-tabs" id="tabdragdrop"></ul>' +
            '</div>' +
            '</div>' +
            '<a href="#" class="roll-nav roll-nav-right"><span class="fa fa-angle-right"></span><span class="hidden">탭 오른쪽 이동버튼</span></a>' +
            '<div class="dropdown roll-nav right-nav-list">' +
            '<a href="#" class="dropdown-toggle" data-toggle="dropdown"><span class="hidden">탭 드롭다운 메뉴버튼</span>' +
            '<span class="tab-down"></span></a>' +
            '<ul class="dropdown-menu">' +
            '<li><a href="#" class="tab-location">'+tabCloseTxt[0]+'</a></li>' +
            '<li><a href="#" class="tab-close-current">'+tabCloseTxt[1]+'</a></li>' +
            '<li role="separator" class="divider"></li>' +
            '<li><a href="#" class="tab-close-other">'+tabCloseTxt[2]+'</a></li>' +
            '<li><a href="#" class="tab-close-all">'+tabCloseTxt[3]+'</a></li>' +
            '<li class="divider"></li>' +
            '<li class="scrollbar-outer tab-list-scrollbar">' +
            '<div class="tab-list-container"><ul class="tab-list"></ul></div>' +
            '</li>' +
            '</ul>' +
            '</div>' +
            '</div>' +
            '<div class="tab-content"></div>';

        // 플러그인 사용
        var run = function(){
            nthTabs.html(template);
            event.onWindowsResize().onTabClose().onTabRollLeft().onTabRollRight().onTabList()
                .onTabCloseOpt().onTabCloseAll().onTabCloseOther().onLocationTab().onTabToggle();
            return methods;
        };

        // 방법 목록
        var methods = {

            // 모든 탭 너비 가져 오기
            getAllTabWidth: function () {
                var sum_width = 0;
                nthTabs.find('.nav-tabs li').each(function () {
                    sum_width += parseFloat($(this).width());
                });
                return sum_width;
            },

            // 왼쪽 및 오른쪽 슬라이딩 단계 값 가져 오기
            getMarginStep: function () {
                return settings.rollWidth / 2;
            },

            // 현재 활성 상태 탭 ID 가져 오기
            getActiveId: function () {
                return nthTabs.find('li[class="active"]').find("a").attr("href").replace('#', '');
            },

            // 모든 탭 가져 오기
            getTabList: function () {
                var tabList = [];
                nthTabs.find('.nav-tabs li a').each(function () {
                    tabList.push({id: $(this).attr('href'), title: $(this).children('span').html()});
                });
                return tabList;
            },

            // 새 단일 탭 만들기
            addTab: function (options) {
                // nav-tab
                var tab = [];
                var active = options.active == undefined ? settings.active : options.active;
                var allowClose = options.allowClose == undefined ? settings.allowClose : options.allowClose;
                var location = options.location == undefined ? settings.location : options.location;
                var fadeIn = options.fadeIn == undefined ? settings.fadeIn : options.fadeIn;
                var url = options.url == undefined ? "" : options.url;
                tab.push('<li title="' + options.title + '" '+(allowClose ? '' : 'not-allow-close')+'>');
                tab.push('<a href="#' + options.id + '" data-toggle="tab">');
                tab.push('<span>' + options.title + '</span>');
                tab.push('</a>');
                allowClose ? tab.push('<i class="icon nth-icon-close-mini tab-close"></i>') : '';
                tab.push('</li>');
                nthTabs.find(".nav-tabs").append(tab.join(''));
                //tab-content
                var tabContent = [];
                tabContent.push('<div class="tab-pane '+(fadeIn ? 'animation-fade' : '')+'" id="' + options.id  +'" '+(allowClose ? '' : 'not-allow-close')+'>');
                if(url.length>0){
                    tabContent.push('<iframe src="'+options.url+'" frameborder="0" name="iframe-'+frameName+'" class="nth-tabs-frame"></iframe>');
                    frameName++;
                }else{
                    tabContent.push('<div class="nth-tabs-content">'+options.content+"</div>");
                }
                tabContent.push('</div>');
                nthTabs.find(".tab-content").append(tabContent.join(''));
                active && this.setActTab(options.id);
                location && this.locationTab(options.id);

                // 탭 이동가능 기능추가 (2020.03.25 dy.lee) - 31라인 ul태그에 아이디 추가 <ul class="nav nav-tabs" id="tabdragdrop"></ul>
                sortable('#tabdragdrop', {
                    forcePlaceholderSize: true
                });
                return this;
            },

            // 여러 개의 새 탭 만들기
            addTabs: function (tabsOptions) {
                for(var index in tabsOptions){
                    this.addTab(tabsOptions[index]);
                }
                return this;
            },

            // 위치 지정 탭
            locationTab: function (tabId) {
                tabId = tabId == undefined ? methods.getActiveId() : tabId;
                tabId = tabId.indexOf('#') > -1 ? tabId : '#' + tabId;
                var navTabOpt = nthTabs.find("[href='" + tabId + "']"); // 현재 작동중인 탭 객체
                // 현재 활성 탭 앞에있는 모든 형제 탭의 너비 합계를 계산하십시오
                var beforeTabsWidth = 0;
                navTabOpt.parent().prevAll().each(function () {
                    beforeTabsWidth += $(this).width();
                });
                // 탭 컨테이너 객체 가져 오기
                var contentTab = navTabOpt.parent().parent().parent();
                // 사례 1 : 이전 형제 탭의 너비 합계가 탭 표시 영역보다 작습니다.
                if (beforeTabsWidth <= settings.rollWidth) {
                    margin_left_total = 40;
                }
                // 사례 2 : 이전 형제 탭의 너비 합계가 탭의 표시 영역보다 큰 경우 여백은 왼쪽으로 정수 배만큼 간격 띄우기입니다.
                else{
                    margin_left_total = 40 - Math.floor(beforeTabsWidth / settings.rollWidth) * settings.rollWidth;
                }
                contentTab.css("margin-left", margin_left_total);
                return this;
            },

            // 단일 탭 삭제
            delTab: function (tabId) {
                tabId = tabId == undefined ? methods.getActiveId() : tabId;
                tabId = tabId.indexOf('#') > -1 ? tabId : '#' + tabId;
                var navTabA = nthTabs.find("[href='" + tabId + "']");
                if(navTabA.parent().attr('not-allow-close')!=undefined) return false;
                // 꺼짐이 활성 탭인 경우
                if (navTabA.parent().attr('class') == 'active') {
                    // 탭 활성화, 그 뒤에 활성화가 있으면 탭 활성화, 그렇지 않으면 정면 활성화
                    var activeNavTab = navTabA.parent().next();
                    var activeTabContent = $(tabId).next();
                    if (activeNavTab.length < 1) {
                        activeNavTab = navTabA.parent().prev();
                        activeTabContent = $(tabId).prev();
                    }
                    activeNavTab.addClass('active');
                    activeTabContent.addClass('active');
                }
                // 이전 탭 제거
                navTabA.parent().remove();
                $(tabId).remove();
                
                var $delTabObj = $('#left-menu').find('a[data-objid="'+tabId.replace('#','')+'"]').closest('li.has_sub');
                $delTabObj.removeClass('open');
                $delTabObj.children('ul').hide();
                
                delflag = true;
                return this;
            },

            // 다른 탭 삭제
            delOtherTab: function () {
                nthTabs.find(".nav-tabs li").not('[class="active"]').not('[not-allow-close]').remove();
                nthTabs.find(".tab-content div.tab-pane").not('[not-allow-close]').not('[class$="active"]').remove();
                nthTabs.find('.content-tabs-container').css("margin-left", 40); 
                return this;
            },

            // 모든 탭 삭제
            delAllTab: function () {
                this.delOtherTab();
                this.delTab();
                return this;
            },

            // 활동 탭 설정
            setActTab: function (tabId) {
                tabId = tabId == undefined ? methods.getActiveId() : tabId;
                tabId = tabId.indexOf('#') > -1 ? tabId : '#' + tabId;
                nthTabs.find('.active').removeClass('active');
                nthTabs.find("[href='" + tabId + "']").parent().addClass('active');
                nthTabs.find(tabId).addClass('active');
                return this;
            },

            // 스위치 탭
            toggleTab: function (tabId) {
                this.setActTab(tabId).locationTab(tabId);
                return this;
            },

            // 탭의 존재 여부 지정
            isExistsTab: function (tabId) {
                tabId = tabId.indexOf('#') > -1 ? tabId : '#' + tabId;
                return nthTabs.find(tabId).length>0;
            },

            // 탭 스위치 이벤트 핸들러
            tabToggleHandler: function(func){
                handler["tabToggleHandler"] = func;
            }
        };

        // 이벤트 처리
        var event = {

            // 창 변경
            onWindowsResize: function () {
                $(window).resize(function () {
                    settings.rollWidth = nthTabs.width() - 120;
                });
                return this;
            },
            // 위치 지정 탭
            onLocationTab: function () {
                nthTabs.on("click", '.tab-location', function () {
                    methods.locationTab();
                });
                return this;
            },

            // 탭 닫기 버튼
            onTabClose: function () {
                nthTabs.on("click", '.tab-close', function () {
                    var tabId = $(this).parent().find("a").attr('href');
                    // 현재 작업의 레이블 너비
                    var navTabOpt = nthTabs.find("[href='" + tabId + "']"); // 현재 작업 탭 객체
                    // 현재 탭 뒤에 탭이 있으면 처리되지 않으며, 그렇지 않으면 탭이 왼쪽으로 전체적으로 옵셋됩니다.
                    if(navTabOpt.parent().next().length == 0){
                        // 현재 작업 탭 앞에있는 모든 형제 탭의 너비 합계를 계산합니다.
                        var beforeTabsWidth = 0;
                        navTabOpt.parent().prevAll().each(function () {
                            beforeTabsWidth += $(this).width();
                        });
                        // 현재 작업 탭의 너비
                        var optTabWidth = navTabOpt.parent().width();
                        var margin_left_total = 40; // 기본 오프셋 (전체 너비가 스크롤 영역을 초과하지 않음)
                        // 탭 컨테이너 객체 가져 오기
                        var contentTab = navTabOpt.parent().parent().parent();
                        // 이 상황을 만족 시키려면 전체 왼쪽 오프셋 처리를 수행해야합니다.
                        if (beforeTabsWidth > settings.rollWidth) {
                            var margin_left_origin = contentTab.css('marginLeft').replace('px', '');
                            margin_left_total = parseFloat(margin_left_origin) + optTabWidth + 40;
                        }
                        contentTab.css("margin-left", margin_left_total);
                    }
                    methods.delTab(tabId);
                });
                return this;
            },

            // 현재 탭 작업 닫기
            onTabCloseOpt: function () {
                nthTabs.on("click", '.tab-close-current', function () {
                    methods.delTab();
                });
                return this;
            },

            // 다른 탭 닫기
            onTabCloseOther: function () {
                nthTabs.on("click", '.tab-close-other', function () {
                    methods.delOtherTab();
                });
                return this;
            },

            // 모든 탭 닫기
            onTabCloseAll: function () {
                nthTabs.on("click", '.tab-close-all', function () {
                    methods.delAllTab();
                });
                return this;
            },

            // 왼쪽 슬라이드 탭
            onTabRollLeft: function () {
                nthTabs.on("click", '.roll-nav-left', function () {
                    var contentTab = $(this).parent().find('.content-tabs-container');
                    var margin_left_total;
                    if (methods.getAllTabWidth() <= settings.rollWidth) {
                        // 눈에 보이는 영역을 넘어서 보이지 않고 미끄러질 수 없음
                        margin_left_total = 40;
                    }else{
                        var margin_left_origin = contentTab.css('marginLeft').replace('px', '');
                        margin_left_total = parseFloat(margin_left_origin) + methods.getMarginStep() + 40;
                    }
                    contentTab.css("margin-left", margin_left_total > 40 ? 40 : margin_left_total);
                });
                return this;
            },

            // 오른쪽 슬라이드 탭
            onTabRollRight: function () {
                nthTabs.on("click", '.roll-nav-right', function () {
                    if (methods.getAllTabWidth() <= settings.rollWidth) return false; // 눈에 보이는 영역을 넘어서 보이지 않고 미끄러질 수 없음
                    var contentTab = $(this).parent().find('.content-tabs-container');
                    var margin_left_origin = contentTab.css('marginLeft').replace('px', '');
                    var margin_left_total = parseFloat(margin_left_origin) - methods.getMarginStep();
                    if (methods.getAllTabWidth() - Math.abs(margin_left_origin) <= settings.rollWidth) return false; // 숨김이없고 스크롤 할 필요가 없습니다.
                    contentTab.css("margin-left", margin_left_total);
                });
                return this;
            },

            // 탭 목록
            onTabList: function () {
                nthTabs.on("click", '.right-nav-list', function () {
                    var tabList = methods.getTabList();
                    var html = [];
                    $.each(tabList, function (key, val) {
                        html.push('<li class="toggle-tab" data-id="' + val.id + '">' + val.title + '</li>');
                    });
                    nthTabs.find(".tab-list").html(html.join(''));
                });
                nthTabs.find(".tab-list-scrollbar").scrollbar();
                this.onTabListToggle();
                return this;
            },

            // 목록 아래의 탭 전환
            onTabListToggle: function () {
                nthTabs.on("click", '.toggle-tab', function () {
                    var tabId = $(this).data("id");
                    methods.setActTab(tabId).locationTab(tabId);
                });
                // 탭 목록 스크롤 막대를 클릭하여 다른 탭을 닫지 마세요.
                nthTabs.on('click','.scroll-element',function (e) {
                    e.stopPropagation();
                });
                return this;
            },

            // 탭 전환 이벤트
            onTabToggle: function(){
                nthTabs.on("click", '.nav-tabs li', function () {
                    if (delflag == false) {
                        // 이전에 열려있던 대메뉴 닫기
                        var _objIdPrev = methods.getActiveId();
                        var $parentPrev = $('#left-menu').find('a[data-objid="'+_objIdPrev+'"]').closest('li.has_sub');
                        $parentPrev.removeClass('open');
                        $parentPrev.children('ul').hide();
                        // 현재선택한 탭이 포함된 대메뉴 열기
                        var _objId = $(this).children('a').prop('hash').replace('#','');
                        var $parentLi = $('#left-menu').find('a[data-objid="'+_objId+'"]').closest('li.has_sub');
                        $parentLi.addClass('open');
                        $parentLi.children('ul').show();
                    } else {
                        var $parentLi = $('#left-menu').find('a[data-objid="'+methods.getActiveId()+'"]').closest('li.has_sub');
                        $parentLi.addClass('open');
                        $parentLi.children('ul').show();
                    }
                    delflag = false;
                	
                    var lastTabText = nthTabs.find(".nav-tabs li a[href='#"+methods.getActiveId()+"'] span").text();
                    handler.hasOwnProperty("tabToggleHandler") && handler["tabToggleHandler"]({
                        last:{
                            tabId:methods.getActiveId(),
                            tabText:lastTabText
                        },
                        active:{
                            tabId:$(this).find("a").attr("href").replace('#',''),
                            tabText:$(this).find("a span").text()
                        }
                    });
                });
            }
        };
        return run();
    }
})(jQuery);
