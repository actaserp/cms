// //
//
// let MenuUtils = {
//     addLog: function (gui_code) {
//
//     },
// };
// // LBN 메뉴 처리 script (default layout 사용)
// // 메뉴 세팅, 북마크, 클릭 이벤트 등
// var addMenu = function (_nodeData) {
//     var strtxt = '';
//     for (var i = 0; i < _nodeData.length; i++) {
//
//         let node = _nodeData[i];
//         var litag = '<li class="has_sub"><a href="#">';
//         if (node.nodes.length == 0 && (node.objUrl != '')) {
//             litag = '<li class="documents"><a href="#" data-manual="' + node.ismanual + '" data-bookmark="' + node.isbookmark + '" data-objid="' + node.objId + '" menuurl="#">';
//         }
//         if (node.menuIconCls || null) {
//             litag += '<i class="fas ' + node.menuIconCls + '"></i>';
//         }
//         litag += ' ' + i18n.getMenuText(node.objNm);
//         litag += '</a>';
//         strtxt += litag;
//
//         if (node.nodes.length > 0) {
//             var litag2 = '';
//             litag2 += '<ul class="documents">';
//             for (var j = 0; j < node.nodes.length; j++) {
//                 let subnode = node.nodes[j];
//                 if (subnode.menuDepth == '2' || subnode.menuDepth == '3') {
//                     if (subnode.nodes.length > 0) {
//                         litag2 += '<li class="has_sub2">';
//                     }
//                     else {
//                         litag2 += '<li>';
//                     }
//                     litag2 += '<a href="#" data-manual="' + subnode.ismanual
//                         + '" data-bookmark="' + subnode.isbookmark
//                         + '" data-objid="' + subnode.objId
//                         + '" menuurl="' + ((subnode.objUrl == '') ? '#' : subnode.objUrl) + '">' + i18n.getMenuText(subnode.objNm) + '</a>';
//                     if (subnode.nodes.length > 0) {
//                         litag2 += '<ul class="documents">';
//                         for (var z = 0; z < subnode.nodes.length; z++) {
//                             litag2 += '<li><a href="#" data-manual="' + subnode.nodes[z].ismanual
//                                 + '" data-bookmark="' + subnode.nodes[z].isbookmark
//                                 + '" data-objid="' + subnode.nodes[z].objId
//                                 + '" menuurl="' + ((subnode.nodes[z].objUrl == '') ? '#' : subnode.nodes[z].objUrl) + '">' + i18n.getMenuText(subnode.nodes[z].objNm) + '</a></li>';
//                         }
//                         litag2 += '</ul>';
//                         litag2 += '</li>';
//                     }
//                     else {
//                         litag2 += '</li>';
//                     }
//                 }
//             }
//             litag2 += '</ul>';
//             strtxt += litag2;
//         }
//         strtxt += '</li>';
//     }
//
//     $('#left-menu').html(strtxt);
//
//     // 이벤트 바인딩
//     $('.documents a').off("click").on('click', function (e) {
//         e.preventDefault();
//
//         var val = $(this).text();
//         var anum = $(this).attr('number');
//         var menuurl = $(this).attr('menuurl');
//         var objid = $(this).attr('data-objid');
//         var _bookmark = $(this).attr('data-bookmark');
//         var _manual = $(this).attr('data-manual');
//         if (menuurl != '#') {
//             // 메뉴링크가 있을경우에만
//             if (nthTabs.isExistsTab('#' + objid)) {
//                 nthTabs.toggleTab('#' + objid);
//             } else {
//                 nthTabs.addTab({ id: objid, title: val, url: menuurl, active: true, allowClose: true, ismanual: _manual });
//                 // menu_log insert
//             }
//
//             /* 탭 추가 생성시 북마크아이콘 추가*/
//             if (!$("[href*=#" + objid + "]").find("i").hasClass("fa-star")) {
//                 if ($('#bookmark-menu a[data-objid=' + objid + ']').length == 1) {
//                     _bookmark = 'true';
//                 } else {
//                     _bookmark = 'false';
//                 }
//                 $("[href*=#" + objid + "]").prepend("<i class='fas fa-star" + (_bookmark == 'true' ? ' bookmark' : '') + "'></i>");
//                 $(".nav-tabs .fa-star").off('click').on('click', function (e) {
//                     $(this).toggleClass("bookmark");
//                     fnBookMarkSave($(this).parent('a').prop('hash').replace('#', ''), $(this).hasClass('bookmark'));
//                 });
//             }
//
//             if ($(this).parents('.has_sub2').length > 0) {
//                 $('.gnb ul li a').removeClass('on');
//                 $(this).addClass('on');
//             } else {
//                 $('li.has_sub2').removeClass('open');
//                 $('li.has_sub2 > ul').slideUp(200);
//                 $('.gnb ul li a').removeClass('on');
//                 $(this).addClass('on');
//             }
//
//             if ($(window).innerWidth() < 1024) {
//                 $('body').addClass('open');
//                 $('.wrap').addClass('menu_close');
//                 $('.con_area').addClass('open');
//             }
//         }
//     });
//
//   // 메뉴
//   $('.gnb li.active').addClass('open').children('ul').show();
//   $('.gnb li.has_sub>a').on('click', function () {
//       $(this).removeAttr('href');
//       var element = $(this).parent('li');
//       if (element.hasClass('open')) {
//           element.removeClass('open');
//           element.find('li').removeClass('open');
//           element.find('ul').slideUp(200);
//       } else {
//           element.addClass('open');
//           element.children('ul').slideDown(200);
//           element.siblings('li').children('ul').slideUp(200);
//           element.siblings('li').removeClass('open');
//           element.siblings('li').find('li').removeClass('open');
//           element.siblings('li').find('ul').slideUp(200);
//       }
//   });
//   $('.has_sub2>a').on('click', function () {
// 	  $('.documents > li > a').removeClass('on');
//       $(this).removeAttr('href');
//       var element = $(this).parent('li');
//       if (element.hasClass('open')) {
//           element.removeClass('open');
//           element.find('li').removeClass('open');
//           element.find('ul').slideUp(200);
//       } else {
//           element.addClass('open');
//           element.children('ul').slideDown(200);
//           element.siblings('li').children('ul').slideUp(200);
//           element.siblings('li').removeClass('open');
//           element.siblings('li').find('li').removeClass('open');
//           element.siblings('li').find('ul').slideUp(200);
//       }
//   });
//
// };
//
// var addMenuBookmark = function () {
//
//     // 즐겨찾기 조회
//
//     $.getJSON('/api/system/bookmark', function (result) {
//         let _data = result.data
//         var litagbook = '';
//         for (var i = 0; i < _data.length; i++) {
//             litagbook += '<li><a href="#" data-manual="' + _data[i].ismanual + '" data-objid="' + _data[i].code + '" menuurl="' + _data[i].url + '"><i class="star_icon"></i>' + i18n.getMenuText(_data[i].name) + '</a></li>';
//         }
//         $('#bookmark-menu').html(litagbook);
//
//         $('#bookmark-menu a').on('click', function (e) {
//             var val = $(this).text();
//             var menuurl = $(this).attr('menuurl');
//             var objid = $(this).attr('data-objid');
//             var _manual = $(this).attr('data-manual');
//             if (nthTabs.isExistsTab('#' + objid)) {
//                 nthTabs.toggleTab('#' + objid);
//             } else {
//                 nthTabs.addTab({
//                     id: objid,
//                     title: val,
//                     url: menuurl,
//                     active: true,
//                     allowClose: true,
//                     ismanual: _manual
//                 });
//                 // menu_log insert
//             }
//             /* 탭 추가 생성시 북마크아이콘 추가*/
//             if (!$("[href*=#" + objid + "]").find("i").hasClass("fa-star")) {
//                 $("[href*=#" + objid + "]").prepend("<i class='fas fa-star bookmark'></i>");
//                 $(".nav-tabs .fa-star").off('click').on('click', function (e) {
//                     $(this).toggleClass("bookmark");
//                     fnBookMarkSave($(this).parent('a').prop('hash').replace('#', ''), $(this).hasClass('bookmark'));
//                 });
//             }
//         });
//     }).fail(function (e) {
//         Notify.error('즐겨찾기 메뉴 생성에 실패하였습니다.');
//     });
// }
//
// // 북마크 저장
// var fnBookMarkSave = function (_objId, _isbookmark) {
//     let csrf = $('[name=_csrf]').val();
//     let param_data = {
//         'menucode': _objId,
//         isbookmark: _isbookmark,
//         '_csrf': csrf
//     };
//     $.post('/api/system/bookmark/save', param_data, function (data) {
//         if (_isbookmark) {
//             $('.documents a[data-objid="' + _objId + '"]').attr('data-bookmark', 'true');
//         } else {
//             $('.documents a[data-objid="' + _objId + '"]').attr('data-bookmark', 'false');
//         }
//         addMenuBookmark();
//     }).fail(function (e) {
//         console.log('fnBookMarkSave error', e.message);
//     });
// }
//
// // 대쉬보드에서 탭 북마크확인을 위한 호출함수
// var fnCheckTabBookMark = function (objid, _bookmark) {
//     if (!$("[href*=#" + objid + "]").find("i").hasClass("fa-star")) {
//         $("[href*=#" + objid + "]").prepend("<i class='fas fa-star" + (_bookmark == 'true' ? ' bookmark' : '') + "'></i>");
//         $(".nav-tabs .fa-star").off('click').on('click', function (e) {
//             $(this).toggleClass("bookmark");
//             fnBookMarkSave($(this).parent('a').prop('hash').replace('#', ''), $(this).hasClass('bookmark'));
//         });
//     }
// }
//
// var menuLink = function(objid, val, menuurl, manual) {
//         if (menuurl != '#') {
//             // 메뉴링크가 있을경우에만
//             if (nthTabs.isExistsTab('#' + objid)) {
//                 nthTabs.toggleTab('#' + objid);
//             } else {
//                 nthTabs.addTab({ id: objid, title: val, url: menuurl, active: true, allowClose: true, ismanual: manual });
//                 // menu_log insert
//             }
//         }
//
// 		let $href = $("[href=#" + objid + "]");
//
// 		/* 탭 추가 생성시 북마크아이콘 추가*/
//     	if (!$href.find("i").hasClass("fa-star")) {
// 	        if ($('#bookmark-menu a[data-objid=' + objid + ']').length == 1) {
// 	            _bookmark = 'true';
// 	        } else {
// 	            _bookmark = 'false';
// 	        }
// 	        $href.prepend("<i class='fas fa-star" + (_bookmark == 'true' ? ' bookmark' : '') + "'></i>");
//
// 	        $(".nav-tabs .fa-star").off('click').on('click', function (e) {
// 	            $(this).toggleClass("bookmark");
// 	            fnBookMarkSave($(this).parent('a').prop('hash').replace('#', ''), $(this).hasClass('bookmark'));
// 	        });
//    	 }
// }
//
// $(document).ready(function () {
//     // 메뉴 조회
//     $.getJSON('/api/system/menus', function (datas) {
//         addMenu(datas.data);
//     }).fail(function (e) {
//         Notify.error('메뉴 생성에 실패하였습니다.');
//         //Notify.error(getMessage('error.E000000032'));
//     });
//     // 즐겨찾기 조회
//     addMenuBookmark();
//
//     $(".nav-tabs .fa-star").off('click').on('click', function (e) {
//         $(this).toggleClass("bookmark");
//         fnBookMarkSave('${targetObjId}', $(this).hasClass('bookmark'));
//     });
//
//     /*///// 버튼클릭시 메뉴활성화 스크립트 /////*/
//     $(".btn_menu_list").click(function () {
//         $(this).addClass("active");
//         $(".btn_mymenu_list").removeClass("active");
//         $(".menu:eq(0)").removeClass("hide");
//         $(".menu:eq(1)").addClass("hide");
//     });
//     $(".btn_mymenu_list").click(function () {
//         $(this).addClass("active");
//         $(".btn_menu_list").removeClass("active");
//         $(".menu:eq(1)").removeClass("hide");
//         $(".menu:eq(0)").addClass("hide");
//         $(".has_sub").removeClass("open");
//         $(".has_sub ul").css({ "display": "none" });
//     });
// });