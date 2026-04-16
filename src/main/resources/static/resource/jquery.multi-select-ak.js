// AK몰용 멀티셀렉터 플러그인

;(function($) {

  "use strict";

  var pluginName = "multiSelect",
    defaults = {
      'containerHTML': '<div class="multi-select-container">',
      'menuHTML': '<div class="multi-select-menu">',
      'buttonHTML': '<span class="multi-select-button">',
      'menuItemsHTML': '<div class="multi-select-menuitems">',
      'menuItemHTML': '<label class="multi-select-menuitem">',
      'presetsHTML': '<div class="multi-select-presets">',
      'modalHTML': undefined,
      'menuItemTitleClass': 'multi-select-menuitem--titled',
      'activeClass': 'multi-select-container--open',
      'noneText': '선택',
      'allText': undefined,
      'presets': undefined,
      'positionedMenuClass': 'multi-select-container--positioned',
      'positionMenuWithin': undefined,
      'viewportBottomGutter': 20,
      'menuMinHeight': 200
    };

  /**
   * @constructor
   */
  function MultiSelect(element, options) {
    this.element = element; // 멀티셀렉터 구현하고자 하는 실제 select
    this.$element = $(element);
    this.settings = $.extend( {}, defaults, options );
    this._defaults = defaults;
    this._name = pluginName;
    this.init();
  }

  function arraysAreEqual(array1, array2) {
    if ( array1.length != array2.length ){
      return false;
    }

    array1.sort();
    array2.sort();

    for ( var i = 0; i < array1.length; i++ ){
      if ( array1[i] !== array2[i] ){
        return false;
      }
    }

    return true;
  }

  $.extend(MultiSelect.prototype, {
	// 초기화 및 멀티셀렉터 구성  
    init: function() {
      
      this.checkSuitableInput();
      this.findLabels();
      this.constructContainer();
      this.constructButton();
      this.constructMenu();
      this.constructModal();

      this.setUpBodyClickListener();
      this.setUpLabelsClickListener();

      this.$element.hide();
    },

    // 멀티셀렉터인지 확인
    checkSuitableInput: function(text) {
      if ( this.$element.is('select[multiple]') === false ) {
        throw new Error('$.multiSelect only works on <select multiple> elements');
      }
    },
    // label for 속성을 구성할 기본 문자열 설정 : 해당 select id 속성을 설정
    findLabels: function() {
      this.$labels = $('label[for="' + this.$element.attr('id') + '"]');
    },
    // button,menu/menuitems/menuitem Role을 포함하는 tag생성 (div) 
    constructContainer: function() {
      this.$container = $(this.settings['containerHTML']);
      this.$element.data('multi-select-container', this.$container);
      this.$container.insertAfter(this.$element);
    },
    // 메뉴 보이기/감추기, 현재 선택된 항목 건수를 보여주는  tag생성 (span) 
    constructButton: function() {
      var _this = this;
      this.$button = $(this.settings['buttonHTML']);
      this.$button.attr({
        'role': 'button',
        'aria-haspopup': 'true',
        'tabindex': 0,
        'aria-label': this.$labels.eq(0).text()
      })
      .on('keydown.multiselect', function(e) { //event namespaces 
        var key = e.which;
        var returnKey = 13;
        var escapeKey = 27;
        var spaceKey = 32;
        var downArrow = 40;
        if ((key === returnKey) || (key === spaceKey)) { // 리턴키나 스페이스키일 경우 button span 태그 클릭
          e.preventDefault();
          _this.$button.click();
        } else if (key === downArrow) { // 아래화살표일 경우는 menu 보여주기
          e.preventDefault();
          _this.menuShow();
          var group = _this.$presets || _this.$menuItems; // 프리셋 또는 menuitems 가 있으면 그 중 첫번째 자식에 포커스
          group.children(":first").focus();
        } else if (key === escapeKey) { // esc 키일 경운ㄴ menu 감추기
          _this.menuHide();
        }
      }).on('click.multiselect', function(e) { // 클릭할 경우는 메뉴 토글 (감추기/보이기)
        _this.menuToggle();
      })
      .appendTo(this.$container); // 컨테이너 div 태그 안에 span 태그로 추가

      this.$element.on('change.multiselect', function() { // 오리지널 select에 변경이 이루어지면 button 이 표시하는 값 변경
        _this.updateButtonContents();
      });

      this.updateButtonContents();
    },
    
    // 현재 선택된 항목 건수를 보여주는 작업 처리
    updateButtonContents: function() {
      var _this = this;
      var options = [];
      var selected = [];

      // 오리지널 select 에서 선택된 option text 모음
      this.$element.find('option').each(function() {
        var text = /** @type string */ ($(this).text());
        options.push(text);
        if ($(this).is(':selected')) {
          selected.push( $.trim(text) );
        }
      });

      this.$button.empty(); // 현재 표시값 삭제

      if (selected.length == 0) { // 선택된 option이 없으면 noneText 로 설정한 값 보이기
        this.$button.text( this.settings['noneText'] );
      } else if ( (selected.length === options.length) && this.settings['allText']) { // option 전체가 선택되면 allText 로 설정한 값 보이기
        this.$button.text( this.settings['allText'] );
      } else { // 현재 선택된 항목 건수 보이기
        //this.$button.text( selected.join(', ') );
//    	  this.$button.text(selected.length + " 항목 선택");  
    	  this.$button.text(selected.length + " Item Selected");  
      }
    },

    // menuitems/menuitem Role을 포함하는 tag생성 (div) 
    constructMenu: function() {
      var _this = this;

      this.$menu = $(this.settings['menuHTML']);
      this.$menu.attr({
        'role': 'menu'
      }).on('keyup.multiselect', function(e){
        var key = e.which; // event.keyCode 와 동일 (jquery에서 사용)
        var escapeKey = 27;
        if (key === escapeKey) { // esc 키일 경우에는 메뉴 숨기고 포커스를 버튼으로 이동
          _this.menuHide();
          _this.$button.focus();
        }
      })
      .appendTo(this.$container); // 컨테이너 div 태그 안에 div 태그로 추가

      this.constructMenuItems(); // memnuitems Role을 가진 태그 생성

      if ( this.settings['presets'] ) { // 프리셋이 있을 경우 프리셋 구성 태크 생성
        this.constructPresets();
      }
    },
    
    // menuitem 들을 가지는 태그 생성 (div)
    constructMenuItems: function() { // 
      var _this = this;

      this.$menuItems = $(this.settings['menuItemsHTML']);
      this.$menu.append(this.$menuItems); // menu Role을 가진 div 태그 안에 삽입
      // 오리지널 select 태그에서 변경이 이루어지면
      this.$element.on('change.multiselect', function(e, internal) { 
        // Don't need to update the menu items if this
        // change event was fired by our tickbox handler.
        if(internal !== true){
          _this.updateMenuItems();
        }
      });

      this.updateMenuItems();
    },

    updateMenuItems: function() {
      var _this = this;
      this.$menuItems.empty();

      this.$element.children('optgroup,option').each(function(index, element) {
        var $item;
        if (element.nodeName === 'OPTION') {
          $item = _this.constructMenuItem($(element), index);
          _this.$menuItems.append($item);
        } else { // optgroup 일 경우 그룹핑
          _this.constructMenuItemsGroup($(element), index);
        }
      });
    },

    // optgroup 으로 그룹생성
    constructMenuItemsGroup: function($optgroup, optgroup_index) {
      var _this = this;

      $optgroup.children('option').each(function(option_index, option) {
        var cls = _this.settings['menuItemTitleClass'];
        
        if (option_index === 0) { // 0인 경우는 그룹
          // 그룹타이틀 생성
    	  var unique_id = _this.$element.attr('name') + '_' + optgroup_index;
          var $gitem = $(_this.settings['menuItemHTML'])
            .attr({
              'for': unique_id,
              'role': 'menuitem'
            });
          
          $gitem.on('click.multiselect', function(){
          	var title = $(this).attr("data-group-title");
          	$("label.multi-select-menuitem").each(function(index,_item){
          		if($(_item).attr("data-group-title") === title ){
          			$(_item).children('input').click();
          		}
          	});
          });
            
	      $gitem.addClass(cls).attr('data-group-title', $optgroup.attr('label'));
	      _this.$menuItems.append($gitem);
          
	      // 자식 생성
          cls += 'sr';
          var $item = _this.constructMenuItem($(option), optgroup_index + '_' + option_index);
          $item.addClass(cls).attr('data-group-title', $optgroup.attr('label'));
          _this.$menuItems.append($item);
        }
        else{
	      cls += 'sr';
	      var $item = _this.constructMenuItem($(option), optgroup_index + '_' + option_index);
	      $item.addClass(cls).attr('data-group-title', $optgroup.attr('label'));
	      _this.$menuItems.append($item);
        }
      });
    },

    // option 에 해당하는 menuitem 생성
    constructMenuItem: function($option, option_index) {
      var unique_id = this.$element.attr('name') + '_' + option_index;
      var $item = $(this.settings['menuItemHTML'])
        .attr({
          'for': unique_id,
          'role': 'menuitem'
        })
        .on('keydown.multiselect', this.upDown.bind(this, 'menuitem'))
        .text(' ' + $option.text());

      var $input = $('<input>')
        .attr({
          'type': 'checkbox',
          'id': unique_id,
          'value': $option.val()
        })
        .prependTo($item);

      if ( $option.is(':disabled') ) {
        $input.attr('disabled', 'disabled');
      }
      if ( $option.is(':selected') ) {
        $input.prop('checked', 'checked');
      }

      $input.on('change.multiselect', function() {
        if ($(this).prop('checked')) {
          $option.prop('selected', true);
        } else {
          $option.prop('selected', false);
        }

        // .prop() on its own doesn't generate a change event.
        // Other plugins might want to do stuff onChange.
        $option.trigger('change', [true]);
      });

      return $item;
    },
    
    // 화살표 키로 이동 (업/다운) 이벤트 정의
    upDown: function(type, e) {
    var key = e.which;
    var upArrow = 38;
    var downArrow = 40;

    if (key === upArrow) {
      e.preventDefault();
      var prev = $(e.currentTarget).prev();
      if (prev.length) {
        prev.focus();
      } else if (this.$presets && type === 'menuitem') {
        this.$presets.children(':last').focus();
      } else {
        this.$button.focus();
      }
    } else if (key === downArrow) {
      e.preventDefault();
      var next = $(e.currentTarget).next();
      if (next.length || type === 'menuitem') {
        next.focus();
      } else {
        this.$menuItems.children(':first').focus();
      }
    }
  },

  //------------------------------ 프리셋 설정 내용 시작 -----------------------------------------------
    // 프리셋 생성 및 설정
    constructPresets: function() {
      var _this = this;
      this.$presets = $(this.settings['presetsHTML']);
      this.$menu.prepend(this.$presets);

      $.each(this.settings['presets'], function(i, preset){
        var unique_id = _this.$element.attr('name') + '_preset_' + i;
        var $item = $(_this.settings['menuItemHTML'])
          .attr({
            'for': unique_id,
            'role': 'menuitem'
          })
          .text(' ' + preset.name)
          .on('keydown.multiselect', _this.upDown.bind(_this, 'preset'))
          .appendTo(_this.$presets);

        var $input = $('<input>')
          .attr({
            'type': 'radio',
            'name': _this.$element.attr('name') + '_presets',
            'id': unique_id
          })
          .prependTo($item);

        $input.on('change.multiselect', function(){
          _this.$element.val(preset.options);
          _this.$element.trigger('change');
        });
      });

      this.$element.on('change.multiselect', function() {
        _this.updatePresets();
      });

      this.updatePresets();
    },

    updatePresets: function() {
        var _this = this;

        $.each(this.settings['presets'], function(i, preset){
          var unique_id = _this.$element.attr('name') + '_preset_' + i;
          var $input = _this.$presets.find('#' + unique_id);

          if ( arraysAreEqual(preset.options || [], _this.$element.val() || []) ){
            $input.prop('checked', true);
          } else {
            $input.prop('checked', false);
          }
        });
      },
      
    constructModal: function() {
      var _this = this;

      if (this.settings['modalHTML']) {
        this.$modal = $(this.settings['modalHTML']);
        this.$modal.on('click.multiselect', function(){
          _this.menuHide();
        })
        this.$modal.insertBefore(this.$menu);
      }
    },
  //------------------------------ 프리셋 설정 내용 끝 -----------------------------------------------

    setUpBodyClickListener: function() {
      var _this = this;

      // Hide the $menu when you click outside of it.
      $('html').on('click.multiselect', function(){
        _this.menuHide();
      });

      // Stop click events from inside the $button or $menu from
      // bubbling up to the body and closing the menu!
      this.$container.on('click.multiselect', function(e){
        e.stopPropagation();
      });
    },

    setUpLabelsClickListener: function() {
      var _this = this;
      this.$labels.on('click.multiselect', function(e) {
        e.preventDefault();
        e.stopPropagation();
        _this.menuToggle();
      });
    },

    menuShow: function() {
      $('html').trigger('click.multiselect'); // Close any other open menus
      this.$container.addClass(this.settings['activeClass']);

      if ( this.settings['positionMenuWithin'] && this.settings['positionMenuWithin'] instanceof $ ) {
        var menuLeftEdge = this.$menu.offset().left + this.$menu.outerWidth();
        var withinLeftEdge = this.settings['positionMenuWithin'].offset().left +
          this.settings['positionMenuWithin'].outerWidth();

        if ( menuLeftEdge > withinLeftEdge ) {
          this.$menu.css( 'width', (withinLeftEdge - this.$menu.offset().left) );
          this.$container.addClass(this.settings['positionedMenuClass']);
        }
      }

      var menuBottom = this.$menu.offset().top + this.$menu.outerHeight();
      var viewportBottom = $(window).scrollTop() + $(window).height();
      if ( menuBottom > viewportBottom - this.settings['viewportBottomGutter'] ) {
        this.$menu.css({
          'maxHeight': Math.max(
            viewportBottom - this.settings['viewportBottomGutter'] - this.$menu.offset().top,
            this.settings['menuMinHeight']
          ),
          'overflow': 'scroll'
        });
      } else {
        this.$menu.css({
          'maxHeight': '',
          'overflow': ''
        });
      }
    },

    menuHide: function() {
      this.$container.removeClass(this.settings['activeClass']);
      this.$container.removeClass(this.settings['positionedMenuClass']);
      this.$menu.css('width', 'auto');
      // maxHeight 버그 패치
      this.$menu.css({
          'maxHeight': '',
          'overflow': ''
        });
    },

    menuToggle: function() {
      if ( this.$container.hasClass(this.settings['activeClass']) ) {
        this.menuHide();
      } else {
        this.menuShow();
      }
    }

  });

  $.fn[ pluginName ] = function(options) {
    return this.each(function() {
      // 존재하지 않는 다면 초기화
      if ( !$.data(this, "plugin_" + pluginName) ) {
        $.data(this, "plugin_" + pluginName,
          new MultiSelect(this, options) );
      }
      // 이미 초기화 했다면 삭제 후 다시 생성
      else{
    	// 태그 삭제
	    $(this).next(".multi-select-container").remove();
	    // 기존 초기화 객체 삭제
	    $.removeData(this, "plugin_" + pluginName);
	    // 신규 초기화
    	$.data(this, "plugin_" + pluginName,
    	          new MultiSelect(this, options) );
      }
    });
  };

})(jQuery);
