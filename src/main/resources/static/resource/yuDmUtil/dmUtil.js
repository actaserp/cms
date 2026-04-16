var _imgData = [];
var _imgDataIndex = 0;
var TdmUtils = {
  doImgPop: function (_obj){
    img1= new Image();
    img1.src=$(_obj).attr('src');
    TdmUtils.imgControll(img1.src);
  },
  imgControll: function (img){
    if((img1.width!=0)&&(img1.height!=0)){
      TdmUtils.viewImage(img);
    }
    else{
      controller="imgControll('"+img+"')";
      intervalID=setTimeout(controller,20);
    }
  },
  viewImage: function (img){
    W=img1.width;
    H=img1.height;
    O="width="+W+",height="+H+",scrollbars=yes";
    imgWin=window.open("","",O);
    imgWin.document.write("<html><head><title>=========== 이미지상세보기 ===========</title></head>");
    imgWin.document.write("<body topmargin=0 leftmargin=0>");
    imgWin.document.write("<img src="+img+" onclick='self.close()' style='cursor:pointer;' title ='클릭하시면 창이 닫힙니다.'>");
    imgWin.document.close();
  },
  resetPaste: function () {
    $('.imgpaste').hide();
    $('.imgpasteresult').hide();
    _imgData = [];
    _imgDataIndex = 0;
    $('#btn_del').prop('disabled', true);
    $('#btn_save').prop('disabled', true);
  },
  addPaste: function () {
    $('.imgpaste').show();
    $('.imgpasteresult').remove();
    _imgData = [];
    _imgDataIndex = 0;
    $('#btn_del').prop('disabled', true);
    $('#btn_save').prop('disabled', false);
  },
  removeIamge: function (_obj) {
    var $obj = $(_obj);
    if($obj.data('dtype') === 'paste') {
      var _idx = _imgData.indexOf($obj.children('img').attr('src').replace(';base64,',';base64_'));
      _imgData.splice(_idx,1);
      $obj.remove();
    } else {
      if(confirm('이미지를 삭제하시겠습니까?')) {
        $.delete('/devtool/dm/showimage/del/' + $obj.data('attachcd'), function() {
          Notify.success('삭제되었습니다.'); // Notification
          $obj.remove();
        }).fail(function(e) {
          Notify.error(JSON.parse(e.responseText).message);
        });
      }
    }
  },
  clearPaste: function (_tdmCd, _isOwner) {
    $('.imgpaste').show();
    $('.imgpasteresult').remove();
    $.get('/devtool/dm/showimage/' + _tdmCd, function(data) {
      $.each(data, function() {
        $('<div class="imgpasteresult" data-dtype="load" data-attachcd="'+this.attachCd+'" id="load_'+this.attachCd+'">' + (_isOwner == '1' ? '<a href="javascript:TdmUtils.removeIamge(load_'+this.attachCd+');" class="closedm-thin"></a>' : '') + '<img src="' + this.attachFile.replace(';base64_',';base64,') +'" width="100px" onclick="TdmUtils.doImgPop(this);"></div>').insertAfter($('.imgpaste'));
      });
    }).fail(function(e) {
      Notify.error(JSON.parse(e.responseText).message);
    });

    _imgData = [];
    _imgDataIndex = 0;
  },
  viewPaste: function (_tdmCd) {
    $('.imgpasteresult').remove();
    $.get('/devtool/dm/showimage/' + _tdmCd, function(data) {
      $.each(data, function() {
        $('<div class="imgpasteresult" data-dtype="load" data-attachcd="'+this.attachCd+'" id="load_'+this.attachCd+'">' + '<img src="' + this.attachFile.replace(';base64_',';base64,') +'" width="100px" onclick="TdmUtils.doImgPop(this);"></div>').insertAfter($('.imgfiles'));
      });
    }).fail(function(e) {
      Notify.error(JSON.parse(e.responseText).message);
    });
  },
  preLoadFile: function(_upfiles) {
    var file = document.querySelector('input[type=file]').files[0];
    if(file.size > 4194304) {
        Alert.alert('', '4MB 이하의 이미지만 업로드 가능합니다. <br>(선택이미지:' + TdmUtils.bytesToSize(file.size) + ')');
        $('input[type=file]').val('');
        return false;
    }
    if(file.type !== 'image/png' && file.type !== 'image/jpeg' && file.type !== 'image/gif') {
        Alert.alert('', 'png/jpg/gif 파일만 업로드 가능합니다.');
        $('input[type=file]').val('');
        return false;
    }

    var reader  = new FileReader();
    reader.readAsDataURL(file);
    reader.addEventListener("load", function () {
      if (file) {
        _imgData.push(reader.result.replace(';base64,',';base64_'));
        $('<div class="imgpasteresult" data-dtype="paste" id="paste_'+_imgDataIndex+'"><a href="javascript:TdmUtils.removeIamge(paste_'+_imgDataIndex+');" class="closedm-thin"></a><img src="' + reader.result +'" width="100px" onclick="TdmUtils.doImgPop(this);"></div>').insertAfter($('.imgpaste'));
        _imgDataIndex++;
      }
    }, false);
  },
  bytesToSize: function(bytes) {
	    var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
	    if (bytes == 0) return 'n/a';
	    var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
	    return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
  }
};
