'use strict';
var UPLOAD;
var UPLOAD2;
var UPLOAD3;
var YuFileUploader = {
    init: function () {
        $.fn.yuFileUploader = function (_config) {
            var $target = $(this);
            var defaultconfig = {
                title: '파일업로드',
                type: 'date', // date(월별저장), default(날짜가 아닌 others의 경로대로 저장)
                others: 'test', // 저장폴더 상위경로(회사코드)
                filepath: '', // 저장폴더 하위경로
                fileSize: '1', // 1회 최대업로드 용량
                multiple: true,  // 멑티업로드 허용여부
                maxcnt: 10, // 업로드 최대갯수
                accept: '*/*', // 허용 확장자 image/* .gif,.jpg,.png
                accepts: '',    // 드래그로 업로드할 때 체크할 용도
                dropZone: '',
                boxZone: 'fileuploadAx',
                fileIdsCtl: '#hid_fileIds',
                removeAllCtl: '#removeFileAll',
                downloadAllCtl: '#downloadFileAll',
                attachurl: '',
                icons: {},
                emptymsg: '',
                addfileext: 'N',
                uploadCallback: function (_params) { },
                deleteCallback: function (_params) { },
                uploadsxn: 1, // 업로드객체번호 (1: UPLOAD, 2: UPLOAD2, 3: UPLOAD3)
                tableName: 'test',
                attachName: 'basic', // 2021-04-06 업무룰로 인한 추가
                thumbnailYN: 'N',  // 썸네일파일 저장시 Y
            }
            _config.others = _config.others + (_config.filepath == '' ? '' : ((_config.others == '' ? '' : ',') + _config.filepath));

            let fn_success = function (res) {
                var myconfig = $.extend(true, {}, defaultconfig, _config);
                var _supportedHTML5_emptyListMsg = '<div class="text-center" style="padding: 10px;"><em class="fa fa-upload text-muted uploadIcon"></em><br>' + myconfig.emptymsg + '</div>';
                var DIALOG = new ax5.ui.dialog({
                    title: '파일업로드'
                });

                $target.children('span.fileuploadTitle').html('<span>첨부파일</span> [' + _config.accept + '] [1개당 최대Size: ' + _config.fileSize + 'MB, 최대파일수: ' + _config.maxcnt + '개]');
                if (myconfig.dropZone != '') {
                } else {
                    _supportedHTML5_emptyListMsg = '<div class="text-center" style="padding: 10px;">' + myconfig.emptymsg + '</div>';
                }

                let _uri = '/api/files/upload/file_upload?tableName=' + myconfig.tableName;
                _uri += '&attachName=' + myconfig.attachName;
                _uri += '&accepts=' + myconfig.accepts;
                _uri += '&fileSize=' + myconfig.fileSize;
                _uri += '&addfileext=' + myconfig.addfileext;
                _uri += '&type = ' + myconfig.type;
                _uri += '&others=' + myconfig.others;
                _uri += '&thumbnailYN=' + myconfig.thumbnailYN;

                try {

                    if (myconfig.uploadsxn == 1) {
                        if (UPLOAD) {
                            UPLOAD = null;  //작동 안 됨.
                        }
                        UPLOAD = new ax5.ui.uploader({
                            debug: true,
                            target: $target,
                            form: {
                                action: encodeURI(_uri),
                                fileName: 'uploadfile'
                            },
                            accept: myconfig.accept,
                            multiple: myconfig.multiple,
                            manualUpload: false,
                            progressBox: true,
                            progressBoxDirection: 'left',
                            dropZone: {
                                target: $('[data-uploaded-box="' + myconfig.dropZone + '"]')
                            },
                            uploadedBox: {
                                target: $('[data-uploaded-box="' + myconfig.boxZone + '"]'),
                                icon: myconfig.icons,
                                columnKeys: {
                                    name: 'fileNm',
                                    type: 'fileExt',
                                    size: 'fileSize',
                                    //uploadedName: 'fileNm',
                                    uploadedName: 'fileNm',
                                    uploadedPath: '',
                                    downloadPath: '',
                                    //previewPath: 'C:\\Temp\\mes21\\calib_resut\\11cb173c-37ca-43c5-92ea-48dc22847303.png',
                                    previewPath: '',
                                    thumbnail: ''
                                },
                                lang: {
                                    supportedHTML5_emptyListMsg: _supportedHTML5_emptyListMsg,
                                    emptyListMsg: '<div class="text-center" style="padding-top: 30px;">Empty of List.</div>'
                                },
                                onchange: function () {
                                },
                                onclick: function () {
                                    let fileIndex = this.fileIndex;
                                    let file = this.uploadedFiles[fileIndex];
                                    switch (this.cellType) {
                                        case 'delete':
                                            if (this.self.can_write == false) {
                                                return;
                                            }
                                            DIALOG.confirm(
                                                {
                                                    title: '삭제',
                                                    msg: '해당 파일을 삭제하시겠습니까?',
                                                    autoCloseTime: 10000,
                                                },
                                                function () {
                                                    if (this.key == 'ok') {
                                                        let data = {};
                                                        data['fileId'] = file.fileId;
                                                        data['tableName'] = file.TableName;
                                                        data['physicFileName'] = file.AttachName;
                                                        let fnSuccess = function (res) {
                                                            UPLOAD.removeFile(fileIndex);
                                                            Notify.success('파일이 삭제되었습니다.');
                                                            DIALOG.close();
                                                        }
                                                        //AjaxUtil.postAsyncData('/api/common/attach_file?action=deleteFile', data, fnSuccess);
                                                        let result = AjaxUtil.postSyncData('/api/common/attach_file/deleteFile', data);
                                                        if (result && result.success == true) {
                                                            DIALOG.close();
                                                            UPLOAD.removeFile(fileIndex);
                                                            myconfig.deleteCallback(result);
                                                            Notify.success('파일이 삭제되었습니다.');

                                                        }
                                                    }
                                                });
                                            break;

                                        case 'download':
                                            if (file) {
                                                let url = '/api/files/download?TableName=' + file.TableName + '&AttachName=' + file.AttachName + '&file_id=' + file.fileId;
                                                AjaxUtil.downloadFile(url, file.fileNm);
                                            }
                                            break;
                                    }
                                }
                            },
                            validateSelectedFiles: function () {

                                if (this.self.mode != 'active') {
                                    return;
                                }

                                if (this.self.can_write == false) {
                                    Alert.alert('파일 업로드 오류', '권한이 없습니다.');
                                    return false;
                                }

                                // 10개 이상 업로드 되지 않도록 제한.
                                if (this.uploadedFiles.length + this.selectedFiles.length > myconfig.maxcnt) {
                                    Alert.alert('', myconfig.maxcnt + "개 이상의 파일을 업로드할 수 없습니다.");
                                    return false;
                                }

                                return true;
                            },
                            onprogress: function () {

                            },
                            onuploaderror: function () {
                                Alert.alert('업로드 오류', this.self.statusText);
                            },
                            onuploaded: function () {
                                //myconfig.uploadCallback(this);
                            },
                            onuploadComplete: function () {
                                var fileIds = '';
                                $.each(this.self.uploadedFiles, function () {
                                    fileIds += (fileIds == '' ? '' : ',') + this.fileId;
                                });
                                $(myconfig.fileIdsCtl).val(fileIds);
                                // 파일 목록 가져오기
                                $.ajax({
                                    method: 'GET',
                                    success: function (res) {
                                        UPLOAD.setUploadedFiles(res);
                                        myconfig.uploadCallback(res);
                                    }
                                });
                            }
                        });

                        return UPLOAD;
                    } else if (myconfig.uploadsxn == 2) {
                        UPLOAD2 = new ax5.ui.uploader({
                            //debug: true,
                            target: $target,
                            form: {
                                action: encodeURI(_uri),
                                fileName: 'uploadfile'
                            },
                            accept: myconfig.accept,
                            multiple: myconfig.multiple,
                            manualUpload: false,
                            progressBox: true,
                            progressBoxDirection: 'left',
                            dropZone: {
                                target: $('[data-uploaded-box="' + myconfig.dropZone + '"]')
                            },
                            uploadedBox: {
                                target: $('[data-uploaded-box="' + myconfig.boxZone + '"]'),
                                icon: myconfig.icons,
                                columnKeys: {
                                    name: 'fileNm',
                                    type: 'fileExt',
                                    size: 'fileSize',
                                    uploadedName: 'fileNm',
                                    uploadedPath: '',
                                    downloadPath: '',
                                    previewPath: '',
                                    thumbnail: ''
                                },
                                lang: {
                                    supportedHTML5_emptyListMsg: _supportedHTML5_emptyListMsg,
                                    emptyListMsg: '<div class="text-center" style="padding-top: 30px;">Empty of List.</div>'
                                },
                                onchange: function () {

                                },
                                onclick: function () {
                                    var fileIndex = this.fileIndex;
                                    var file = this.uploadedFiles[fileIndex];
                                    switch (this.cellType) {
                                        case 'delete':
                                            DIALOG.confirm(
                                                {
                                                    title: '삭제',
                                                    msg: '해당 파일을 삭제하시겠습니까?'
                                                },
                                                function () {
                                                    if (this.key == 'ok') {
                                                        let data = {};
                                                        data['fileId'] = file.fileId;
                                                        let fnSuccess = function (res) {
                                                            UPLOAD2.removeFile(fileIndex);
                                                            Notify.success('삭제되었습니다');
                                                        }

                                                        AjaxUtil.postAsyncData('/api/common/attach_file/deleteFile', data, fnSuccess);
                                                    }
                                                });
                                            break;

                                        case 'download':
                                            if (file) {
                                                let url = '/api/files/download?TableName=' + file.TableName + '&AttachName=' + file.AttachName + '&file_id=' + file.fileId;
                                                AjaxUtil.downloadFile(url, file.fileNm);
                                            }
                                            break;

                                    }
                                }
                            },
                            validateSelectedFiles: function () {
                                // 10개 이상 업로드 되지 않도록 제한.
                                if (this.uploadedFiles.length + this.selectedFiles.length > myconfig.maxcnt) {
                                    Alert.alert('', myconfig.maxcnt + "개 이상의 파일을 업로드할 수 없습니다.");
                                    return false;
                                }

                                return true;
                            },
                            onprogress: function () {

                            },
                            onuploaderror: function () {
                                Alert.alert('업로드 오류', this.self.statusText);
                            },
                            onuploaded: function () {
                                myconfig.uploadCallback(this);
                            },
                            onuploadComplete: function () {
                                var fileIds = '';
                                $.each(this.self.uploadedFiles, function () {
                                    fileIds += (fileIds == '' ? '' : ',') + this.fileId;
                                });
                                $(myconfig.fileIdsCtl).val(fileIds);
                                // 파일 목록 가져오기
                                $.ajax({
                                    method: 'GET',
                                    success: function (res) {
                                        UPLOAD2.setUploadedFiles(res);
                                    }
                                });
                            }
                        });
                        return UPLOAD2;
                    } else if (myconfig.uploadsxn == 3) {
                        UPLOAD3 = new ax5.ui.uploader({
                            //debug: true,
                            target: $target,
                            form: {
                                action: encodeURI(_uri),
                                fileName: 'uploadfile'
                            },
                            accept: myconfig.accept,
                            multiple: myconfig.multiple,
                            manualUpload: false,
                            progressBox: true,
                            progressBoxDirection: 'left',
                            dropZone: {
                                target: $('[data-uploaded-box="' + myconfig.dropZone + '"]')
                            },
                            uploadedBox: {
                                target: $('[data-uploaded-box="' + myconfig.boxZone + '"]'),
                                icon: myconfig.icons,
                                columnKeys: {
                                    name: 'fileNm',
                                    type: 'fileExt',
                                    size: 'fileSize',
                                    uploadedName: 'fileNm',
                                    uploadedPath: '',
                                    downloadPath: '',
                                    previewPath: '',
                                    thumbnail: ''
                                },
                                lang: {
                                    supportedHTML5_emptyListMsg: _supportedHTML5_emptyListMsg,
                                    emptyListMsg: '<div class="text-center" style="padding-top: 30px;">Empty of List.</div>'
                                },
                                onchange: function () {

                                },
                                onclick: function () {
                                    var fileIndex = this.fileIndex;
                                    var file = this.uploadedFiles[fileIndex];
                                    switch (this.cellType) {
                                        case 'delete':
                                            DIALOG.confirm(
                                                {
                                                    title: '삭제',
                                                    msg: '해당파일을 삭제하시겠습니까?'
                                                },
                                                function () {
                                                    if (this.key == 'ok') {
                                                        let data = {};
                                                        data['fileId'] = file.fileId;
                                                        let fnSuccess = function (res) {
                                                            UPLOAD3.removeFile(fileIndex);
                                                            Notify.success('삭제되었습니다');
                                                        }
                                                        AjaxUtil.postAsyncData('/api/common/attach_file/deleteFile', data, fnSuccess);
                                                    }
                                                });
                                            break;

                                        case 'download':
                                            if (file) {
                                                let url = '/api/files/download?TableName=' + file.TableName + '&AttachName=' + file.AttachName + '&file_id=' + file.fileId;
                                                AjaxUtil.downloadFile(url, file.fileNm);
                                            }
                                            break;
                                    }
                                }
                            },
                            validateSelectedFiles: function () {
                                // 10개 이상 업로드 되지 않도록 제한.
                                if (this.uploadedFiles.length + this.selectedFiles.length > myconfig.maxcnt) {
                                    Alert.alert('', myconfig.maxcnt + "개 이상의 파일을 업로드할 수 없습니다.");
                                    return false;
                                }

                                return true;
                            },
                            onprogress: function () {

                            },
                            onuploaderror: function () {
                                Alert.alert('업로드 오류', this.self.statusText);
                            },
                            onuploaded: function () {
                                myconfig.uploadCallback(this);
                            },
                            onuploadComplete: function () {
                                var fileIds = '';
                                $.each(this.self.uploadedFiles, function () {
                                    fileIds += (fileIds == '' ? '' : ',') + this.fileId;
                                });
                                $(myconfig.fileIdsCtl).val(fileIds);
                                // 파일 목록 가져오기
                                $.ajax({
                                    method: 'GET',
                                    success: function (res) {
                                        UPLOAD3.setUploadedFiles(res);
                                    }
                                });
                            }
                        });
                        return UPLOAD3;
                    }
                }
                catch (ex) {
                }

                $(myconfig.downloadAllCtl).on('click', function () {
                    if (UPLOAD.uploadedFiles.length + UPLOAD.selectedFiles.length > 0) {
                        DIALOG.confirm({
                            title: '전체 다운로드',
                            msg: 'zip으로 압축하여 다운로드하시겠습니까?'
                        }, function () {
                            if (this.key == 'ok') {
                                location.href = '/files/filedown/zip/' + $(myconfig.fileIdsCtl).val();
                            }
                        });
                    }
                });
            }

            return fn_success();
        }
    },
    deleteId: function (_ctl, _fileid) {
        var ids = $(_ctl).val().split(',');
        var returnids = new Array;
        $.each(ids, function () {
            if (this != _fileid) {
                returnids.push(this);
            }
        });
        $(_ctl).val(returnids.join(','));
    },
    getAttachFiles: function (param) {
        
        let attresult = AjaxUtil.getSyncData('/api/common/attach_file/detailFiles', param);
        if (UPLOAD) {
            UPLOAD.setUploadedFiles(attresult.data);
        }
        if (UPLOAD2) {
            UPLOAD2.setUploadedFiles(attresult.data);
        }
        if (UPLOAD3) {
            UPLOAD3.setUploadedFiles(attresult.data);
        }
        
    }
}

$(function () {
    try {
        YuFileUploader.init();
    }
    catch (ex) {
        alert(ex);
    }

});
