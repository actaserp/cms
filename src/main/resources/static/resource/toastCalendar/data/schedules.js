'use strict';

/*eslint-disable*/

var ScheduleList = [];

var SCHEDULE_CATEGORY = [
    'milestone',
    'task'
];

function ScheduleInfo() {
    this.id = null;
    this.calendarId = null;

    this.title = null;
    this.start = null;
    this.end = null;
    this.category = '';
    this.isReadOnly = true;

    this.color = null;
    this.bgColor = null;
    this.dragBgColor = null;
    this.borderColor = null;

}

// 시간 생성
function generateTime(schedule, renderStart, renderEnd, data) {
    schedule.category = 'time';
    schedule.start = moment(data.eduStartDtm).toDate();
    schedule.end = moment(data.eduEndDtm).toDate();
}

// 스케쥴 생성
function generateSchedule(viewName, renderStart, renderEnd, datas) {
  ScheduleList = [];
  CalendarList.forEach(function(calendar, index) {
	for(var i=0; i<datas.length; i++) {
  	  var schedule = new ScheduleInfo();
	  schedule.id = datas[i].eduSchSeq;
	  schedule.title = datas[i].title;
	  schedule.body = datas[i].description;
	  generateTime(schedule, renderStart, renderEnd, datas[i]);
	  schedule.location = datas[i].location;
	  schedule.color = calendar.color;
	  schedule.bgColor = calendar.bgColor;
	  schedule.dragBgColor = calendar.dragBgColor;
	  schedule.borderColor = calendar.borderColor;
	  if(index == 0 && datas[i].eduType == "E") {
	    schedule.calendarId = calendar.id;
	    ScheduleList.push(schedule);
	  } else if(index == 1 && datas[i].eduType == "C") {
	    schedule.calendarId = calendar.id;
	    ScheduleList.push(schedule);
	  }
	}
  });
}
