package server

import (
	"testing"
	"time"
)

var (
	//AlarmUrl = "http://alarmgate.jd.local/simplealarm"
	AlarmUrl = "http://alarmgate.jd.local/alarm"
	Mail = "hewei8@jd.com"
	Sms = "18612696398"
)

func TestRemoteAlarm(t *testing.T) {
	alarm := NewAlarm(1, AlarmUrl, Mail, Sms)
	defer alarm.Stop()
	//err := alarm.Alarm("test", "alarm")
	//if err != nil {
	//	t.Errorf("alarm err %v", err)
	//	return
	//}
	msg := &AlarmMsg{
		Email: []*EmailMsg{&EmailMsg{Title: "test", Content: "alarm", To: "hewei8@jd.com"}},
		Sms: []*SmsMsg{&SmsMsg{Content: "alarm", To: "18612696398"}},
	}
	err := alarm.send(msg)
	if err != nil {
		t.Errorf("alarm err %v", err)
		return
	}
	time.Sleep(time.Second * 2)
	t.Log("test success!!!")
}
