package models

import (
	"fmt"
)

const UPDATE_TYPE_START = "start"
const UPDATE_TYPE_STEP = "step"
const UPDATE_TYPE_END = "end"
const UPDATE_TYPE_LABEL = "label"

// Represents the update JSON object that is expected
// from the client
type Update struct {
	// event metadata
	EventName string `json:"eventName"`
	EventId string	`json:"eventId"`

	// update type: step, label or end
	UpdateType string `json:"updateType"`

	// step metadata
	Timestamp int64 `json:"timestamp"`
	StepNumber int `json:"stepNumber"`
	StepName string `json:"stepName"`

	// label metadata
	LabelKey string `json:"labelKey"`
	LabelVal string `json:"labelVal"`

	// end metadata
	Result string `json:"result"`
}

func (u Update) String() string {
	switch u.UpdateType{
		case "step":
			return fmt.Sprintf("Step{name: %s, number: %d, eventName: %s, eventId: %s, timeStamp: %d}", u.StepName, u.StepNumber, u.EventName, u.EventId, u.Timestamp)
		case "label":
			return fmt.Sprintf("Label{key: %s, val: %s, stepName: %s, stepNumber: %d, eventName: %s, eventId: %s, step timestamp: %d}", u.LabelKey, u.LabelVal, u.StepName, u.StepNumber, u.EventName, u.EventId, u.Timestamp)
		case "end":
			return fmt.Sprintf("End{result: %s, number: %d, eventName: %s, eventId: %s, timeStamp: %d}", u.Result, u.StepNumber, u.EventName, u.EventId, u.Timestamp)
		default:
			return fmt.Sprintf("BrokenUpdate{updateType: %s}", u.UpdateType)
	}
}