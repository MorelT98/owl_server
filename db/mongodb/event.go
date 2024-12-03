package mongodb

import (
	"fmt"
	"strings"
)

// Represents an event as saved in the mongoDB database.
// Events retrieved from the database will have this
// format.
type Event struct {
	Name string `bson:"name"`
	Id string `bson:"_id"`
	Result string `bson:"result"`
	Steps []Step `bson:"steps"`
}

func (e Event) String() string {
	var builder strings.Builder
	builder.WriteString("[")
	for i, step := range e.Steps {
		if i < len(e.Steps) - 1 {
			builder.WriteString(fmt.Sprintf("%v, ", step))
		} else {
			builder.WriteString(fmt.Sprintf("%v", step))
		}
	}
	builder.WriteString("]")
	return fmt.Sprintf("Event{name: %s, _id: %s, result: %s, steps: %v}", e.Name, e.Id, e.Result, builder.String())
}


// Represents a step as saved in the mongoDB database.
// Steps retrieved from the database will have this
// format.
type Step struct {
	Name string `bson:"name"`
	Number int `bson:"number"`
	Timestamp int64 `bson:"timestamp"`
	Labels []Label `bson:"labels"`
}

func (s Step) String() string {
	var builder strings.Builder
	builder.WriteString("[")
	for i, label := range s.Labels {
		if i < len(s.Labels) - 1 {
			builder.WriteString(fmt.Sprintf("%v, ", label))
		} else {
			builder.WriteString(fmt.Sprintf("%v", label))
		}
	}
	builder.WriteString("]")
	return fmt.Sprintf("Step{name: %s, number: %d, timestamp: %d, labels: %v}", s.Name, s.Number, s.Timestamp, builder.String())
}

// Represents a label as saved in the mongoDB database.
// Labels retrieved from the database will have this
// format.
type Label struct {
	Key string `bson:"key"`
	Val string `bson:"val"`
}

func (l Label) String() string {
	return fmt.Sprintf("Label{key: %s, val: %s}", l.Key, l.Val)
}