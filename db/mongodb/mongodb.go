package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"owl_server/models"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const PORT = 27017
const USER string = "morel"
const DB_NAME string = "owl_users"

type MongoDB struct {
	client *mongo.Client
	collection *mongo.Collection
}

// Connects to the MongoDB server, accesses the database
// and the user's collection. Stores a pointer to the user's
// collection for later use.
//
// Returns an error if any of these steps fail.
func (db *MongoDB) Connect() error {
	// log.Println("Connecting to mongodb.")
	var uri, err = getConnectionString()
	if err != nil {
		return err
	}

	clientOptions := options.Client().ApplyURI(uri)
	db.client, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}

	database := db.client.Database(DB_NAME)
	if database == nil {
		return fmt.Errorf("could not open the database %v", DB_NAME)
	}

	db.collection = database.Collection(USER)
	if db.collection == nil {
		return fmt.Errorf("could not open the collection %v", USER)
	}

	// log.Println("Connected to mongodb.")
	return nil
}

// Disconnects from the database.
// Returns an error if the disconnection fails.
func (db *MongoDB) Disconnect() error {
	if db.client == nil {
		return nil
	}
	// log.Println("Disconnecting from mongodb")
	err := db.client.Disconnect(context.TODO())
	if err != nil {
		log.Println(err)
		return err
	}
	// log.Println("Disconnected from mongodb")
	return nil
}

type ConnectionConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
	RemainingURI string `json:remainingURI`
}

func getConnectionString() (string, error) {
	configString, err := os.ReadFile("mongodbConnectionConfig.json")
	if err != nil {
		return "", fmt.Errorf("unable to retrieve connection string. Underlying error: %s", err.Error())
	}
	var config ConnectionConfig
	err = json.Unmarshal(configString, &config)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve connection string. Underlying error: %s", err.Error())
	}
	return "mongodb+srv://" + config.Username + ":" + url.QueryEscape(config.Password) + "@" + config.RemainingURI, nil
}

// Creates an stores the event in the database.
// Returns an error if the db insertion fails.
func (db *MongoDB) createEvent(eventName string, eventId string) error {
	// log.Printf("Creating event %v, %v", eventName, eventId)
	query := bson.M{
		"_id": GetID(eventName, eventId),
		"name": eventName,
		"steps": []interface{}{},
		"result": nil,
	}
	_, err := db.collection.InsertOne(context.TODO(), query)
	if err != nil {
		return err
	}
	// log.Printf("Inserted ID: %v", result.InsertedID)
	return nil
}

// Retrieves an event from the database.
// Returns a tuple (*Event, error), where *Event points
// to the successfully retrieved Event and error is non nil
// if the db retrieval fails.
// However, if the retrieval succeeds but there is no event with that (name, ID)
// in the database, it returns (nil, nil)
func (db *MongoDB) getEvent(eventName string, eventId string) (*Event, error) {
	// log.Printf("Getting event %v, %v", eventName, eventId)

	query := bson.M{"_id": GetID(eventName, eventId)}
	result := db.collection.FindOne(context.TODO(), query)
	
	var event Event
	err := result.Decode(&event)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// log.Printf("Found no event with that name")
			return nil, nil
		}
		return nil, err
	}
	// log.Printf("Got event %v", event)
	return &event, nil
}

// Inserts a given update to the database.
// The update type supported are step, label and end.
// If the given update isn't one of those types,
// an error is returned.
//
// If any error occurs during the insertion of the update
// to the db, an error is returned as well.
func (db *MongoDB) InsertUpdate(update models.Update) error {
	// log.Printf("Inserting update %s\n", update)
	// Get or create event
	event, err := db.getEvent(update.EventName, update.EventId)
	if err != nil {
		return err
	}
	if event == nil {
		err := db.createEvent(update.EventName, update.EventId)
		if err != nil {
			return err
		}
	}
	switch update.UpdateType {
	case models.UPDATE_TYPE_START:
		return db.insertStartUpdate(update)
	case models.UPDATE_TYPE_STEP:
		return db.insertStepUpdate(update)
	case models.UPDATE_TYPE_LABEL:
		return db.insertLabelUpdate(update)
	case models.UPDATE_TYPE_END:
		return db.insertEndUpdate(update)
	default:
		return fmt.Errorf("unknown update: %v", update.UpdateType)
	}
}

// Inserts the start update to the database
func (db *MongoDB) insertStartUpdate(update models.Update) error {
	// A start is basically a step, with an extra field "creationTime"
	err := db.insertStepUpdate(update)
	if err != nil {
		return err
	}
	
	// update creation time. The event should have been created already
	creationTime := time.UnixMilli(update.CreationTime * 1000)
	log.Printf("Updating creation time %v, %v for event %v", update.CreationTime * 1000, creationTime.Format("2006-01-02 15:04:05"), GetID(update.EventName, update.EventId))
	filter := bson.M{
		"_id": GetID(update.EventName, update.EventId),
	}
	query := bson.M{
		"$set": bson.M{
			"creationTime": creationTime,
		},
	}
	res, err := db.collection.UpdateOne(context.TODO(), filter, query)
	log.Printf("Update result: %v", res)
	return err
}

// Inserts the step update to the database.
func (db *MongoDB) insertStepUpdate(update models.Update) error {
	return db.createStep(update.EventName, update.EventId, update.StepName, update.StepNumber, update.Timestamp)
}

// Inserts the given label update to the database.
// If the corresponding step doesn't exist yet, it will create it with a timestamp of -1
// When the step update will be inserted, the timestamp will be updated.
func (db *MongoDB) insertLabelUpdate(update models.Update) error {
	// If step doesn't exist yet, create it
	// log.Printf("Creating corresponding step %v, %v (if it doesn't exist)", update.StepName, update.StepNumber)
	err := db.createStep(update.EventName, update.EventId, update.StepName, update.StepNumber, -1)
	if (err != nil) {
		return err
	}

	// retrieve the step
	// log.Printf("Retrieving corresponding step %v, %v", update.StepName, update.StepNumber)
	step, err := db.getStep(update.EventName, update.EventId, update.StepNumber)
	if err != nil {
		return err
	}
	if step == nil {
		// If step doesn't exist at this point, there is a bug
		return fmt.Errorf("could not retrieve step with name %v and number %d", update.StepName, update.StepNumber)
	}

	for _, label := range step.Labels {
		if label.Key == update.LabelKey {
			// duplicate label: this means the client logged the same label multiple times
			// update the label value
			// Note: Since these updates can come out of order, there's no guarantee that
			// this will be the latest label value
			// log.Printf("Label %v, %v had already been inserted in the db. Will override", update.LabelKey, update.LabelVal)

			filter := bson.M{
				"_id": GetID(update.EventName, update.EventId),
			}

			query := bson.M{
				"$set": bson.M{
					"steps.$[elem1].labels.$[elem2].val": update.LabelVal,
				},
			}

			options := options.Update().SetArrayFilters(options.ArrayFilters{
				Filters: []interface{}{
					bson.M{"elem1.number": update.StepNumber},
					bson.M{"elem2.key": update.LabelKey},
				},
			})

			_, err = db.collection.UpdateOne(context.TODO(), filter, query, options)
			return err
		}
	}

	// New label. Create it
	// log.Printf("Label %v, %v is new. Creating it and inserting into the db", update.LabelKey, update.LabelVal)
	filter := bson.M{
        "_id": GetID(update.EventName, update.EventId),
        "steps.number": update.StepNumber,
    }

    query := bson.M{
        "$push": bson.M{
            "steps.$.labels": bson.M{
                "key": update.LabelKey,
                "val": update.LabelVal,
            },
        },
    }

    // Execute the update operation
    _, err = db.collection.UpdateOne(context.TODO(), filter, query)
	return err
}

// Inserts the given end update to the database.
// Creates an 'end' step and saves the result.
// Returns an error if any of these steps failed.
func (db *MongoDB) insertEndUpdate(update models.Update) error {
	// Add end step
	// log.Println("Creating end step")
	err := db.createStep(update.EventName, update.EventId, "end", update.StepNumber, update.Timestamp)
	if err != nil {
		return err
	}

	// Save result
	// log.Printf("Saving result %s", update.Result)
	filter := bson.M{
		"_id": GetID(update.EventName, update.EventId),
	}
	query := bson.M{
		"$set": bson.M{
			"result": update.Result,
		},
	}
	_, err = db.collection.UpdateOne(context.TODO(), filter, query)
	return err
}

// Retrieves the step from the database, given the event name, event Id
// and step number.
// If there is an error retrieving the step, the error is returned.
// Otherwise a pointer to the given step is returned.
func (db *MongoDB) getStep(eventName string, eventId string, stepNumber int) (*Step, error) {
	// log.Printf("Looking for step #%d in the database", stepNumber)
	event, err := db.getEvent(eventName, eventId)
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, fmt.Errorf("couldn't find the event with event name %s and id %s", eventName, eventId)
	}

	for _, step := range event.Steps {
		if step.Number == stepNumber {
			// log.Printf("Step retrieved: %v", step)
			return &step, nil
		}
	}
	return nil, nil
}

// Creates a step if it doesn't exist.
// If the step does exist, but the timestamp doesn't (timestamp == -1),
// the timestamp is updated.
// Returns an error if any of these steps fails
func (db *MongoDB) createStep(eventName string, eventId string, stepName string, stepNumber int, timestamp int64) error {
	// log.Printf("Creating step %s", stepName)
	step, err := db.getStep(eventName, eventId, stepNumber)
	if err != nil {
		return err
	}

	filter := bson.M{"_id": GetID(eventName, eventId)}

	if step != nil {
		if (step.Timestamp == -1) {
			// step was previously added by a label,
			// so it has no timestamp. Update the timestamp
			// log.Printf("step %s had no timestamp. Updating the timestamp", stepName)
			query := bson.M{
				"$set": bson.M{
					"steps.$[elem].timestamp": timestamp,
				},
			}
			options := options.Update().SetArrayFilters(options.ArrayFilters{
				Filters: []interface{}{bson.M{"elem.number": stepNumber}},
			})

			_, err := db.collection.UpdateOne(context.TODO(), filter, query, options)
			return err
		}
		// step already exists. Return
		// log.Printf("step %s already exists", stepName)
		return nil
	}

	// The step doesn't exist. Create it
	update := bson.M{
        "$push": bson.M{
            "steps": bson.M{
                "name":      stepName,
                "number":    stepNumber,
                "timestamp": timestamp,
                "labels":    []string{},
            },
        },
    }

    _, err = db.collection.UpdateOne(context.TODO(), filter, update)
	// log.Printf("step %s created", stepName)
	return err
}

// Returns a unique db identifier in the format
// <user_collection_name>-<event_name>-<event_id>
func GetID(eventName string, eventId string) string {
	return fmt.Sprintf("%s-%s-%s", USER, eventName, eventId)
}