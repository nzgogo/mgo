package mgo

import (
"crypto/tls"
"github.com/nzgogo/mgo"
"github.com/nzgogo/mgo/bson"
"net"
"strings"
"time"
)

const (
	DefaultConnTimeout = 60 * time.Second
	DefaultProtocol        = "tcp"
)

type MgoDB interface {
	Connect() error
	Close()
	Session() *mgo.Session
	DB(string) *GomgoDB
}

type mgodb struct {
	conn     *mgo.Session
	opts     Options
	dialInfo *mgo.DialInfo
}

func (d *mgodb) Connect() error {
	var tlsConfig *tls.Config
	if d.opts.TLS != nil {
		tlsConfig = d.opts.TLS
	} else {
		tlsConfig = &tls.Config{}
		tlsConfig.InsecureSkipVerify = true
	}
	if d.opts.sslMgo {
		d.dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial(d.opts.Protocol, addr.String(), tlsConfig)
			return conn, err
		}
	}

	if d.dialInfo.Timeout == 0 {
		d.dialInfo.Timeout = DefaultConnTimeout
	}

	var err error
	d.conn, err = mgo.DialWithInfo(d.dialInfo)
	return err
}

func (d *mgodb) Close() {
	d.conn.Close()
}

func (d *mgodb) Session() *mgo.Session {
	return d.conn
}

func (d *mgodb) DB(name string) *GomgoDB {
	return &GomgoDB{d.conn.DB(name)}
}

func NewMongoDB(url string, opts ...Option) MgoDB {
	options := Options{
		Protocol: DefaultProtocol,
		sslMgo:   strings.Contains(url, "ssl=true"),
	}
	url = strings.Replace(url, "ssl=true", "", -1)
	dialOp, err := mgo.ParseURL(url)
	if err != nil {
		panic("Failed to parse URI: " + err.Error())
	}

	for _, o := range opts {
		o(&options)
	}

	return &mgodb{
		opts:     options,
		dialInfo: dialOp,
	}
}

type GomgoDB struct {
	*mgo.Database
}

func (d *GomgoDB) C(name string) *GCollect {
	return &GCollect{d.Database.C(name)}
}

type GCollect struct {
	*mgo.Collection
}

// Count returns the total number of documents in the collection.
func (m *GCollect) Count() (n int, err error) {
	return m.Find(nil).Count()
}

// Find prepares a query using the provided document. A additional condition
// is added to the query -> { delete_at: { $exists: false } }.
// The document may be a map or a struct value capable of being marshalled with bson.
// The map may be a generic one using interface{} for its key and/or values, such as
// bson.M, or it may be a properly typed map.  Providing nil as the document
// is equivalent to providing an empty document such as bson.M{}.
//
// Further details of the query may be tweaked using the resulting Query value,
// and then executed to retrieve results using methods such as One, For,
// Iter, or Tail.
//
// In case the resulting document includes a field named $err or errmsg, which
// are standard ways for MongoDB to return query errors, the returned err will
// be set to a *QueryError value including the Err message and the Code.  In
// those cases, the result argument is still unmarshalled into with the
// received document so that any other custom values may be obtained if
// desired.
func (m *GCollect) Find(query interface{}) *mgo.Query {
	if s, ok := query.(bson.M); ok {
		return m.Collection.Find(bson.M{"$and": []bson.M{
			s,
			{"delete_at": bson.M{"$exists": false}},
		}})
	} else {
		bytes, _ := bson.Marshal(query)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		return m.Collection.Find(bson.M{"$and": []bson.M{
			origin,
			{"delete_at": bson.M{"$exists": false}},
		}})
	}
	return nil
}

// FindId is a convenience helper equivalent to:
//
//     query := GCollect.Find(bson.M{"_id": id,"delete_at": bson.M{"$exists":false}},)
//
// See the Find method for more details.
func (m *GCollect) FindId(id interface{}) *mgo.Query {
	return m.Collection.Find(bson.M{"$and": []bson.M{
		{"_id": id},
		{"delete_at": bson.M{"$exists": false}},
	}})
}

// See details in m.Collection.Find()
func (m *GCollect) FindWithTrash(query interface{}) *mgo.Query {
	return m.Collection.Find(query)
}

// See details in m.Collection.FindId()
func (m *GCollect) FindIdWithTrash(id interface{}) *mgo.Query {
	return m.Collection.FindId(id)
}

// Remove finds a single document matching the provided selector document
// and performs a soft delete to the matched document (add a pair of
// key/value "delete_at":time.Now()).
//
// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
func (m *GCollect) Remove(selector interface{}) error {
	update := bson.M{"$set": bson.M{"delete_at": time.Now()}}
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}
	return m.Collection.Update(newSelector, update)
}

// RemoveId is a convenience helper equivalent to:
//
//     err := GCollect.Remove(bson.M{"_id": id})
//
// See the Remove method for more details.
func (m *GCollect) RemoveId(id interface{}) error {
	return m.Remove(bson.D{{Name: "_id", Value: id}})
}

// RemoveAll finds all documents matching the provided selector document
// and performs soft delete to the matched documents.
//
// In case the session is in safe mode (see the SetSafe method) and an
// error happens when attempting the change, the returned error will be
// of type *LastError.
func (m *GCollect) RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	update := bson.M{"$set": bson.M{"delete_at": time.Now()}}
	return m.UpdateAll(selector, update)
}

// See details in m.Collection.Remove()
func (m *GCollect) ForceRemove(selector interface{}) error {
	return m.Collection.Remove(selector)
}

// See details in m.Collection.RemoveId()
func (m *GCollect) ForceRemoveId(id interface{}) error {
	return m.Collection.RemoveId(id)
}

// See details in m.Collection.RemoveAll()
func (m *GCollect) ForceRemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	return m.Collection.RemoveAll(selector)
}

// Update finds a single document matching the provided selector document
// that is not marked as deleted (without field deleted_at) and modifies
// it according to the update document.

// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
func (m *GCollect) Update(selector interface{}, update interface{}) error {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}
	return m.Collection.Update(newSelector, update)
}

// UpdateId is a convenience helper equivalent to:
//
//     err := GCollect.Update(bson.M{"_id": id}, update)
//
// See the Update method for more details.
func (m *GCollect) UpdateId(id interface{}, update interface{}) error {
	return m.Update(bson.M{"_id": id}, update)
}

// UpdateAll finds all documents matching the provided selector document
// that is not marked as deleted (without field deleted_at) and modifies
// them according to the update document.
// If the session is in safe mode (see SetSafe) details of the executed
// operation are returned in info or an error of type *LastError when
// some problem is detected. It is not an error for the update to not be
// applied on any documents because the selector doesn't match.
func (m *GCollect) UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}
	return m.Collection.UpdateAll(newSelector, update)
}

// Update finds a single document matching the provided selector document
// that is not marked as deleted (without field deleted_at) and partially
// modifies it according to the update document.

// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
func (m *GCollect) UpdateParts(selector interface{}, update interface{}) error {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}
	var newUpdate interface{}
	if s, ok := selector.(bson.M); ok {
		newUpdate = bson.M{"$set": s}
	} else {
		bytes, _ := bson.Marshal(update)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newUpdate = bson.M{"$set": origin}
	}
	return m.Collection.Update(newSelector, newUpdate)
}

// See more details in m.Collection.Update
func (m *GCollect) UpdateWithTrash(selector interface{}, update interface{}) error {
	return m.Collection.Update(selector, update)
}

// IncrementUpdate finds a single document matching the provided selector document
// and performs a soft delete, then inserts the update document. Do not
// use Update Operators here since it's actually an insertion operation.
//
// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
func (m *GCollect) IncrementUpdate(selector interface{}, update interface{}) error {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}
	if err := m.Remove(newSelector); err != nil {
		return err
	}

	var newDoc interface{}
	if s, ok := update.(bson.M); ok {
		s["_id"] = bson.NewObjectId()
		newDoc = s
	} else {
		bytes, _ := bson.Marshal(update)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		origin["_id"] = bson.NewObjectId()
		newDoc = origin
	}

	if err := m.Insert(newDoc); err != nil {
		return err
	}
	return nil
}

// UpdateId is a convenience helper equivalent to:
//
//     err := GCollect.Update(bson.M{"_id": id}, update)
//
// See the Update method for more details.
func (m *GCollect) IncrementUpdateId(id interface{}, update interface{}) error {
	return m.IncrementUpdate(bson.D{{Name: "_id", Value: id}}, update)
}

// UpdateAll finds all documents matching the provided selector document
// and performs soft delete to them, then inserts the update document. Do
// not use Update Operators here since it's actually an insertion operation.
//
// If the session is in safe mode (see SetSafe) details of the executed
// operation are returned in info or an error of type *LastError when
// some problem is detected. It is not an error for the update to not be
// applied on any documents because the selector doesn't match.
func (m *GCollect) IncrementUpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}

	info, err = m.RemoveAll(newSelector)
	if err != nil {
		return
	}

	var newDoc interface{}
	if s, ok := update.(bson.M); ok {
		newDoc = s
	} else {
		bytes, _ := bson.Marshal(update)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newDoc = origin
	}

	for i := 0; i < info.Updated; i++ {
		if err = m.Insert(newDoc); err != nil {
			return
		}
	}

	return
}

// IncreUpsert finds a single document matching the provided selector document
// and performs a soft delete, then inserts the update document.  If no
// document matching the selector is found, the update document is inserted
// in the collection.
//
// If the session is in safe mode (see SetSafe) details  of the executed
// operation are returned in info, or an error of type *LastError when
// some problem is detected.
func (m *GCollect) IncreUpsert(selector interface{}, update interface{}) (err error) {
	var newSelector interface{}
	if s, ok := selector.(bson.M); ok {
		newSelector = bson.M{"$and": []bson.M{s, {"delete_at": bson.M{"$exists": false}}}}
	} else {
		bytes, _ := bson.Marshal(selector)
		origin := bson.M{}
		bson.Unmarshal(bytes, origin)
		newSelector = bson.M{"$and": []bson.M{origin, {"delete_at": bson.M{"$exists": false}}}}
	}

	err = m.Remove(newSelector)
	if err != nil && err != mgo.ErrNotFound {
		return
	}
	err = m.Insert(update)
	return
}

// IncreUpsertId is a convenience helper equivalent to:
//
//     info, err := GCollect.Upsert(bson.M{"_id": id}, update)
//
// See the Upsert method for more details.
func (m *GCollect) IncreUpsertId(id interface{}, update interface{}) (err error) {
	return m.IncreUpsert(bson.M{"_id": id}, update)
}
