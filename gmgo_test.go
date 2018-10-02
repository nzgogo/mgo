package mgo_test

import (
	"github.com/nzgogo/mgo"
	. "gopkg.in/check.v1"
)

func (s *S) TestGCollect_Count(c *C) {
	session, err := mgo.Dial("mongodb://localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	var cnt int
	cnt, err = coll.Count()
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 2)
}

func (s *S) TestGCollect_Find(c *C) {
	session, err := mgo.Dial("mongodb://localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 1, "b": 3})
	c.Assert(err, IsNil)

	result := struct{ A, B int }{}

	err = coll.Find(M{"a": 1}).Sort("b").One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 1)
	c.Assert(result.B, Equals, 2)

	err = coll.Find(M{"a": 1}).Sort("-b").One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 1)
	c.Assert(result.B, Equals, 3)
}

func (s *S) TestGCollect_FindId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")
	err = coll.Insert(M{"_id": 41, "n": 41})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"_id": 42, "n": 42})
	c.Assert(err, IsNil)

	result := struct{ N int }{}

	err = coll.FindId(42).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)
}

func (s *S) TestGCollect_FindAll(c *C)  {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 3, "b": 4})
	c.Assert(err, IsNil)

	type R struct{ A, B int }
	var result []R

	assertResult := func() {
		c.Assert(len(result), Equals, 2)
		c.Assert(result[0].A, Equals, 1)
		c.Assert(result[0].B, Equals, 2)
		c.Assert(result[1].A, Equals, 3)
		c.Assert(result[1].B, Equals, 4)
	}

	// nil slice
	err = coll.Find(nil).Sort("a").All(&result)
	c.Assert(err, IsNil)
	assertResult()

	// Previously allocated slice
	allocd := make([]R, 5)
	result = allocd
	err = coll.Find(nil).Sort("a").All(&result)
	c.Assert(err, IsNil)
	assertResult()

	// Ensure result is backed by the originally allocated array
	c.Assert(&result[0], Equals, &allocd[0])

	// Re-run test destination as a pointer to interface{}
	var resultInterface interface{}

	anotherslice := make([]R, 5)
	resultInterface = anotherslice
	err = coll.Find(nil).Sort("a").All(&resultInterface)
	c.Assert(err, IsNil)
	assertResult()

	// Ensure result is backed by the originally allocated array
	c.Assert(&result[0], Equals, &allocd[0])

	// Non-pointer slice error
	f := func() { coll.Find(nil).All(result) }
	c.Assert(f, Panics, "result argument must be a slice address")

	// Non-slice error
	f = func() { coll.Find(nil).All(new(int)) }
	c.Assert(f, Panics, "result argument must be a slice address")
}

func (s *S) TestGCollect_Remove(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	// call the gmgo soft delete, which actually add field 'deletedAt' to the matched doc rather than deleting it.
	err = coll.Remove(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)

	// call the gmgo find, which will skip all docs that contains field 'deletedAt'
	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 44)

	// call the mgo find, which will not skip docs that contains field 'deletedAt'
	err = coll.Collection.Find(M{"n": 43}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 43)
}

func (s *S) TestGCollect_RemoveId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{session.DB("mydb")}.C("mycoll")

	err = coll.Insert(M{"_id": 40}, M{"_id": 41}, M{"_id": 42})
	c.Assert(err, IsNil)

	err = coll.RemoveId(41)
	c.Assert(err, IsNil)

	c.Assert(coll.FindId(40).One(nil), IsNil)
	c.Assert(coll.FindId(41).One(nil), Equals, mgo.ErrNotFound)
	c.Assert(coll.FindId(42).One(nil), IsNil)

	c.Assert(coll.Collection.FindId(41).One(nil),IsNil)
}

func (s *S) TestGCollect_RemoveAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{Database: session.DB("mydb")}.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	// gmgo RemoveAll actually performs a mgo UpdateAll. It add field 'deletedAll' to each of the matched doc.
	info, err := coll.RemoveAll(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 4)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.Matched, Equals, 4)
	c.Assert(info.UpsertedId, IsNil)

	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	// gmgo Find
	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// mgo Find
	err = coll.Collection.Find(M{"n": 43}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 43)

	err = coll.Collection.Find(M{"n": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 44)

	info, err = coll.RemoveAll(nil)
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 3)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.Matched, Equals, 3)
	c.Assert(info.UpsertedId, IsNil)

	// gmgo count
	n, err := coll.Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)

	// mgo count
	n, err = coll.Collection.Find(nil).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 7)
}

func (s *S) TestGCollect_Update(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{Database: session.DB("mydb")}.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	// No changes is a no-op and shouldn't return an error.
	err = coll.Update(M{"k": 42}, M{"$set": M{"n": 42}})
	c.Assert(err, IsNil)

	err = coll.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.Update(M{"k": 47}, M{"k": 47, "n": 47})
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"k": 47}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// gmgo update should skip docs that are softly deleted
	err = coll.Remove(M{"k": 42})
	c.Assert(err, IsNil)
	err = coll.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, Equals, mgo.ErrNotFound)
	err = coll.Collection.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Collection.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)
}

func (s *S) TestGCollect_UpdateId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := mgo.GomgoDB{Database: session.DB("mydb")}.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"_id": n, "n": n})
		c.Assert(err, IsNil)
	}

	err = coll.UpdateId(42, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.FindId(42).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.UpdateId(47, M{"k": 47, "n": 47})
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.FindId(47).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// gmgo update should skip docs that are softly deleted
	err = coll.Remove(M{"_id": 42})
	c.Assert(err, IsNil)
	err = coll.Update(M{"_id": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, Equals, mgo.ErrNotFound)
	err = coll.Collection.Update(M{"_id": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	err = coll.Find(M{"_id": 42}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Collection.Find(M{"_id": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)
}

func (s *S) TestGCollect_UpdateAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	info, err := coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$unset": M{"missing": 1}})
	c.Assert(err, IsNil)
	if s.versionAtLeast(2, 6) {
		c.Assert(info.Updated, Equals, 0)
		c.Assert(info.Matched, Equals, 4)
	} else {
		c.Assert(info.Updated, Equals, 4)
		c.Assert(info.Matched, Equals, 4)
	}

	info, err = coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 4)
	c.Assert(info.Matched, Equals, 4)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 42)

	err = coll.Find(M{"k": 43}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)

	err = coll.Find(M{"k": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 45)

	if !s.versionAtLeast(2, 6) {
		// 2.6 made this invalid.
		info, err = coll.UpdateAll(M{"k": 47}, M{"k": 47, "n": 47})
		c.Assert(err, Equals, nil)
		c.Assert(info.Updated, Equals, 0)
	}

	// gmgo updateAll should skip docs that are softly deleted
	err = coll.Remove(M{"k": 43})
	c.Assert(err, IsNil)
	info, err = coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$inc": M{"n": -1}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 3)
	c.Assert(info.Matched, Equals, 3)

	err = coll.Find(M{"k": 43}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"k": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)
}

//func (s *S) TestGCollect_Upsert(c *C) {
//
//}
//
//func (s *S) TestGCollect_UpsertId(c *C) {
//
//}

