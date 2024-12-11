package turbopuffer

import (
	"encoding/json"
	"errors"
	"strconv"
)

/*
	This file defines top-level types for the turbopuffer API.
	Mostly a lot of Go schnanigans to make the API type-safe.
*/

// ValueType is an enumeration over the possible types of values, for both IDs and attributes.
type ValueType int

// Possible `ValueType` values.
const (
	ValueTypeInvalid ValueType = iota
	ValueTypeUInt
	ValueTypeString
	ValueTypeUIntArray
	ValueTypeStringArray
	ValueTypeFloat32Array
)

func (vt ValueType) String() string {
	switch vt {
	case ValueTypeUInt:
		return "?uint"
	case ValueTypeString:
		return "?string"
	case ValueTypeUIntArray:
		return "[]uint"
	case ValueTypeStringArray:
		return "[]string"
	case ValueTypeFloat32Array:
		return "[]f32"
	default:
		return "invalid"
	}
}

// Document is the top-level object for representing a single document in a namespace.
// Each document is uniquely identified by an ID, and as a set of attributes attached to it.
type Document struct {
	ID         DocumentID                `json:"id"`
	Vector     *Vector                   `json:"vector,omitempty"`
	Attributes map[string]AttributeValue `json:"attributes,omitempty"`
}

// ScoredDocument is a document with an associated score.
type ScoredDocument struct {
	ID         DocumentID                `json:"id"`
	Score      float32                   `json:"dist"`
	Vector     *Vector                   `json:"vector,omitempty"`
	Attributes map[string]AttributeValue `json:"attributes,omitempty"`
}

// DocumentID is a unique identifier for a document in a namespace.
type DocumentID struct {
	Value any
}

// Type returns the value type of the document ID.
// Document IDs can be either uint64 or string.
func (d DocumentID) Type() ValueType {
	switch d.Value.(type) {
	case uint64:
		return ValueTypeUInt
	case string:
		return ValueTypeString
	default:
		return ValueTypeInvalid
	}
}

// NewDocumentIDUInt creates a new DocumentID from a uint64.
func NewDocumentIDUInt(id uint64) DocumentID {
	return DocumentID{Value: id}
}

// NewDocumentIDString creates a new DocumentID from a string.
func NewDocumentIDString(id string) DocumentID {
	return DocumentID{Value: id}
}

// AsUInt returns the document ID as a uint64. Panics if the document ID is not a uint.
func (d DocumentID) AsUInt() uint64 {
	val, ok := d.Value.(uint64)
	if !ok {
		panic("document ID is not a uint")
	}
	return val
}

// AsString returns the document ID as a string. Panics if the document ID is not a string.
func (d DocumentID) AsString() string {
	val, ok := d.Value.(string)
	if !ok {
		panic("document ID is not a string")
	}
	return val
}

func (d DocumentID) String() string {
	switch val := d.Value.(type) {
	case uint64:
		return strconv.FormatUint(val, 10)
	case string:
		return val
	default:
		return "invalid"
	}
}

func (d DocumentID) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Value)
}

func (d *DocumentID) UnmarshalJSON(data []byte) error {
	var uint uint64
	if err := json.Unmarshal(data, &uint); err == nil {
		*d = NewDocumentIDUInt(uint)
		return nil
	}

	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*d = NewDocumentIDString(str)
		return nil
	}

	return errors.New("invalid document ID")
}

// Vector is a represents a vector stored within a document.
type Vector struct {
	F32 []float32
}

// Type returns the value type of the vector.
func (v Vector) Type() ValueType {
	return ValueTypeFloat32Array
}

// NewVector creates a new Vector from a float32 array.
func NewVectorF32(f32 []float32) *Vector {
	return &Vector{F32: f32}
}

// AsF32Array returns the vector as a float32 array. Panics if the vector is not a float32 array.
func (v Vector) AsF32Array() []float32 {
	return v.F32
}

func (v Vector) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.F32)
}

func (v *Vector) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.F32)
}

// AttributeValue is an attached value to a document.
type AttributeValue struct {
	String      *string
	UInt        *uint64
	StringArray []string
	UIntArray   []uint64
}

// Type returns the value type of the attribute value.
func (a AttributeValue) Type() ValueType {
	if a.String != nil {
		return ValueTypeString
	}
	if a.UInt != nil {
		return ValueTypeUInt
	}
	if a.StringArray != nil {
		return ValueTypeStringArray
	}
	if a.UIntArray != nil {
		return ValueTypeUIntArray
	}
	return ValueTypeInvalid
}

// NewValueString creates a new AttributeValue from a string.
func NewValueString(value string) AttributeValue {
	return AttributeValue{String: &value}
}

// NewValueUInt creates a new AttributeValue from a uint64.
func NewValueUInt(value uint64) AttributeValue {
	return AttributeValue{UInt: &value}
}

// NewValueStringArray creates a new AttributeValue from a string array.
func NewValueStringArray(value []string) AttributeValue {
	return AttributeValue{StringArray: value}
}

// NewValueUIntArray creates a new AttributeValue from a uint64 array.
func NewValueUIntArray(value []uint64) AttributeValue {
	return AttributeValue{UIntArray: value}
}

// AsString returns the attribute value as a string. Panics if the attribute value is not a string.
func (a AttributeValue) AsString() string {
	if a.String == nil {
		panic("attribute value is not a string")
	}
	return *a.String
}

// AsUInt returns the attribute value as a uint64. Panics if the attribute value is not a uint.
func (a AttributeValue) AsUInt() uint64 {
	if a.UInt == nil {
		panic("attribute value is not a uint")
	}
	return *a.UInt
}

// AsStringArray returns the attribute value as a string array. Panics if the attribute value is not a string array.
func (a AttributeValue) AsStringArray() []string {
	if a.StringArray == nil {
		panic("attribute value is not a string array")
	}
	return a.StringArray
}

// AsUIntArray returns the attribute value as a uint64 array. Panics if the attribute value is not a uint array.
func (a AttributeValue) AsUIntArray() []uint64 {
	if a.UIntArray == nil {
		panic("attribute value is not a uint array")
	}
	return a.UIntArray
}

func (a AttributeValue) MarshalJSON() ([]byte, error) {
	if a.String != nil {
		return json.Marshal(a.String)
	} else if a.UInt != nil {
		return json.Marshal(a.UInt)
	} else if a.StringArray != nil {
		return json.Marshal(a.StringArray)
	} else if a.UIntArray != nil {
		return json.Marshal(a.UIntArray)
	}
	return nil, errors.New("invalid attribute type")
}

func (a *AttributeValue) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		a.String = &str
		return nil
	}

	var uint uint64
	if err := json.Unmarshal(data, &uint); err == nil {
		a.UInt = &uint
		return nil
	}

	var strArray []string
	if err := json.Unmarshal(data, &strArray); err == nil {
		a.StringArray = strArray
	}

	var uintArray []uint64
	if err := json.Unmarshal(data, &uintArray); err == nil {
		a.UIntArray = uintArray
	}

	return nil
}

// BM25Params is used to configure a field to be indexed with BM25, i.e. for
// the purposes of full-text search.
type BM25Params struct {
	Language        *string `json:"language,omitempty"`         // Defaults to "english"
	Stemming        *bool   `json:"stemming,omitempty"`         // Defaults to false
	RemoveStopwords *bool   `json:"remove_stopwords,omitempty"` // Defaults to true
	CaseSensitive   *bool   `json:"case_sensitive,omitempty"`   // Defaults to false
}

// AttributeSchema is describes the schema for an attribute.
type AttributeSchema struct {
	Type string      `json:"type"`
	BM25 *BM25Params `json:"bm25,omitempty"`
}

// FilterOp is an enumeration over the supported filtering operations.
type FilterOp string

const (
	FilterOpEq       FilterOp = "Eq"
	FilterOpNotEq    FilterOp = "NotEq"
	FilterOpIn       FilterOp = "In"
	FilterOpNotIn    FilterOp = "NotIn"
	FilterOpLt       FilterOp = "Lt"
	FilterOpGt       FilterOp = "Gt"
	FilterOpLte      FilterOp = "Lte"
	FilterOpGte      FilterOp = "Gte"
	FilterOpGlob     FilterOp = "Glob"
	FilterOpNotGlob  FilterOp = "NotGlob"
	FilterOpIGlob    FilterOp = "IGlob"
	FilterOpNotIGlob FilterOp = "NotIGlob"
)

func (f FilterOp) String() string {
	return string(f)
}

// Condition is a filter condition for a query.
type Condition struct {
	Key   string
	Op    FilterOp
	Value AttributeValue
}

func (c Condition) MarshalJSON() ([]byte, error) {
	filterObj := make([]any, 0, 3)
	filterObj = append(filterObj, c.Key)
	filterObj = append(filterObj, c.Op.String())
	filterObj = append(filterObj, c.Value)
	return json.Marshal(filterObj)
}

// Filters is a set of conditions to filter a query.
type Filters struct {
	Condition *Condition
	And       []Filters
	Or        []Filters
}

func (f Filters) MarshalJSON() ([]byte, error) {
	if f.Condition != nil {
		return json.Marshal(f.Condition)
	}
	obj := make([]any, 0, 2)
	if len(f.Or) > 0 {
		obj = append(obj, "Or")
		obj = append(obj, f.Or)
	} else {
		obj = append(obj, "And")
		obj = append(obj, f.And)
	}
	return json.Marshal(obj)
}

// IncludeAttributes is a set of attributes to include in the query response.
// Caller can specify true as a shorthand to include all attributes.
type IncludeAttributes struct {
	Boolean *bool
	Keys    []string
}

// IncludeAllAttributes returns an IncludeAttributes object that includes all attributes.
func IncludeAllAttributes() *IncludeAttributes {
	return &IncludeAttributes{Boolean: AsRef(true)}
}

// IncludeSpecificAttributes returns an IncludeAttributes object that includes only the specified attributes.
func IncludeSpecificAttributes(keys ...string) *IncludeAttributes {
	return &IncludeAttributes{Keys: keys}
}

func (ia IncludeAttributes) MarshalJSON() ([]byte, error) {
	if ia.Boolean != nil {
		return json.Marshal(ia.Boolean)
	}
	return json.Marshal(ia.Keys)
}

// RankByOp is an enumeration over the possible rank operations.
type RankByOp string

const (
	RankByOpBM25 RankByOp = "BM25"
)

func (r RankByOp) String() string {
	return string(r)
}

// RankField is a field to rank by in a query.
type RankField struct {
	Key   string
	Op    RankByOp
	Input AttributeValue
}

func (rb RankField) MarshalJSON() ([]byte, error) {
	rankObj := make([]any, 0, 3)
	rankObj = append(rankObj, rb.Key)
	rankObj = append(rankObj, rb.Op.String())
	rankObj = append(rankObj, rb.Input)
	return json.Marshal(rankObj)
}

// RankBy is a set of rank fields to rank by in a query.
type RankBy struct {
	Field *RankField
	Sum   []RankField
}

func (rb RankBy) MarshalJSON() ([]byte, error) {
	if rb.Field != nil {
		return json.Marshal(rb.Field)
	}
	rankObj := make([]any, 0, 2)
	rankObj = append(rankObj, "Sum")
	rankObj = append(rankObj, rb.Sum)
	return json.Marshal(rankObj)
}

// A helper to return a reference to a value.
func AsRef[T any](v T) *T {
	return &v
}
