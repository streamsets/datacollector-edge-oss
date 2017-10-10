package api

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"math/big"
	"reflect"
)

type Field struct {
	Type  string
	Value interface{}
}

func (f *Field) Clone() *Field {
	switch f.Type {
	case fieldtype.MAP:
		mapField := f.Value.(map[string](*Field))
		returnMap := map[string](*Field){}
		for k, v := range mapField {
			returnMap[k] = v.Clone()
		}
		return &Field{Type: f.Type, Value: returnMap}
	case fieldtype.LIST_MAP:
		mapField := f.Value.(map[string](*Field))
		returnMap := map[string](*Field){}
		for k, v := range mapField {
			returnMap[k] = v.Clone()
		}
		return &Field{Type: f.Type, Value: returnMap}
	case fieldtype.LIST:
		listField := f.Value.([](*Field))
		returnList := make([](*Field), len(listField))
		for idx, v := range listField {
			returnList[idx] = v.Clone()
		}
		return &Field{Type: f.Type, Value: returnList}
	default:
		field, _ := CreateField(f.Value)
		return field
	}
}

func CreateField(value interface{}) (*Field, error) {
	if value == nil {
		return CreateStringField("")
	}
	var err error = nil
	switch value.(type) {
	case bool:
		return CreateBoolField(value.(bool))
	case []byte:
		return CreateByteArrayField(value.([]byte))
	case byte:
		return CreateByteField(value.(byte))
	case int8:
		return CreateShortField(value.(int8))
	case int32:
		return CreateInteger32Field(value.(int32))
	case int:
		return CreateIntegerField(value.(int))
	case int64:
		return CreateLongField(value.(int64))
	case uint16:
		return CreateUInteger16Field(value.(uint16))
	case uint32:
		return CreateUInteger32Field(value.(uint32))
	case uint64:
		return CreateLongFieldU64(value.(uint64))
	case float32:
		return CreateFloatField(value.(float32))
	case float64:
		return CreateDoubleField(value.(float64))
	case big.Int:
		return CreateBigIntField(value.(big.Int))
	case big.Float:
		return CreateBigFloatField(value.(big.Float))
	case string:
		return CreateStringField(value.(string))
	case []string:
		return CreateStringListField(value.([]string))
	case map[string]interface{}:
		return CreateMapField(value.(map[string]interface{}))
	case []interface{}:
		return CreateListField(value.([]interface{}))
	default:
		err = errors.New(fmt.Sprintf("Unsupported Field Type %s", reflect.TypeOf(value)))
	}
	return nil, err
}

func CreateBoolField(value bool) (*Field, error) {
	return &Field{Type: fieldtype.BOOLEAN, Value: value}, nil
}

func CreateByteArrayField(value []byte) (*Field, error) {
	return &Field{Type: fieldtype.BYTE_ARRAY, Value: value}, nil
}

func CreateByteField(value byte) (*Field, error) {
	return &Field{Type: fieldtype.BYTE, Value: value}, nil
}

func CreateShortField(value int8) (*Field, error) {
	return &Field{Type: fieldtype.SHORT, Value: value}, nil
}

func CreateIntegerField(value int) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateInteger32Field(value int32) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateUInteger16Field(value uint16) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateUInteger32Field(value uint32) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateLongFieldU64(value uint64) (*Field, error) {
	return &Field{Type: fieldtype.LONG, Value: value}, nil
}

func CreateLongField(value int64) (*Field, error) {
	return &Field{Type: fieldtype.LONG, Value: value}, nil
}

func CreateFloatField(value float32) (*Field, error) {
	return &Field{Type: fieldtype.FLOAT, Value: value}, nil
}

func CreateDoubleField(value float64) (*Field, error) {
	return &Field{Type: fieldtype.DOUBLE, Value: value}, nil
}

func CreateBigIntField(value big.Int) (*Field, error) {
	return &Field{Type: fieldtype.DECIMAL, Value: value}, nil
}

func CreateBigFloatField(value big.Float) (*Field, error) {
	return &Field{Type: fieldtype.DECIMAL, Value: value}, nil
}

func CreateStringField(value string) (*Field, error) {
	return &Field{Type: fieldtype.STRING, Value: value}, nil
}

func CreateStringListField(listStringValue []string) (*Field, error) {
	listFieldValue := []*Field{}
	for _, value := range listStringValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue = append(listFieldValue, valField)
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}

func CreateMapField(mapValue map[string]interface{}) (*Field, error) {
	mapFieldValue := make(map[string](*Field))
	for key, value := range mapValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		mapFieldValue[key] = valField
	}
	mapField := Field{Type: fieldtype.MAP, Value: mapFieldValue}
	return &mapField, nil
}

func CreateListField(listValue []interface{}) (*Field, error) {
	listFieldValue := []*Field{}
	for _, value := range listValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue = append(listFieldValue, valField)
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}
