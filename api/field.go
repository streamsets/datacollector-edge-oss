package api

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"math/big"
)

type Field struct {
	Type  string
	Value interface{}
}

func CreateField(value interface{}) (*Field, error) {
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
	case map[string]interface{}:
		return CreateMapField(value.(map[string]interface{}))
	case []interface{}:
		return CreateListField(value.([]interface{}))
	default:
		err = errors.New("Unsupported Field Type")
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

func CreateMapField(mapValue map[string]interface{}) (*Field, error) {
	mapFieldValue := make(map[string]Field)
	for key, value := range mapValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		mapFieldValue[key] = *valField
	}
	mapField := Field{Type: fieldtype.MAP, Value: mapFieldValue}
	return &mapField, nil
}

func CreateListField(listValue []interface{}) (*Field, error) {
	listFieldValue := []Field{}
	for _, value := range listValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue = append(listFieldValue, *valField)
	}
	mapField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &mapField, nil
}
