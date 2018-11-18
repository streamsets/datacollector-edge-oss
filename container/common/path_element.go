// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	ROOT = "ROOT"
	MAP  = "MAP"
	LIST = "LIST"

	InvalidFieldPath       = "Invalid fieldPath '%s' at char '%d'"
	InvalidFieldPathReason = "Invalid fieldPath '%s' at char position '%d' (%s)"
	ReasonEmptyFieldName   = "field name can't be empty"
	ReasonInvalidStart     = "field path needs to start with '[' or '/'"
	ReasonNotANumber       = "only numbers and '*' allowed between '[' and ']'"
	ReasonQuotes           = "quotes are not properly closed"
	InvalidFieldPathNumber = "Invalid fieldPath '%s' at char '%d' ('%s' needs to be a number or '*')"
)

type PathElement struct {
	Type string
	Name string
	Idx  int
}

var RootPathElement = &PathElement{
	Type: ROOT,
	Name: "",
	Idx:  0,
}

func CreateMapElement(name string) PathElement {
	return PathElement{
		Type: MAP,
		Name: name,
		Idx:  0,
	}
}

func CreateListElement(idx int) PathElement {
	return PathElement{
		Type: LIST,
		Name: "",
		Idx:  idx,
	}
}

func ParseFieldPath(fieldPath string, isSingleQuoteEscaped bool) ([]PathElement, error) {
	pathElementList := make([]PathElement, 0)
	pathElementList = append(pathElementList, *RootPathElement)

	if len(fieldPath) > 0 {
		requiresStart := true
		requiresName := false
		requiresIndex := false
		singleQuote := false
		doubleQuote := false
		collector := ""
		pos := 0
		for pos = 0; pos < len(fieldPath); pos++ {
			if requiresStart {
				requiresStart = false
				requiresName = false
				requiresIndex = false
				singleQuote = false
				doubleQuote = false
				switch fieldPath[pos] {
				case '/':
					requiresName = true
					break
				case '[':
					requiresIndex = true
					break
				default:
					return nil, errors.New(fmt.Sprintf(InvalidFieldPathReason, fieldPath, 0, ReasonInvalidStart))
				}
			} else {
				if requiresName {
					switch fieldPath[pos] {
					case '\\':
						if pos == 0 || fieldPath[pos-1] != '\\' {
							if !doubleQuote {
								singleQuote = !singleQuote
							} else {
								collector += string(fieldPath[pos])
							}
						} else {
							collector = collector[0 : len(collector)-1]
							collector += string(fieldPath[pos])
						}
					case '"':
						if pos == 0 || fieldPath[pos] != '\\' {
							if !singleQuote {
								doubleQuote = !doubleQuote
							} else {
								collector += string(fieldPath[pos])
							}
						} else {
							collector = collector[0 : len(collector)-1]
							collector += string(fieldPath[pos])
						}
					case '/':
						fallthrough
					case '[':
						fallthrough
					case ']':
						if singleQuote || doubleQuote {
							collector += string(fieldPath[pos])
						} else {
							if len(fieldPath) <= pos+1 {
								return nil, errors.New(
									fmt.Sprintf(InvalidFieldPathReason, fieldPath, pos, ReasonEmptyFieldName),
								)
							}
							if fieldPath[pos] == fieldPath[pos+1] {
								collector += string(fieldPath[pos])
								pos++
							} else {
								pathElementList = append(pathElementList, CreateMapElement(collector))
								requiresStart = true
								collector = ""
								//not very kosher, we need to replay the current char as start of path element
								pos--
							}
						}
					default:
						collector += string(fieldPath[pos])
					}
				} else if requiresIndex {
					switch fieldPath[pos] {
					case '0':
						fallthrough
					case '1':
						fallthrough
					case '2':
						fallthrough
					case '3':
						fallthrough
					case '4':
						fallthrough
					case '5':
						fallthrough
					case '6':
						fallthrough
					case '7':
						fallthrough
					case '8':
						fallthrough
					case '9':
						fallthrough
					case '*': //wildcard character
						collector += string(fieldPath[pos])
					case ']':
						indexString := collector
						idx := 0
						var err error

						if indexString != "*" {
							idx, err = strconv.Atoi(indexString)
							if err != nil {
								return nil, errors.New(
									fmt.Sprintf(InvalidFieldPathNumber, fieldPath, pos, err.Error()),
								)
							}
						}

						if idx >= 0 {
							pathElementList = append(pathElementList, CreateListElement(idx))
							requiresStart = true
							collector = ""
						} else {
							return nil, errors.New(
								fmt.Sprintf(InvalidFieldPath, fieldPath, pos),
							)
						}

					default:
						return nil, errors.New(
							fmt.Sprintf(InvalidFieldPathReason, fieldPath, pos, ReasonNotANumber),
						)
					}
				}
			}
		}

		if singleQuote || doubleQuote {
			// If there is no matching quote
			return nil, errors.New(fmt.Sprintf(InvalidFieldPathReason, fieldPath, 0, ReasonQuotes))
		} else if pos < len(fieldPath) {
			return nil, errors.New(fmt.Sprintf(InvalidFieldPath, fieldPath, pos))
		} else if len(collector) > 0 {
			// the last path element was a map entry, we need to create it.
			pathElementList = append(pathElementList, CreateMapElement(collector))
		}
	}
	return pathElementList, nil
}
