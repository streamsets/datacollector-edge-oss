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
package el

import (
	"errors"
	"fmt"
	"github.com/madhukard/govaluate"
	"github.com/satori/go.uuid"
	"github.com/spf13/cast"
	"net/url"
	"regexp"
	"strings"
)

type StringEL struct {
}

func (stringEL *StringEL) Substring(args ...interface{}) (interface{}, error) {
	if len(args) < 3 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:substring' requires 3 arguments but was passed %d", len(args)),
		)
	}

	str := cast.ToString(args[0])
	beginIndex := cast.ToInt(args[1])
	endIndex := cast.ToInt(args[2])

	if beginIndex < 0 {
		return nil, errors.New("Argument beginIndex should be 0 or greater")
	}

	if endIndex < 0 {
		return nil, errors.New("Argument endIndex should be 0 or greater")
	}

	length := len(str)

	if beginIndex > length {
		return "", nil
	}

	if endIndex > length {
		endIndex = length
	}

	return str[beginIndex:endIndex], nil
}

func (stringEL *StringEL) IndexOf(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:indexOf' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	subStr := cast.ToString(args[1])
	return strings.Index(str, subStr), nil
}

func (stringEL *StringEL) Trim(args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:trim' requires 1 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	return strings.TrimSpace(str), nil
}

func (stringEL *StringEL) ToUpper(args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:toUpper' requires 1 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	return strings.ToUpper(str), nil
}

func (stringEL *StringEL) ToLower(args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:toLower' requires 1 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	return strings.ToLower(str), nil
}

func (stringEL *StringEL) Replace(args ...interface{}) (interface{}, error) {
	if len(args) < 3 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:replace' requires 3 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	oldString := cast.ToString(args[1])
	newString := cast.ToString(args[2])
	return strings.Replace(str, oldString, newString, -1), nil
}

func (stringEL *StringEL) ReplaceAll(args ...interface{}) (interface{}, error) {
	if len(args) < 3 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:replaceAll' requires 3 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	regEx := cast.ToString(args[1])
	replacement := cast.ToString(args[2])
	reg, err := regexp.Compile(regEx)
	if err != nil {
		return nil, err
	}
	return reg.ReplaceAllString(str, replacement), nil
}

func (stringEL *StringEL) Truncate(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:truncate' requires 2 arguments but was passed %d", len(args)),
		)
	}

	str := cast.ToString(args[0])
	endIndex := cast.ToInt(args[1])

	if endIndex < 0 {
		return nil, errors.New(fmt.Sprintf("Unable to truncate '%s' at index %d", str, endIndex))
	}

	length := len(str)
	if endIndex > length {
		endIndex = length
	}

	return str[0:endIndex], nil
}

func (stringEL *StringEL) RegExCapture(args ...interface{}) (interface{}, error) {
	if len(args) < 3 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:regExCapture' requires 3 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	regEx := cast.ToString(args[1])
	groupNumber := cast.ToInt(args[2])
	reg, err := regexp.Compile(regEx)
	if err != nil {
		return nil, err
	}

	subMatchSlice := reg.FindStringSubmatch(str)

	if len(subMatchSlice) <= groupNumber {
		return nil, errors.New(fmt.Sprintf("Unable to capture '%s' at index %d", str, groupNumber))
	}

	return subMatchSlice[groupNumber], nil
}

func (stringEL *StringEL) Contains(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:contains' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	substring := cast.ToString(args[1])
	return strings.Contains(str, substring), nil
}

func (stringEL *StringEL) StartsWith(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:startsWith' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	prefix := cast.ToString(args[1])
	return strings.HasPrefix(str, prefix), nil
}

func (stringEL *StringEL) EndsWith(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:endsWith' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	suffix := cast.ToString(args[1])
	return strings.HasSuffix(str, suffix), nil
}

func (stringEL *StringEL) Concat(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:concat' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str1 := cast.ToString(args[0])
	str2 := cast.ToString(args[1])
	return str1 + str2, nil
}

func (stringEL *StringEL) Length(args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:length' requires 1 arguments but was passed %d", len(args)),
		)
	}
	length := len(cast.ToString(args[0]))
	return length, nil
}

func (stringEL *StringEL) UrlEncode(args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:urlEncode' requires 1 arguments but was passed %d", len(args)),
		)
	}
	length := url.QueryEscape(cast.ToString(args[0]))
	return length, nil
}

func (stringEL *StringEL) EscapeXML10(args ...interface{}) (interface{}, error) {
	return nil, errors.New("Not Supported")
}

func (stringEL *StringEL) EscapeXML11(args ...interface{}) (interface{}, error) {
	return nil, errors.New("Not Supported")
}

func (stringEL *StringEL) UnescapeXML(args ...interface{}) (interface{}, error) {
	return nil, errors.New("Not Supported")
}

func (stringEL *StringEL) UnescapeJava(args ...interface{}) (interface{}, error) {
	return nil, errors.New("Not Supported")
}

func (stringEL *StringEL) Split(args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return "", errors.New(
			fmt.Sprintf("The function 'str:split' requires 2 arguments but was passed %d", len(args)),
		)
	}
	str := cast.ToString(args[0])
	separator := cast.ToString(args[1])
	return strings.Split(str, separator), nil
}

func (stringEL *StringEL) Uuid(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'uuid:uuid' requires 0 arguments but was passed %d", len(args)),
		)
	}
	return uuid.NewV4().String(), nil
}

func (stringEL *StringEL) GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction {
	functions := map[string]govaluate.ExpressionFunction{
		"str:substring":    stringEL.Substring,
		"str:indexOf":      stringEL.IndexOf,
		"str:trim":         stringEL.Trim,
		"str:toUpper":      stringEL.ToUpper,
		"str:toLower":      stringEL.ToLower,
		"str:replace":      stringEL.Replace,
		"str:replaceAll":   stringEL.ReplaceAll,
		"str:truncate":     stringEL.Truncate,
		"str:regExCapture": stringEL.RegExCapture,
		"str:contains":     stringEL.Contains,
		"str:concat":       stringEL.Concat,
		"str:length":       stringEL.Length,
		"str:startsWith":   stringEL.StartsWith,
		"str:endsWith":     stringEL.EndsWith,
		"str:urlEncode":    stringEL.UrlEncode,
		"str:escapeXML10":  stringEL.EscapeXML10,
		"str:escapeXML11":  stringEL.EscapeXML11,
		"str:unescapeXML":  stringEL.UnescapeXML,
		"str:unescapeJava": stringEL.UnescapeJava,
		"str:split":        stringEL.Split,
		"uuid:uuid":        stringEL.Uuid,
	}
	return functions
}
