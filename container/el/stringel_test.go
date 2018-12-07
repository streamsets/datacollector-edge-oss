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
	"testing"
)

func TestStringEL(test *testing.T) {
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test function str:length",
			Expression: "${str:length('abcd')}",
			Expected:   4,
		},
		{
			Name:       "Test function str:substring - 1",
			Expression: "${str:substring('hamburger', 4, 8)}",
			Expected:   "urge",
		},
		{
			Name:       "Test function str:substring - 2",
			Expression: "${str:substring('smiles', 1, 5)}",
			Expected:   "mile",
		},
		{
			Name:       "Test function str:substring - 3",
			Expression: "${str:substring('smiles', 7, 9)}",
			Expected:   "",
		},
		{
			Name:       "Test function str:substring - 2",
			Expression: "${str:substring('smiles', 2, 10)}",
			Expected:   "iles",
		},
		{
			Name:       "Test function str:substring - Error 1",
			Expression: "${str:substring('smiles', -1, 30)}",
			Expected:   "Argument beginIndex should be 0 or greater",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:substring - Error 2",
			Expression: "${str:substring('smiles', 1, -2)}",
			Expected:   "Argument endIndex should be 0 or greater",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:substring - Error 3",
			Expression: "${str:substring()}",
			Expected:   "The function 'str:substring' requires 3 arguments but was passed 0",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:indexOf - 1",
			Expression: "${str:indexOf('smiles', 'mile')}",
			Expected:   1,
		},
		{
			Name:       "Test function str:indexOf - 2",
			Expression: "${str:indexOf('smiles', 'test')}",
			Expected:   -1,
		},
		{
			Name:       "Test function str:indexOf - Error 1",
			Expression: "${str:indexOf()}",
			Expected:   "The function 'str:indexOf' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:trim - 1",
			Expression: "${str:trim('smiles ')}",
			Expected:   "smiles",
		},
		{
			Name:       "Test function str:trim - Error 1",
			Expression: "${str:trim()}",
			Expected:   "The function 'str:trim' requires 1 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:toUpper - 1",
			Expression: "${str:toUpper('smiles')}",
			Expected:   "SMILES",
		},
		{
			Name:       "Test function str:toUpper - Error 1",
			Expression: "${str:toUpper()}",
			Expected:   "The function 'str:toUpper' requires 1 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:toLower - 1",
			Expression: "${str:toLower('smILes')}",
			Expected:   "smiles",
		},
		{
			Name:       "Test function str:toLower - Error 1",
			Expression: "${str:toLower()}",
			Expected:   "The function 'str:toLower' requires 1 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:replace - 1",
			Expression: "${str:replace('sample string xyz value xyz', 'xyz', 'abc')}",
			Expected:   "sample string abc value abc",
		},
		{
			Name:       "Test function str:replace - Error 1",
			Expression: "${str:replace()}",
			Expected:   "The function 'str:replace' requires 3 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:replaceAll - 1",
			Expression: "${str:replaceAll('-ab-axxb-', 'a(x*)b', 'T')}",
			Expected:   "-T-T-",
		},
		{
			Name:       "Test function str:replaceAll - Error 1",
			Expression: "${str:replaceAll()}",
			Expected:   "The function 'str:replaceAll' requires 3 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:truncate - 1",
			Expression: "${str:truncate('abcdefgh', 4)}",
			Expected:   "abcd",
		},
		{
			Name:       "Test function str:truncate - 1",
			Expression: "${str:truncate('smiles', 10)}",
			Expected:   "smiles",
		},
		{
			Name:       "Test function str:truncate - Error 1",
			Expression: "${str:truncate()}",
			Expected:   "The function 'str:truncate' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:truncate - Error 2",
			Expression: "${str:truncate('abcdefgh', -1)}",
			Expected:   "Unable to truncate 'abcdefgh' at index -1",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:regExCapture - 1",
			Expression: "${str:regExCapture('-axxxbyc-', 'a(x*)b(y|z)c', 1)}",
			Expected:   "xxx",
		},
		{
			Name:       "Test function str:regExCapture - Error 1",
			Expression: "${str:regExCapture('-axxxbyc-', 'a(x*)b(y|z)c', 3)}",
			Expected:   "Unable to capture '-axxxbyc-' at index 3",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:regExCapture - Error 2",
			Expression: "${str:regExCapture()}",
			Expected:   "The function 'str:regExCapture' requires 3 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:contains - 1",
			Expression: "${str:contains('smiles', 'ile')",
			Expected:   true,
		},
		{
			Name:       "Test function str:contains - 2",
			Expression: "${str:contains('smiles', 'not')",
			Expected:   false,
		},
		{
			Name:       "Test function str:contains - Error 1",
			Expression: "${str:contains()}",
			Expected:   "The function 'str:contains' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:startsWith - 1",
			Expression: "${str:startsWith('smiles', 'smi')",
			Expected:   true,
		},
		{
			Name:       "Test function str:startsWith - 2",
			Expression: "${str:startsWith('smiles', 'ab')",
			Expected:   false,
		},
		{
			Name:       "Test function str:startsWith - Error 1",
			Expression: "${str:startsWith()}",
			Expected:   "The function 'str:startsWith' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:endsWith - 1",
			Expression: "${str:endsWith('smiles', 'es')",
			Expected:   true,
		},
		{
			Name:       "Test function str:endsWith - 2",
			Expression: "${str:endsWith('smiles', 'ab')",
			Expected:   false,
		},
		{
			Name:       "Test function str:endsWith - Error 1",
			Expression: "${str:endsWith()}",
			Expected:   "The function 'str:endsWith' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:length",
			Expression: "${str:length('abcd')}",
			Expected:   4,
		},
		{
			Name:       "Test function str:length - Error 1",
			Expression: "${str:length()}",
			Expected:   "The function 'str:length' requires 1 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:concat",
			Expression: "${str:concat('abcd', 'xyz')}",
			Expected:   "abcdxyz",
		},
		{
			Name:       "Test function str:concat - Error 1",
			Expression: "${str:concat()}",
			Expected:   "The function 'str:concat' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},

		{
			Name:       "Test function str:urlEncode",
			Expression: "${str:urlEncode('Ahoj tady medusa')}",
			Expected:   "Ahoj+tady+medusa",
		},
		{
			Name:       "Test function str:urlEncode - Error 1",
			Expression: "${str:urlEncode()}",
			Expected:   "The function 'str:urlEncode' requires 1 arguments but was passed 0",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:split - 1",
			Expression: "${str:split('a,b,c,d', ',')}",
			Expected:   []string{"a", "b", "c", "d"},
		},
		{
			Name:       "Test function str:split - Error 1",
			Expression: "${str:split()}",
			Expected:   "The function 'str:split' requires 2 arguments but was passed 0",
			ErrorCase:  true,
		},
		{
			Name:       "Test function str:split - Error 2",
			Expression: "${str:split('a,b,c,d', ',', '123')}",
			Expected:   "The function 'str:split' requires 2 arguments but was passed 3",
			ErrorCase:  true,
		},

		{
			Name:        "Test function uuid:uuid",
			Expression:  "${uuid:uuid()}",
			NonNilCheck: true,
		},
		{
			Name:       "Test function uuid:uuid() - Error 1",
			Expression: "${uuid:uuid('424')}",
			Expected:   "The function 'uuid:uuid' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},
	}
	RunEvaluationTests(evaluationTests, []Definitions{&StringEL{}}, test)
}
