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
	"os"
	"testing"
)

func TestSdcEL(test *testing.T) {
	hostName, _ := os.Hostname()
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test sdc:hostname()",
			Expression: "${sdc:hostname()}",
			Expected:   hostName,
		},
		{
			Name:       "Test function sdc:hostname() - Error 1",
			Expression: "${sdc:hostname('invalid param')}",
			Expected:   "The function 'sdc:hostname' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},
	}
	RunEvaluationTests(evaluationTests, []Definitions{&SdcEL{}}, test)
}
