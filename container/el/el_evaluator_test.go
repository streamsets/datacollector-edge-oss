package el

import (
	"fmt"
	"strings"
	"testing"
)

type EvaluationTest struct {
	Name       string
	Expression string
	Parameters map[string]interface{}
	Expected   interface{}
	ErrorCase  bool
}

func TestSimpleExpression(test *testing.T) {
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test empty expression",
			Expression: "",
			Expected:   "",
		},
		{
			Name:       "Test string value",
			Expression: "'sample'",
			Expected:   "sample",
		},
		{
			Name:       "Test number value",
			Expression: "10",
			Expected:   float64(10),
		},
		{
			Name:       "Test Parameter",
			Expression: "${PARAM1 > PARAM2}",
			Parameters: map[string]interface{}{
				"PARAM1": 10,
				"PARAM2": 20,
			},
			Expected: false,
		},
		{
			Name:       "Test Invalid expresion",
			Expression: "${PARAM1 > PARAM2}",
			Expected:   "No parameter 'PARAM1' found",
			ErrorCase:  true,
		},
		{
			Name:       "Test invalid expression",
			Expression: "( 10 > 5",
			Expected:   "Unbalanced parenthesis",
			ErrorCase:  true,
		},
	}
	RunEvaluationTests(evaluationTests, nil, test)
}

func RunEvaluationTests(evaluationTests []EvaluationTest, definitionsList []Definitions, test *testing.T) {
	fmt.Printf("Running %d evaluation test cases...\n", len(evaluationTests))
	for _, evaluationTest := range evaluationTests {
		evaluator, _ := NewEvaluator(
			evaluationTest.Name,
			evaluationTest.Parameters,
			definitionsList,
		)
		result, err := evaluator.Evaluate(evaluationTest.Expression)

		if err != nil {
			if evaluationTest.ErrorCase {
				if !strings.Contains(err.Error(), evaluationTest.Expected.(string)) {
					test.Logf("Test '%s' failed", evaluationTest.Name)
					test.Logf("Evaluation error '%v' does not match expected: '%v'", err.Error(),
						evaluationTest.Expected)
					test.Fail()
				}
			} else {
				test.Logf("Test '%s' failed", evaluationTest.Name)
				test.Logf("Encountered error: %s", err.Error())
				test.Fail()
			}
			continue
		}

		if result != evaluationTest.Expected {
			test.Logf("Test '%s' failed", evaluationTest.Name)
			test.Logf("Evaluation result '%v' does not match expected: '%v'", result, evaluationTest.Expected)
			test.Fail()
		}
	}
}
