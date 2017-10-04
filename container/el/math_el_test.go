package el

import "testing"

func TestMathEL(t *testing.T) {
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test function math:abs - 1",
			Expression: "${math:abs(-1.0567)}",
			Expected:   float64(1.0567),
		},
		{
			Name:       "Test function math:abs - 2",
			Expression: "${math:abs(1.0567)}",
			Expected:   float64(1.0567),
		},
		{
			Name:       "Test function math:abs - 3",
			Expression: "${math:abs(\"abc\")}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:ceil - 1",
			Expression: "${math:ceil(2.54)}",
			Expected:   float64(3),
		},
		{
			Name:       "Test function math:ceil - 2",
			Expression: "${math:ceil(-2.54)}",
			Expected:   float64(-2),
		},
		{
			Name:       "Test function math:ceil - 3",
			Expression: "${math:ceil(\"abc\")}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:floor - 1",
			Expression: "${math:floor(2.54)}",
			Expected:   float64(2),
		},
		{
			Name:       "Test function math:floor - 2",
			Expression: "${math:floor(-2.54)}",
			Expected:   float64(-3),
		},
		{
			Name:       "Test function math:floor - 3",
			Expression: "${math:floor(\"abc\")}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:max - 1",
			Expression: "${math:max(2, 3)}",
			Expected:   float64(3),
		},
		{
			Name:       "Test function math:max - 2",
			Expression: "${math:max(3, 2)}",
			Expected:   float64(3),
		},
		{
			Name:       "Test function math:max - 3",
			Expression: "${math:max(2, 2)}",
			Expected:   float64(2),
		},
		{
			Name:       "Test function math:max - 4",
			Expression: "${math:max(\"abc\", 2)}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:max - 5",
			Expression: "${math:max(2, \"abc\")}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:min - 1",
			Expression: "${math:min(2, 3)}",
			Expected:   float64(2),
		},
		{
			Name:       "Test function math:min - 2",
			Expression: "${math:min(3, 2)}",
			Expected:   float64(2),
		},
		{
			Name:       "Test function math:min - 3",
			Expression: "${math:min(2, 2)}",
			Expected:   float64(2),
		},
		{
			Name:       "Test function math:min - 4",
			Expression: "${math:min(\"abc\", 2)}",
			Expected:   "",
			ErrorCase: true,
		},
		{
			Name:       "Test function math:min - 5",
			Expression: "${math:min(2, \"abc\")}",
			Expected:   "",
			ErrorCase: true,
		},
	}
	RunEvaluationTests(evaluationTests, []Definitions{&MathEL{}}, t)
}
