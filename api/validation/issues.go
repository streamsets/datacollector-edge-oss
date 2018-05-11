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

package validation

type Issues struct {
	IssueCount     int                `json:"issueCount"`
	PipelineIssues []Issue            `json:"pipelineIssues"`
	StageIssuesMap map[string][]Issue `json:"stageIssues"`
}

func NewIssues(issues []Issue) *Issues {
	issuesObj := &Issues{
		IssueCount:     len(issues),
		PipelineIssues: make([]Issue, 0),
		StageIssuesMap: make(map[string][]Issue),
	}
	for _, issue := range issues {
		if len(issue.InstanceName) == 0 {
			issuesObj.PipelineIssues = append(issuesObj.PipelineIssues, issue)
		} else {
			if issuesObj.StageIssuesMap[issue.InstanceName] == nil {
				issuesObj.StageIssuesMap[issue.InstanceName] = make([]Issue, 0)
			}
			issuesObj.StageIssuesMap[issue.InstanceName] = append(issuesObj.StageIssuesMap[issue.InstanceName], issue)
		}
	}
	return issuesObj
}
