package common

import (
	"golang.org/x/net/context"
)

func GetStageContext(ctx context.Context) StageContext {
	return ctx.Value("stageContext").(StageContext)
}
