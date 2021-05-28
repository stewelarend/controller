package controller

import (
	"context"

	"github.com/stewelarend/logger"
)

type Context struct {
	context.Context
	logger.ILogger
}
