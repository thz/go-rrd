package rrd

import (
	"context"

	"github.com/paraopsde/go-x/pkg/util"
	"go.uber.org/zap"
)

// List returns the list of available RRDs
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	log := util.CtxLogOrPanic(ctx)

	lines, err := c.ExecCmd(NewCmd("list").WithArgs(prefix))
	if err != nil {
		return nil, err
	}

	log.Info("got list result", zap.Any("lines", lines))

	return lines, nil
}
