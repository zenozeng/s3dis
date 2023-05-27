package server

import (
	"context"
	"fmt"
)

func (c *Server) Info(ctx context.Context) (string, error) {
	info, err := c.db.Info(ctx)
	if err != nil {
		return "", nil
	}
	return fmt.Sprintf("db0: keys=%d,expires=%d,total_write_commands_processed=%d", info.Keys, info.Expires, info.TotalWriteCommandsProcessed), nil
}
