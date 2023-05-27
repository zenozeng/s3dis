package server

import "context"

func (c *Server) Info(ctx context.Context) (string, error) {
	return "db0: keys=0,expires=0", nil
}
