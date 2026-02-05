package sort

import "context"

func Run(ctx context.Context) error {
	// existing source main logic goes here
	packAndPush()
	<-ctx.Done()
	return nil
}
