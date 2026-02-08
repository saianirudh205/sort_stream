package source

import "context"

func Run(ctx context.Context) error {
	// existing source main logic goes here
	Producer()
	<-ctx.Done()
	return nil
}
