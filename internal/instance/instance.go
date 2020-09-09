package instance

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/cenkalti/dalga/v3/internal/table"
)

type Instance struct {
	id    uint32
	table *table.Table
	done  chan struct{}
}

func New(t *table.Table) *Instance {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s) // nolint: gosec
	id := r.Uint32()
	return &Instance{
		id:    id,
		table: t,
		done:  make(chan struct{}),
	}
}

func (i *Instance) ID() uint32 {
	return i.id
}

func (i *Instance) NotifyDone() chan struct{} {
	return i.done
}

func (i *Instance) Run(ctx context.Context) {
	defer close(i.done)
	i.updateInstance(ctx)
	for {
		select {
		case <-time.After(time.Second):
			i.updateInstance(ctx)
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := i.table.DeleteInstance(shutdownCtx, i.id)
			if err != nil {
				log.Print("cannot delete instance from db:", err)
			}
			return
		}
	}
}

func (i *Instance) updateInstance(ctx context.Context) {
	err := i.table.UpdateInstance(ctx, i.id)
	if err != nil {
		log.Print("cannot update instance at db: ", err)
	}
}
