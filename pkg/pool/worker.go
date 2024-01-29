/*
 * SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company and Gardener contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pool

import (
	"fmt"
	"strconv"
	"time"

	"github.com/mandelsoft/engine/pkg/healthz"
	"github.com/mandelsoft/logging"
)

// worker describe a single threaded worker entity synchronously working
// on requests provided by the controller workqueue
// It is basically a single go routine with a state for subsequent methods
// called from w go routine
type worker struct {
	logging.UnboundLogger
	dynlog logging.UnboundLogger
	pool   *pool
}

func newWorker(p *pool, number int) *worker {
	lgr := logging.DynamicLogger(p.lctx,
		logging.NewName(fmt.Sprintf("worker %d", number)),
		logging.NewAttribute("worker", strconv.Itoa(number)),
	)

	return &worker{
		UnboundLogger: lgr,
		dynlog:        lgr,
		pool:          p,
	}
}

func (w *worker) Run() {
	w.Info("starting worker")
	for w.processNextWorkItem() {
	}
	w.Info("exit worker")
}

func (w *worker) internalErr(obj interface{}, err error) bool {
	w.LogError(err, "internal error")
	w.pool.workqueue.Forget(obj)
	return true
}

func (w *worker) loggerForKey(key string) func() {
	w.UnboundLogger = w.dynlog.WithContext(logging.NewAttribute("resource-key", key), logging.NewName(key))
	return func() { w.UnboundLogger = w.dynlog }
}

func catch(f func() Status) (result Status) {
	defer func() {
		if r := recover(); r != nil {
			if res, ok := r.(Status); ok {
				result = res
			} else {
				panic(r)
			}
		}
	}()
	return f()
}

type actionFunction func() Status

func (w *worker) processNextWorkItem() bool {
	obj, shutdown := w.pool.workqueue.Get()
	if shutdown {
		return false
	}
	w.Debug("request", "key", obj)
	defer w.pool.workqueue.Done(obj)
	defer w.Debug("request done", "key", obj)
	healthz.Tick(w.pool.Key())

	key, ok := obj.(string)
	if !ok {
		return w.internalErr(obj, fmt.Errorf("expected string in workqueue but got %#v", obj))
	}

	defer w.loggerForKey(key)()

	reqlog := w.dynlog
	if w.pool.useKeyName {
		reqlog = w.UnboundLogger
	}

	cmd, rkey, err := DecodeKey(key)

	if err != nil {
		w.Error("request key error", "error", err)
		return true
	}

	ok = true
	var reschedule time.Duration = -1
	if cmd != "" {
		actions := w.pool.GetActions(cmd)
		if actions != nil && len(actions) > 0 {
			for _, action := range actions {
				status := catch(func() Status { return action.Command(w.pool, reqlog, cmd) })
				if !status.Completed {
					ok = false
				}
				if status.Error != nil {
					err = status.Error
					w.Error("command failed", "command", cmd, "error", err)
				}
				updateSchedule(&reschedule, status.Interval)
			}
		} else {
			if cmd == tickCmd {
				healthz.Tick(w.pool.Key())
				w.pool.workqueue.AddAfter(tickCmd, tick)
			} else {
				w.Error("no action found for command", "command", cmd)
			}
			return true
		}
	}
	deleted := false
	if rkey != nil {
		actions := w.pool.GetActions(ObjectType(rkey.GetType()))

		for _, a := range actions {
			status := catch(func() Status { return a.Reconcile(w.pool, reqlog, rkey) })
			if !status.Completed {
				ok = false
			}
			if status.Error != nil {
				err = status.Error
			}
			if status.Interval >= 0 {
				w.Debug("requested reschedule", "delay", status.Interval/time.Second)
			}
			updateSchedule(&reschedule, status.Interval)
		}

	}
	if err != nil {
		if ok && reschedule < 0 {
			w.Warn("add rate limited because of problem", "key", obj, "problem", err)
			// valid resources, but resources not ready yet (required state for reconciliation/deletion not yet) reached, re-add to the queue rate-limited
			w.pool.workqueue.AddRateLimited(obj)
		} else {
			// invalid resources
			if reschedule > 0 {
				w.Info("request reschedule", "key", obj, "delay", reschedule/time.Second)
				w.pool.workqueue.AddAfter(obj, reschedule)
			} else {
				w.Info("wait for new change", "key", obj, "problem", err)
			}
		}
	} else {
		if ok {
			// valid resources, everything ok, just continue normally
			w.pool.workqueue.Forget(obj)
			if reschedule < 0 || (w.pool.Period() > 0 && w.pool.Period() < reschedule) {
				if !deleted {
					reschedule = w.pool.Period()
				}
			}

			if reschedule > 0 {
				if w.pool.Period() != reschedule {
					w.Info("reschedule", "key", obj, "delay", reschedule/time.Second)
				} else {
					w.Debug("reschedule", "key", obj, "delay", reschedule/time.Second)
				}
				w.pool.workqueue.AddAfter(obj, reschedule)
			} else {
				if w.pool.Period() > 0 {
					w.Info("stop reconciling", "key", obj)
				} else {
					w.Debug("stop reconciling", "key", obj)
				}
			}
		} else {
			// valid resources, but reconciliation failed temporarily, just re-add to the queue
			w.Info("redo reconcile", "key", obj)
			w.pool.workqueue.Add(obj)
		}
	}
	return true
}

func updateSchedule(reschedule *time.Duration, interval time.Duration) {
	if interval >= 0 && (*reschedule <= 0 || interval < *reschedule) {
		*reschedule = interval
	}
}
