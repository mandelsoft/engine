package locks_test

import (
	"context"
	"runtime"
	"time"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/future"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/locks"
)

var _ = Describe("Test Environment", func() {
	Context("", func() {
		var locks *me.ElementLocks[string]
		var ctx context.Context

		BeforeEach(func() {
			ctx = ctxutil.CancelContext(ctxutil.TimeoutContext(context.Background(), 10*time.Second))
			locks = me.NewElementLocks[string]()
		})

		It("locks and unlocks", func() {
			MustBeSuccessful(locks.Lock(ctx, "A"))
			MustBeSuccessful(locks.Lock(ctx, "B"))

			Expect(locks.TryLock("A")).To(BeFalse())
			Expect(locks.TryLock("B")).To(BeFalse())
			Expect(locks.TryLock("C")).To(BeTrue())
			Expect(locks.TryLock("C")).To(BeFalse())

			locks.Unlock("A")
			Expect(locks.TryLock("A")).To(BeTrue())
			Expect(locks.TryLock("B")).To(BeFalse())
			Expect(locks.TryLock("C")).To(BeFalse())

			locks.Unlock("B")
			Expect(locks.TryLock("A")).To(BeFalse())
			Expect(locks.TryLock("B")).To(BeTrue())
			Expect(locks.TryLock("C")).To(BeFalse())

			locks.Unlock("C")
			Expect(locks.TryLock("A")).To(BeFalse())
			Expect(locks.TryLock("B")).To(BeFalse())
			Expect(locks.TryLock("C")).To(BeTrue())
		})

		It("blocks and unlocks", func() {
			MustBeSuccessful(locks.Lock(ctx, "A"))

			fA := future.NewFuture(false)
			fB := future.NewFuture(false)

			go func() {
				defer GinkgoRecover()
				MustBeSuccessful(locks.Lock(ctx, "A"))
				fA.Trigger()
				locks.Unlock("A")
			}()
			go func() {
				defer GinkgoRecover()
				MustBeSuccessful(locks.Lock(ctx, "A"))
				fB.Trigger()
				locks.Unlock("A")
			}()

			for i := 0; i < 10; i++ {
				runtime.Gosched()
				if locks.HasWaiting("A") {
					break
				}
			}
			locks.Unlock("A")
			Expect(fA.Wait(ctx)).To(BeTrue())
			Expect(fB.Wait(ctx)).To(BeTrue())
		})
	})
})
