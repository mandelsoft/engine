package workqueue

import "k8s.io/client-go/util/workqueue"

type Queue = workqueue.RateLimitingInterface
