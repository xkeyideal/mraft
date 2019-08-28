package ondisk

import "github.com/rcrowley/go-metrics"

type ondiskMetrics struct {
	total metrics.Counter
	err   metrics.Counter
}

func newOndiskMetrics() *ondiskMetrics {
	return &ondiskMetrics{
		total: metrics.NewCounter(),
		err:   metrics.NewCounter(),
	}
}

func (m *ondiskMetrics) add(delta int64, err bool) {
	m.total.Inc(delta)

	if err {
		m.err.Inc(delta)
	}
}

func (m *ondiskMetrics) clear() {
	m.total.Clear()
	m.err.Clear()
}
