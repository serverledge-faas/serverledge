package lb

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/stretchr/testify/assert"
)

func newTarget(name string, arch string) *middleware.ProxyTarget {
	return &middleware.ProxyTarget{
		Name: name,
		URL:  &url.URL{Host: name},
		Meta: echo.Map{"arch": arch},
	}
}

func TestNewArchitectureAwareBalancer(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("x86_1", container.X86),
		newTarget("arm2", container.ARM),
	}

	b := getNewLb(targets)

	assert.Equal(t, 2, b.armRing.Size())
	assert.Equal(t, 1, b.x86Ring.Size())
}

func TestAddTarget(t *testing.T) {
	b := getNewLb([]*middleware.ProxyTarget{})
	b.AddTarget(newTarget("arm1", container.ARM))
	b.AddTarget(newTarget("x86_1", container.X86))

	assert.Equal(t, 1, b.armRing.Size())
	assert.Equal(t, 1, b.x86Ring.Size())

	b.AddTarget(newTarget("x86_2", container.X86))
	b.AddTarget(newTarget("arm2", container.ARM))

	assert.Equal(t, 2, b.armRing.Size())
	assert.Equal(t, 2, b.x86Ring.Size())
}

func TestRemoveTarget(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("x86_1", container.X86),
	}
	b := getNewLb(targets)

	assert.Equal(t, 1, b.armRing.Size())
	assert.Equal(t, 1, b.x86Ring.Size())

	assert.True(t, b.RemoveTarget("arm1"))
	assert.False(t, b.RemoveTarget("unknown"))
	assert.Equal(t, 0, b.armRing.Size())
	assert.Equal(t, 1, b.x86Ring.Size())
}

func TestSelectArchitecture(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("x86_1", container.X86),
	}
	b := getNewLb(targets)

	// Test case 1: Function supports both ARM and x86
	funBoth := &function.Function{Name: "bothArchs", SupportedArchs: []string{container.X86, container.ARM}}
	arch, err := b.selectArchitecture(funBoth)
	assert.NoError(t, err)

	// Test case 2: Function supports only ARM
	funArm := &function.Function{Name: "onlyArm", SupportedArchs: []string{container.ARM}}
	arch, err = b.selectArchitecture(funArm)
	assert.NoError(t, err)
	assert.Equal(t, container.ARM, arch)

	// Test case 3: Function supports only x86
	funX86 := &function.Function{Name: "onlyX86", SupportedArchs: []string{container.X86}}
	arch, err = b.selectArchitecture(funX86)
	assert.NoError(t, err)
	assert.Equal(t, container.X86, arch)

	// Test case 4: No available nodes for supported architecture
	b.RemoveTarget("arm1")
	b.RemoveTarget("x86_1")
	_, err = b.selectArchitecture(funBoth)
	assert.Error(t, err)
}

func TestConsistentNodeMapping(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("x86_1", container.X86),
		newTarget("arm2", container.ARM),
		newTarget("x86_2", container.X86),
	}
	b := getNewLb(targets)

	fun := &function.Function{
		Name:           "testFunc",
		SupportedArchs: []string{container.ARM, container.X86},
	}

	// Add the function to the cache to avoid etcd dependency
	cache.GetCacheInstance().Set(fun.Name, fun, 30*time.Second)
	defer cache.GetCacheInstance().Delete(fun.Name)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/invoke/testFunc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// First call
	firstTarget := b.Next(c)
	assert.NotNil(t, firstTarget)

	// Subsequent calls should return the same target
	for i := 0; i < 10; i++ {
		nextTarget := b.Next(c)
		assert.NotNil(t, nextTarget)
	}
}

func TestGetNodeFromRing(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("arm2", container.ARM),
	}
	b := getNewLb(targets)
	nodeMap := map[string]struct{}{}
	nodeMap["arm2"] = struct{}{}
	mockMemChecker := &MockMemChecker{nodesWithEnoughMemory: nodeMap}
	b.armRing.memChecker = mockMemChecker

	fun := &function.Function{
		Name:           "testGetNodeFromRingFunc",
		SupportedArchs: []string{container.ARM},
	}

	// Add the function to the cache to avoid etcd dependency
	cache.GetCacheInstance().Set(fun.Name, fun, 30*time.Second)
	defer cache.GetCacheInstance().Delete(fun.Name)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/invoke/testGetNodeFromRingFunc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// First call
	firstTarget := b.Next(c)
	assert.NotNil(t, firstTarget)
	assert.Equal(t, firstTarget.Name, "arm2")

	// Subsequent calls should return the same target
	for i := 0; i < 10; i++ {
		nextTarget := b.Next(c)
		assert.Equal(t, firstTarget, nextTarget)
	}

	delete(mockMemChecker.nodesWithEnoughMemory, "arm2")
	nextTarget := b.Next(c)
	assert.Nil(t, nextTarget)

	mockMemChecker.nodesWithEnoughMemory["arm1"] = struct{}{}
	nextTarget = b.Next(c)
	assert.NotNil(t, nextTarget)
	assert.Equal(t, nextTarget.Name, "arm1")

}

func TestGetArchFallback(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("arm2", container.ARM),
		newTarget("x86_1", container.X86),
		newTarget("x86_2", container.X86),
	}
	b := getNewLb(targets)
	nodeMap := map[string]struct{}{}
	nodeMap["x86_2"] = struct{}{}
	mockMemChecker := &MockMemChecker{nodesWithEnoughMemory: nodeMap}
	b.armRing.memChecker = mockMemChecker
	b.x86Ring.memChecker = mockMemChecker

	fun := &function.Function{
		Name:           "testGetArchFallbackFunc",
		SupportedArchs: []string{container.ARM, container.X86},
	}

	// Add the function to the cache to avoid etcd dependency
	cache.GetCacheInstance().Set(fun.Name, fun, 30*time.Second)
	defer cache.GetCacheInstance().Delete(fun.Name)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/invoke/testGetArchFallbackFunc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// First call
	firstTarget := b.Next(c)
	assert.NotNil(t, firstTarget)
	assert.Equal(t, firstTarget.Name, "x86_2")

	// Subsequent calls should return the same target
	for i := 0; i < 10; i++ {
		nextTarget := b.Next(c)
		assert.Equal(t, firstTarget, nextTarget)
	}

	delete(mockMemChecker.nodesWithEnoughMemory, "x86_2")
	nextTarget := b.Next(c)
	assert.Nil(t, nextTarget)

	mockMemChecker.nodesWithEnoughMemory["arm2"] = struct{}{}
	nextTarget = b.Next(c)
	assert.NotNil(t, nextTarget)
	assert.Equal(t, nextTarget.Name, "arm2")

}

func TestGetArchFallbackNotPossible(t *testing.T) {
	targets := []*middleware.ProxyTarget{
		newTarget("arm1", container.ARM),
		newTarget("arm2", container.ARM),
		newTarget("x86_1", container.X86),
	}
	b := getNewLb(targets)
	nodeMap := map[string]struct{}{}
	nodeMap["x86_1"] = struct{}{} // has enough memory but should still not be used because incompatible architecture
	mockMemChecker := &MockMemChecker{nodesWithEnoughMemory: nodeMap}
	b.armRing.memChecker = mockMemChecker
	b.x86Ring.memChecker = mockMemChecker

	fun := &function.Function{
		Name:           "testGetArchFallbackNotPossibleFunc",
		SupportedArchs: []string{container.ARM},
	}

	// Add the function to the cache to avoid etcd dependency
	cache.GetCacheInstance().Set(fun.Name, fun, 30*time.Second)
	defer cache.GetCacheInstance().Delete(fun.Name)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/invoke/testGetArchFallbackNotPossibleFunc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// First call
	firstTarget := b.Next(c)
	assert.Nil(t, firstTarget)

	// Subsequent calls should return the same target
	for i := 0; i < 10; i++ {
		nextTarget := b.Next(c)
		assert.Equal(t, firstTarget, nextTarget)
	}

	mockMemChecker.nodesWithEnoughMemory["arm1"] = struct{}{}
	nextTarget := b.Next(c)
	assert.NotNil(t, nextTarget)
	assert.Equal(t, nextTarget.Name, "arm1")

}

type MockMemChecker struct {
	nodesWithEnoughMemory map[string]struct{}
}

func (m *MockMemChecker) HasEnoughMemory(target *middleware.ProxyTarget, fun *function.Function) bool {
	_, ret := m.nodesWithEnoughMemory[target.Name]
	return ret
}

func getNewLb(targets []*middleware.ProxyTarget) *ArchitectureAwareBalancer {
	mab.InitBanditManager()
	return NewArchitectureAwareBalancer(targets)
}
