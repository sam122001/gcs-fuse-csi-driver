/*
Copyright 2018 The Kubernetes Authors.
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testsuites

import (
	"context"
	"os"
	"sync"

	"local/test/e2e/utils"

	"github.com/onsi/ginkgo/v2"
	"k8s.io/kubernetes/test/e2e/framework"
)

// registerWebhookRequireWifDisableIfEnabled registers a BeforeEach that runs
// once per suite (via sync.Once) to disable --require-wif-credential-configmap
// when it is already enabled, then restores it after the suite via
// DeferCleanup. This prevents admission denial for suites that do not supply
// the workload-identity-credential-configmap annotation on their pods, while
// minimizing webhook redeployments to one per suite instead of one per test.
// Only active on OSS deployments (IS_OSS=true); a no-op otherwise.
func registerWebhookRequireWifDisableIfEnabled(ctx context.Context, f *framework.Framework) {
	if os.Getenv(utils.IsOSSEnvVar) != "true" {
		return
	}
	var once sync.Once
	ginkgo.BeforeEach(func() {
		once.Do(func() {
			wasEnabled, err := utils.GetWebhookRequireWifConfigmap(ctx, f.ClientSet)
			framework.ExpectNoError(err)
			if wasEnabled {
				framework.ExpectNoError(utils.SetWebhookRequireWifConfigmap(ctx, f.ClientSet, false))
				ginkgo.DeferCleanup(func() {
					framework.ExpectNoError(utils.SetWebhookRequireWifConfigmap(context.Background(), f.ClientSet, true))
				})
			}
		})
	})
}

// registerWebhookRequireWifEnable registers a BeforeEach that runs once per
// suite (via sync.Once) to enable --require-wif-credential-configmap, then
// restores it to false after the suite via DeferCleanup. Use in WIF/OIDC
// suites so they run under full enforcement. Minimizes webhook redeployments
// to one per suite instead of one per test.
// Only active on OSS deployments (IS_OSS=true); a no-op otherwise.
func registerWebhookRequireWifEnable(ctx context.Context, f *framework.Framework) {
	if os.Getenv(utils.IsOSSEnvVar) != "true" {
		return
	}
	var once sync.Once
	ginkgo.BeforeEach(func() {
		once.Do(func() {
			framework.ExpectNoError(utils.SetWebhookRequireWifConfigmap(ctx, f.ClientSet, true))
			ginkgo.DeferCleanup(func() {
				framework.ExpectNoError(utils.SetWebhookRequireWifConfigmap(context.Background(), f.ClientSet, false))
			})
		})
	})
}
