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

package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	webhookDeploymentNamespace = "gcs-fuse-csi-driver"
	webhookDeploymentName      = "gcs-fuse-csi-driver-webhook"
	webhookContainerName       = "gcs-fuse-csi-driver-webhook"
	requireWifFlagArg          = "--require-wif-credential-configmap"

	webhookRolloutPollInterval = 5 * time.Second
	webhookRolloutTimeout      = 3 * time.Minute
)

// GetWebhookRequireWifConfigmap returns true if the webhook deployment is
// currently running with --require-wif-credential-configmap=true (or the bare
// flag with no value, which Go flag treats as true).
func GetWebhookRequireWifConfigmap(ctx context.Context, cs clientset.Interface) (bool, error) {
	d, err := cs.AppsV1().Deployments(webhookDeploymentNamespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("getting webhook deployment: %w", err)
	}
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name != webhookContainerName {
			continue
		}
		for _, arg := range c.Args {
			if arg == requireWifFlagArg || arg == requireWifFlagArg+"=true" {
				return true, nil
			}
		}
	}
	return false, nil
}

// SetWebhookRequireWifConfigmap patches the webhook deployment to enable or
// disable --require-wif-credential-configmap, then waits for the rollout to
// complete. It is a no-op if the deployment is already in the desired state.
// Retries on 409 Conflict to handle parallel test processes racing to patch
// the same deployment.
func SetWebhookRequireWifConfigmap(ctx context.Context, cs clientset.Interface, enable bool) error {
	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		d, err := cs.AppsV1().Deployments(webhookDeploymentNamespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("getting webhook deployment: %w", err)
		}

		changed := false
		for i, c := range d.Spec.Template.Spec.Containers {
			if c.Name != webhookContainerName {
				continue
			}
			// Build new args without any existing require-wif flag variant.
			newArgs := make([]string, 0, len(c.Args))
			for _, arg := range c.Args {
				if arg != requireWifFlagArg && !strings.HasPrefix(arg, requireWifFlagArg+"=") {
					newArgs = append(newArgs, arg)
				} else {
					changed = true
				}
			}
			if enable {
				newArgs = append(newArgs, requireWifFlagArg+"=true")
				changed = true
			}
			d.Spec.Template.Spec.Containers[i].Args = newArgs
		}

		if !changed && !enable {
			// Flag was already absent and we want it absent: nothing to do.
			return nil
		}

		action := "enabling"
		if !enable {
			action = "disabling"
		}
		klog.Infof("%s %s on webhook deployment %s/%s", action, requireWifFlagArg, webhookDeploymentNamespace, webhookDeploymentName)

		_, err = cs.AppsV1().Deployments(webhookDeploymentNamespace).Update(ctx, d, metav1.UpdateOptions{})
		if err == nil {
			return waitForWebhookRollout(ctx, cs)
		}
		if !errors.IsConflict(err) {
			return fmt.Errorf("updating webhook deployment: %w", err)
		}
		klog.Warningf("conflict updating webhook deployment (attempt %d/%d), retrying...", attempt+1, maxRetries)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("updating webhook deployment: exceeded %d retries due to conflicts", maxRetries)
}

// waitForWebhookRollout polls until the webhook deployment has fully rolled out
// (all replicas updated and available).
func waitForWebhookRollout(ctx context.Context, cs clientset.Interface) error {
	return wait.PollUntilContextTimeout(ctx, webhookRolloutPollInterval, webhookRolloutTimeout, true,
		func(ctx context.Context) (bool, error) {
			d, err := cs.AppsV1().Deployments(webhookDeploymentNamespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("polling webhook deployment status: %v", err)
				return false, nil
			}
			desired := d.Spec.Replicas
			if desired == nil {
				return false, nil
			}
			if d.Status.UpdatedReplicas == *desired &&
				d.Status.AvailableReplicas == *desired &&
				d.Status.ObservedGeneration >= d.Generation {
				return true, nil
			}
			klog.V(4).Infof("webhook rollout in progress: updated=%d available=%d desired=%d",
				d.Status.UpdatedReplicas, d.Status.AvailableReplicas, *desired)
			return false, nil
		},
	)
}
