/*
Copyright 2018 The Kubernetes Authors.
Copyright 2022 Google LLC

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
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	DriverNamespace = "gcs-fuse-csi-driver"

	webhookDeploymentName      = "gcs-fuse-csi-driver-webhook"
	webhookContainerName       = "gcs-fuse-csi-driver-webhook"
	webhookRolloutPollInterval = 5 * time.Second
	webhookRolloutTimeout      = 5 * time.Minute

	requireWIFArgTrue  = "--require-wif-credential-configmap=true"
	requireWIFArgFalse = "--require-wif-credential-configmap=false"

	requireAppCredsArgTrue  = "--require-application-credentials=true"
	requireAppCredsArgFalse = "--require-application-credentials=false"
)

// GetWebhookWIFEnforcement reads the current enforcement state from the webhook
// deployment. Returns true only when BOTH --require-wif-credential-configmap
// and --require-application-credentials are true.
func GetWebhookWIFEnforcement(ctx context.Context, client clientset.Interface, namespace string) (bool, error) {
	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to get webhook deployment %s/%s: %w", namespace, webhookDeploymentName, err)
	}
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == webhookContainerName {
			wif := false
			appCreds := false
			for _, a := range c.Args {
				if a == requireWIFArgTrue {
					wif = true
				}
				if a == requireAppCredsArgTrue {
					appCreds = true
				}
			}
			return wif && appCreds, nil
		}
	}
	return false, fmt.Errorf("container %q not found in webhook deployment", webhookContainerName)
}

// SetWebhookWIFEnforcement sets BOTH --require-wif-credential-configmap and
// --require-application-credentials on the webhook deployment to the same value
// and waits for the rollout to complete.
//
// The two flags are always toggled together:
//   - enabled=true  → both flags true  (full WIF enforcement)
//   - enabled=false → both flags false (normal operation, non-WIF pods unaffected)
//
// If the deployment already has the desired value for both flags, the function
// returns immediately without patching to avoid a spurious rollout wait.
func SetWebhookWIFEnforcement(ctx context.Context, client clientset.Interface, namespace string, enabled bool) error {
	klog.Infof("Setting webhook WIF enforcement to %v", enabled)

	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get webhook deployment %s/%s: %w", namespace, webhookDeploymentName, err)
	}

	// Short-circuit: if both flags are already at the desired value, skip patch.
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == webhookContainerName {
			currentWIF := false
			currentAppCreds := false
			for _, a := range c.Args {
				if a == requireWIFArgTrue {
					currentWIF = true
				}
				if a == requireAppCredsArgTrue {
					currentAppCreds = true
				}
			}
			if currentWIF == enabled && currentAppCreds == enabled {
				klog.Infof("Webhook WIF enforcement already %v, no patch needed", enabled)
				return nil
			}
			break
		}
	}

	updated := deploy.DeepCopy()
	containerIdx := -1
	for i, c := range updated.Spec.Template.Spec.Containers {
		if c.Name == webhookContainerName {
			containerIdx = i
			break
		}
	}
	if containerIdx == -1 {
		return fmt.Errorf("container %q not found in webhook deployment", webhookContainerName)
	}

	updated.Spec.Template.Spec.Containers[containerIdx].Args = setOrReplaceEnforcementArgs(
		updated.Spec.Template.Spec.Containers[containerIdx].Args,
		enabled,
	)

	patch, err := buildArgPatch(updated.Spec.Template.Spec.Containers)
	if err != nil {
		return fmt.Errorf("failed to build patch: %w", err)
	}

	if _, err := client.AppsV1().Deployments(namespace).Patch(
		ctx, webhookDeploymentName, types.StrategicMergePatchType, patch, metav1.PatchOptions{},
	); err != nil {
		return fmt.Errorf("failed to patch webhook deployment: %w", err)
	}

	return waitForWebhookRollout(ctx, client, namespace, deploy.Generation+1)
}

// setOrReplaceEnforcementArgs removes any existing --require-wif-credential-configmap=*
// and --require-application-credentials=* entries and appends both with the new value.
func setOrReplaceEnforcementArgs(args []string, enabled bool) []string {
	filtered := make([]string, 0, len(args))
	for _, a := range args {
		if a == requireWIFArgTrue || a == requireWIFArgFalse ||
			a == requireAppCredsArgTrue || a == requireAppCredsArgFalse {
			continue
		}
		filtered = append(filtered, a)
	}
	if enabled {
		return append(filtered, requireWIFArgTrue, requireAppCredsArgTrue)
	}
	return append(filtered, requireWIFArgFalse, requireAppCredsArgFalse)
}

// buildArgPatch produces a strategic-merge-patch that updates only the container args.
func buildArgPatch(containers []corev1.Container) ([]byte, error) {
	type containerPatch struct {
		Name string   `json:"name"`
		Args []string `json:"args"`
	}
	type specPatch struct {
		Containers []containerPatch `json:"containers"`
	}
	type templatePatch struct {
		Spec specPatch `json:"spec"`
	}
	type deployPatch struct {
		Spec struct {
			Template templatePatch `json:"template"`
		} `json:"spec"`
	}

	dp := deployPatch{}
	for _, c := range containers {
		dp.Spec.Template.Spec.Containers = append(
			dp.Spec.Template.Spec.Containers,
			containerPatch{Name: c.Name, Args: c.Args},
		)
	}
	return json.Marshal(dp)
}

// waitForWebhookRollout blocks until the webhook deployment has fully rolled out
// (all replicas updated and available) or the timeout is reached.
func waitForWebhookRollout(ctx context.Context, client clientset.Interface, namespace string, minGeneration int64) error {
	return wait.PollUntilContextTimeout(ctx, webhookRolloutPollInterval, webhookRolloutTimeout, true,
		func(ctx context.Context) (bool, error) {
			d, err := client.AppsV1().Deployments(namespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
			if err != nil {
				return false, nil //nolint:nilerr
			}
			if !isDeploymentRolledOut(d, minGeneration) {
				klog.V(4).Infof("Waiting for webhook rollout: generation=%d observedGeneration=%d updated=%d available=%d desired=%d",
					d.Generation, d.Status.ObservedGeneration,
					d.Status.UpdatedReplicas, d.Status.AvailableReplicas,
					*d.Spec.Replicas)
				return false, nil
			}
			klog.Infof("Webhook deployment rolled out successfully")
			return true, nil
		},
	)
}

func isDeploymentRolledOut(d *appsv1.Deployment, minGeneration int64) bool {
	if d.Status.ObservedGeneration < minGeneration {
		return false
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return d.Status.UpdatedReplicas == desired &&
		d.Status.AvailableReplicas == desired &&
		d.Status.ReadyReplicas == desired
}
