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
)

// GetWebhookWIFEnforcement reads the current --require-wif-credential-configmap value
// from the webhook deployment and returns it as a bool. Returns false if the arg is
// absent (i.e. the default).
func GetWebhookWIFEnforcement(ctx context.Context, client clientset.Interface, namespace string) (bool, error) {
	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to get webhook deployment %s/%s: %w", namespace, webhookDeploymentName, err)
	}
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == webhookContainerName {
			for _, a := range c.Args {
				if a == requireWIFArgTrue {
					return true, nil
				}
			}
			return false, nil
		}
	}
	return false, fmt.Errorf("container %q not found in webhook deployment", webhookContainerName)
}

// SetWebhookWIFEnforcement sets --require-wif-credential-configmap on the webhook
// deployment and waits for the rollout to complete. Callers must supply both the
// driver namespace (e.g. "gcs-fuse-csi-driver") and a live clientset.
// If the deployment already has the desired value, the function returns immediately
// without patching to avoid a spurious generation increment and rollout wait.
func SetWebhookWIFEnforcement(ctx context.Context, client clientset.Interface, namespace string, enabled bool) error {
	targetArg := requireWIFArgFalse
	if enabled {
		targetArg = requireWIFArgTrue
	}
	klog.Infof("Setting webhook WIF enforcement to %v (arg: %s)", enabled, targetArg)

	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get webhook deployment %s/%s: %w", namespace, webhookDeploymentName, err)
	}

	// Short-circuit: if the arg is already at the desired value, skip the patch and wait.
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == webhookContainerName {
			current := false
			for _, a := range c.Args {
				if a == requireWIFArgTrue {
					current = true
					break
				}
			}
			if current == enabled {
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

	updated.Spec.Template.Spec.Containers[containerIdx].Args = setOrReplaceWIFArg(
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

// setOrReplaceWIFArg removes any existing --require-wif-credential-configmap=*
// entry and appends the new value.
func setOrReplaceWIFArg(args []string, enabled bool) []string {
	filtered := make([]string, 0, len(args))
	for _, a := range args {
		if a != requireWIFArgTrue && a != requireWIFArgFalse {
			filtered = append(filtered, a)
		}
	}
	if enabled {
		return append(filtered, requireWIFArgTrue)
	}
	return append(filtered, requireWIFArgFalse)
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
