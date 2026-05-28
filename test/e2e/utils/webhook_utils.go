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
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	webhookDeploymentName          = "gcs-fuse-csi-driver-webhook"
	webhookDeploymentNamespaceOSS  = "gcs-fuse-csi-driver"
	webhookDeploymentNamespaceGKE  = "kube-system"
	requireWIFFlag                 = "--require-wif-credential-configmap=true"
	webhookRolloutTimeout          = 3 * time.Minute
	webhookRolloutPollInterval     = 5 * time.Second
)

// webhookNamespace returns the namespace of the webhook deployment based on
// whether this is an OSS or managed (GKE) cluster.
func webhookNamespace() string {
	if os.Getenv(IsOSSEnvVar) == "true" {
		return webhookDeploymentNamespaceOSS
	}
	return webhookDeploymentNamespaceGKE
}

// SetWebhookWIFEnforcement patches the webhook deployment to enable or disable
// --require-wif-credential-configmap and waits for the rollout to complete.
// Call with enable=true before WIF/OIDC tests, enable=false in the deferred cleanup.
func SetWebhookWIFEnforcement(ctx context.Context, client clientset.Interface, enable bool) error {
	ns := webhookNamespace()

	deploy, err := client.AppsV1().Deployments(ns).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("webhook deployment %s/%s not found, skipping WIF enforcement toggle", ns, webhookDeploymentName)
			return nil
		}
		return fmt.Errorf("failed to get webhook deployment: %w", err)
	}

	updated := patchWebhookArgs(deploy, enable)
	if !updated {
		klog.Infof("webhook deployment %s/%s args already in desired state (enable=%v), skipping patch", ns, webhookDeploymentName, enable)
		return nil
	}

	if _, err = client.AppsV1().Deployments(ns).Update(ctx, deploy, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update webhook deployment: %w", err)
	}

	action := "enabled"
	if !enable {
		action = "disabled"
	}
	klog.Infof("WIF enforcement %s on webhook deployment %s/%s, waiting for rollout...", action, ns, webhookDeploymentName)

	return waitForWebhookRollout(ctx, client, ns)
}

// patchWebhookArgs adds or removes --require-wif-credential-configmap=true from
// the first container's args in the deployment. Returns true if a change was made.
func patchWebhookArgs(deploy *appsv1.Deployment, enable bool) bool {
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return false
	}
	args := deploy.Spec.Template.Spec.Containers[0].Args

	// Check current state.
	idx := -1
	for i, a := range args {
		if a == requireWIFFlag {
			idx = i
			break
		}
	}

	if enable && idx == -1 {
		// Add the flag.
		deploy.Spec.Template.Spec.Containers[0].Args = append(args, requireWIFFlag)
		return true
	}
	if !enable && idx != -1 {
		// Remove the flag.
		deploy.Spec.Template.Spec.Containers[0].Args = append(args[:idx], args[idx+1:]...)
		return true
	}
	return false
}

// waitForWebhookRollout polls until the webhook deployment has fully rolled out
// (all replicas updated and available).
func waitForWebhookRollout(ctx context.Context, client clientset.Interface, ns string) error {
	ns = webhookNamespace()
	return wait.PollUntilContextTimeout(ctx, webhookRolloutPollInterval, webhookRolloutTimeout, true,
		func(ctx context.Context) (bool, error) {
			deploy, err := client.AppsV1().Deployments(ns).Get(ctx, webhookDeploymentName, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("failed to get webhook deployment during rollout wait: %v", err)
				return false, nil
			}
			desired := deploy.Spec.Replicas
			if desired == nil {
				return false, nil
			}
			ready := deploy.Status.UpdatedReplicas == *desired &&
				deploy.Status.ReadyReplicas == *desired &&
				deploy.Status.AvailableReplicas == *desired &&
				deploy.Status.ObservedGeneration >= deploy.Generation
			return ready, nil
		},
	)
}
