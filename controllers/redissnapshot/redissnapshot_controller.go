/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redissnapshot

import (
	"context"

	"fmt"

	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	croAws "github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
	errorUtil "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
)

const (
	redisProviderName = "aws-elasticache"
)

// RedisSnapshotReconciler reconciles a RedisSnapshot object
type RedisSnapshotReconciler struct {
	client            client.Client
	scheme            *runtime.Scheme
	logger            *logrus.Entry
	provider          providers.RedisSnapshotProvider
	ConfigManager     croAws.ConfigManager
	CredentialManager croAws.CredentialManager
}

// +kubebuilder:rbac:groups="",resources=pods;pods/exec;services;services/finalizers;endpoints;persistentVolumeclaims;events;configmaps;secrets,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resources=deployments;daemonsets;replicasets;statefulsets,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=servicemonitors,verbs=get;create,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="cloud-resource-operator",resources=deployments/finalizers,verbs=update,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="",resources=pods,verbs=get,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resources='*',verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="integreatly",resources='*',verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="integreatly.org",resources='*';smtpcredentialset;redis;postgres;redissnapshots;postgressnapshots,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=prometheusrules,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="config.openshift.io",resources='*';infrastructures;schedulers;featuregates;networks;ingresses;clusteroperators;authentications;builds,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="cloudcredential.openshift.io",resources=credentialsrequests,verbs='*',namespace=cloud-resource-operator
// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=redissnapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=redissnapshots/status,verbs=get;update;patch

func (r *RedisSnapshotReconciler) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling redis snapshot")
	ctx := context.TODO()

	// Fetch the RedisSnapshot instance
	instance := &integreatlyv1alpha1.RedisSnapshot{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// generate info metrics
	defer r.exposeRedisSnapshotMetrics(ctx, instance)

	// get redis cr
	redisCr := &integreatlyv1alpha1.Redis{}
	err = r.client.Get(ctx, types.NamespacedName{Name: instance.Spec.ResourceName, Namespace: instance.Namespace}, redisCr)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get redis cr : %s", err.Error())
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, errorUtil.New(errMsg)
	}

	// check redis cr deployment type is aws
	if !r.provider.SupportsStrategy(redisCr.Status.Strategy) {
		errMsg := fmt.Sprintf("the resource %s uses an unsupported provider strategy %s, only resources using the aws provider are valid", instance.Spec.ResourceName, redisCr.Status.Strategy)
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, errorUtil.New(errMsg)
	}

	if instance.DeletionTimestamp != nil {
		msg, err := r.provider.DeleteRedisSnapshot(ctx, instance, redisCr)
		if err != nil {
			if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, errorUtil.Wrapf(err, "failed to delete redis snapshot")
		}

		r.logger.Info("waiting on redis snapshot to successfully delete")
		if err = resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseDeleteInProgress, msg); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// check status, if complete return
	if instance.Status.Phase == croType.PhaseComplete {
		r.logger.Infof("skipping creation of snapshot for %s as phase is complete", instance.Name)
		return reconcile.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// create the snapshot and return the phase
	snap, msg, err := r.provider.CreateRedisSnapshot(ctx, instance, redisCr)

	// error trying to create snapshot
	if err != nil {
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, msg); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, err
	}

	// no error but the snapshot doesn't exist yet
	if snap == nil {
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseInProgress, msg); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
	}

	// no error, snapshot exists
	if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseComplete, msg); updateErr != nil {
		return reconcile.Result{}, updateErr
	}
	return reconcile.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, nil
}

func buildRedisSnapshotStatusMetricLabels(cr *integreatlyv1alpha1.RedisSnapshot, clusterID, snapshotName string, phase croType.StatusPhase) map[string]string {
	labels := map[string]string{}
	labels["clusterID"] = clusterID
	labels["resourceID"] = cr.Name
	labels["namespace"] = cr.Namespace
	labels["instanceID"] = snapshotName
	labels["productName"] = cr.Labels["productName"]
	labels["strategy"] = redisProviderName
	labels["statusPhase"] = string(phase)
	return labels
}

func (r *RedisSnapshotReconciler) exposeRedisSnapshotMetrics(ctx context.Context, cr *integreatlyv1alpha1.RedisSnapshot) {
	// build instance name
	snapshotName := cr.Status.SnapshotID

	// get Cluster Id
	logrus.Info("setting redis snapshot information metric")
	clusterID, err := resources.GetClusterID(ctx, r.client)
	if err != nil {
		logrus.Errorf("failed to get cluster id while exposing information metric for %v", snapshotName)
		return
	}

	// set generic status metrics
	// a single metric should be exposed for each possible phase
	// the value of the metric should be 1.0 when the resource is in that phase
	// the value of the metric should be 0.0 when the resource is not in that phase
	// this follows the approach that pod status
	for _, phase := range []croType.StatusPhase{croType.PhaseFailed, croType.PhaseDeleteInProgress, croType.PhasePaused, croType.PhaseComplete, croType.PhaseInProgress} {
		labelsFailed := buildRedisSnapshotStatusMetricLabels(cr, clusterID, snapshotName, phase)
		resources.SetMetric(resources.DefaultRedisSnapshotStatusMetricName, labelsFailed, resources.Btof64(cr.Status.Phase == phase))
	}
}

func (r *RedisSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.RedisSnapshot{}).
		Complete(r)
}
