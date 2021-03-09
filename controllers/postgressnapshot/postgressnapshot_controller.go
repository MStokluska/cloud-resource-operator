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

package postgressnapshot

import (
	"context"
	"fmt"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	croAws "github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	errorUtil "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// PostgresSnapshotReconciler reconciles a PostgresSnapshot object
type PostgresSnapshotReconciler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client            client.Client
	scheme            *runtime.Scheme
	logger            *logrus.Entry
	provider          providers.PostgresSnapshotProvider
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
// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=postgressnapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=postgressnapshots/status,verbs=get;update;patch

func (r *PostgresSnapshotReconciler) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling postgres snapshot")
	ctx := context.TODO()

	// Fetch the PostgresSnapshot instance
	instance := &integreatlyv1alpha1.PostgresSnapshot{}
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

	// set info metric
	defer r.exposePostgresSnapshotMetrics(ctx, instance)

	// get postgres cr
	postgresCr := &integreatlyv1alpha1.Postgres{}
	err = r.client.Get(ctx, types.NamespacedName{Name: instance.Spec.ResourceName, Namespace: instance.Namespace}, postgresCr)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get postgres resource: %s", err.Error())
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, errorUtil.New(errMsg)
	}

	// check postgres deployment strategy is aws
	if !r.provider.SupportsStrategy(postgresCr.Status.Strategy) {
		errMsg := fmt.Sprintf("the resource %s uses an unsupported provider strategy %s, only resources using the aws provider are valid", instance.Spec.ResourceName, postgresCr.Status.Strategy)
		if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusMessage(errMsg)); updateErr != nil {
			return reconcile.Result{Requeue: true, RequeueAfter: r.provider.GetReconcileTime(instance)}, updateErr
		}
		return reconcile.Result{}, errorUtil.New(errMsg)
	}

	if instance.DeletionTimestamp != nil {
		msg, err := r.provider.DeletePostgresSnapshot(ctx, instance, postgresCr)
		if err != nil {
			if updateErr := resources.UpdateSnapshotPhase(ctx, r.client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, errorUtil.Wrapf(err, "failed to delete postgres snapshot")
		}

		r.logger.Info("waiting on postgres snapshot to successfully delete")
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
	snap, msg, err := r.provider.CreatePostgresSnapshot(ctx, instance, postgresCr)

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

func buildPostgresSnapshotStatusMetricLabels(cr *integreatlyv1alpha1.PostgresSnapshot, clusterID, snapshotName string, phase croType.StatusPhase) map[string]string {
	labels := map[string]string{}
	labels["clusterID"] = clusterID
	labels["resourceID"] = cr.Name
	labels["namespace"] = cr.Namespace
	labels["instanceID"] = snapshotName
	labels["productName"] = cr.Labels["productName"]
	labels["strategy"] = postgresProviderName
	labels["statusPhase"] = string(phase)
	return labels
}

func (r *PostgresSnapshotReconciler) exposePostgresSnapshotMetrics(ctx context.Context, cr *integreatlyv1alpha1.PostgresSnapshot) {
	// build instance name
	snapshotName := cr.Status.SnapshotID

	// get Cluster Id
	logrus.Info("setting postgres snapshot information metric")
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
		labelsFailed := buildPostgresSnapshotStatusMetricLabels(cr, clusterID, snapshotName, phase)
		resources.SetMetric(resources.DefaultPostgresSnapshotStatusMetricName, labelsFailed, resources.Btof64(cr.Status.Phase == phase))
	}
}

func (r *PostgresSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.PostgresSnapshot{}).
		Complete(r)
}
