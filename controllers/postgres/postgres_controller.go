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

package postgres

import (
	"context"
	"fmt"

	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	"k8s.io/client-go/kubernetes"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers/openshift"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/sirupsen/logrus"

	"github.com/integr8ly/cloud-resource-operator/apis/v1alpha1"
	errorUtil "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
)

// PostgresReconciler reconciles a Postgres object
type PostgresReconciler struct {
	client           client.Client
	scheme           *runtime.Scheme
	logger           *logrus.Entry
	resourceProvider *resources.ReconcileResourceProvider
	providerList     []providers.PostgresProvider
}

// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=postgres,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=integreatly.integreatly.org,resources=postgres/status,verbs=get;update;patch

func (r *PostgresReconciler) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling Postgres")
	ctx := context.TODO()
	cfgMgr := providers.NewConfigManager(providers.DefaultProviderConfigMapName, request.Namespace, r.client)

	// Fetch the Postgres instance
	instance := &v1alpha1.Postgres{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
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

	stratMap, err := cfgMgr.GetStrategyMappingForDeploymentType(ctx, instance.Spec.Type)
	if err != nil {
		if updateErr := resources.UpdatePhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusDeploymentConfigNotFound.WrapError(err)); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, errorUtil.Wrapf(err, "failed to read deployment type config for deployment %s", instance.Spec.Type)
	}

	// Check the CR for existing Strategy
	strategyToUse := stratMap.Postgres
	if instance.Status.Strategy != "" {
		strategyToUse = instance.Status.Strategy
		if strategyToUse != stratMap.Postgres {
			r.logger.Infof("strategy and provider already set, changing of cloud-resource-config config maps not allowed in existing installation. the existing strategy is '%s' , cloud-resource-config is now set to '%s'. operator will continue to use existing strategy", strategyToUse, stratMap.Postgres)
		}
	}

	for _, p := range r.providerList {
		if !p.SupportsStrategy(strategyToUse) {
			continue
		}
		instance.Status.Strategy = strategyToUse
		instance.Status.Provider = p.GetName()
		if instance.Status.Strategy != strategyToUse || instance.Status.Provider != p.GetName() {
			if err = r.client.Status().Update(ctx, instance); err != nil {
				return reconcile.Result{}, errorUtil.Wrapf(err, "failed to update instance %s in namespace %s", instance.Name, instance.Namespace)
			}
		}

		// delete the postgres if the deletion timestamp exists
		if instance.DeletionTimestamp != nil {
			msg, err := p.DeletePostgres(ctx, instance)
			if err != nil {
				if updateErr := resources.UpdatePhase(ctx, r.client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
					return reconcile.Result{}, updateErr
				}
				return reconcile.Result{}, errorUtil.Wrapf(err, "failed to perform provider-specific storage deletion")
			}

			r.logger.Info("waiting on Postgres to successfully delete")
			if err = resources.UpdatePhase(ctx, r.client, instance, croType.PhaseDeleteInProgress, msg); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// handle skip create
		if instance.Spec.SkipCreate {
			r.logger.Info("skipCreate found, skipping postgres reconcile")
			if err := resources.UpdatePhase(ctx, r.client, instance, croType.PhasePaused, croType.StatusSkipCreate); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// create the postgres instance
		ps, msg, err := p.CreatePostgres(ctx, instance)
		if err != nil {
			instance.Status.SecretRef = &croType.SecretRef{}
			if updateErr := resources.UpdatePhase(ctx, r.client, instance, croType.PhaseFailed, msg.WrapError(err)); updateErr != nil {
				return reconcile.Result{}, updateErr
			}
			return reconcile.Result{}, err
		}
		if ps == nil {
			r.logger.Info("secret data is still reconciling, postgres instance is nil")
			instance.Status.SecretRef = &croType.SecretRef{}
			if err = resources.UpdatePhase(ctx, r.client, instance, croType.PhaseInProgress, msg); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
		}

		// return the connection secret
		if err := r.resourceProvider.ReconcileResultSecret(ctx, instance, ps.DeploymentDetails.Data()); err != nil {
			return reconcile.Result{}, errorUtil.Wrap(err, "failed to reconcile secret")
		}

		instance.Status.Phase = croType.PhaseComplete
		instance.Status.Message = msg
		instance.Status.SecretRef = instance.Spec.SecretRef
		instance.Status.Strategy = strategyToUse
		instance.Status.Provider = p.GetName()
		if err = r.client.Status().Update(ctx, instance); err != nil {
			return reconcile.Result{}, errorUtil.Wrapf(err, "failed to update instance %s in namespace %s", instance.Name, instance.Namespace)
		}
		return reconcile.Result{Requeue: true, RequeueAfter: p.GetReconcileTime(instance)}, nil
	}

	// unsupported strategy
	if err = resources.UpdatePhase(ctx, r.client, instance, croType.PhaseFailed, croType.StatusUnsupportedType.WrapError(err)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, errorUtil.New(fmt.Sprintf("unsupported deployment strategy %s", stratMap.Postgres))
}

func (r *PostgresReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.Postgres{}).
		Complete(r)
}
