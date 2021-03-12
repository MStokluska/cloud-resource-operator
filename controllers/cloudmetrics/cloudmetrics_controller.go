// This controller reconciles metrics for cloud resources (currently redis and postgres)
// It takes a sync the world approach, reconciling all cloud resources every set period
// of time (currently every 5 minutes)
package cloudmetrics

import (
	"context"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	customMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	postgresFreeStorageAverage    = "cro_postgres_free_storage_average"
	postgresCPUUtilizationAverage = "cro_postgres_cpu_utilization_average"
	postgresFreeableMemoryAverage = "cro_postgres_freeable_memory_average"

	redisMemoryUsagePercentageAverage = "cro_redis_memory_usage_percentage_average"
	redisFreeableMemoryAverage        = "cro_redis_freeable_memory_average"
	redisCPUUtilizationAverage        = "cro_redis_cpu_utilization_average"
	redisEngineCPUUtilizationAverage  = "cro_redis_engine_cpu_utilization_average"

	labelClusterIDKey   = "clusterID"
	labelResourceIDKey  = "resourceID"
	labelNamespaceKey   = "namespace"
	labelInstanceIDKey  = "instanceID"
	labelProductNameKey = "productName"
	labelStrategyKey    = "strategy"
)

// generic list of label keys used for Gauge Vectors
var labels = []string{
	labelClusterIDKey,
	labelResourceIDKey,
	labelNamespaceKey,
	labelInstanceIDKey,
	labelProductNameKey,
	labelStrategyKey,
}

// CroGaugeMetric allows for a mapping between an exposed prometheus metric and multiple cloud provider specific metric
type CroGaugeMetric struct {
	Name         string
	GaugeVec     *prometheus.GaugeVec
	ProviderType map[string]providers.CloudProviderMetricType
}

// postgresGaugeMetrics stores a mapping between an exposed (postgres) prometheus metric and multiple cloud provider specific metric
// to add any addition metrics simply add to this mapping and it will be scraped and exposed
var postgresGaugeMetrics = []CroGaugeMetric{
	{
		Name: postgresFreeStorageAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: postgresFreeStorageAverage,
				Help: "The amount of available storage space. Units: Bytes",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: postgresFreeStorageAverage,
				ProviderMetricName:   "FreeStorageSpace",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
	{
		Name: postgresCPUUtilizationAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: postgresCPUUtilizationAverage,
				Help: "The percentage of CPU utilization. Units: Percent",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: postgresCPUUtilizationAverage,
				ProviderMetricName:   "CPUUtilization",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
	{
		Name: postgresFreeableMemoryAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: postgresFreeableMemoryAverage,
				Help: "The amount of available random access memory. Units: Bytes",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: postgresFreeableMemoryAverage,
				ProviderMetricName:   "FreeableMemory",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
}

// redisGaugeMetrics stores a mapping between an exposed (redis) prometheus metric and multiple cloud provider specific metric
// to add any addition metrics simply add to this mapping and it will be scraped and exposed
var redisGaugeMetrics = []CroGaugeMetric{
	{
		Name: redisMemoryUsagePercentageAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: redisMemoryUsagePercentageAverage,
				Help: "The percentage of redis used memory. Units: Bytes",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: redisMemoryUsagePercentageAverage,
				//calculated on used_memory/maxmemory from Redis INFO http://redis.io/commands/info
				ProviderMetricName: "DatabaseMemoryUsagePercentage",
				Statistic:          cloudwatch.StatisticAverage,
			},
		},
	},
	{
		Name: redisFreeableMemoryAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: redisFreeableMemoryAverage,
				Help: "The amount of available random access memory. Units: Bytes",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: redisFreeableMemoryAverage,
				ProviderMetricName:   "FreeableMemory",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
	{
		Name: redisCPUUtilizationAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: redisCPUUtilizationAverage,
				Help: "The percentage of CPU utilization. Units: Percent",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: redisCPUUtilizationAverage,
				ProviderMetricName:   "CPUUtilization",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
	{
		Name: redisEngineCPUUtilizationAverage,
		GaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: redisEngineCPUUtilizationAverage,
				Help: "The percentage of CPU utilization. Units: Percent",
			},
			labels),
		ProviderType: map[string]providers.CloudProviderMetricType{
			providers.AWSDeploymentStrategy: {
				PromethuesMetricName: redisEngineCPUUtilizationAverage,
				ProviderMetricName:   "EngineCPUUtilization",
				Statistic:            cloudwatch.StatisticAverage,
			},
		},
	},
}

// blank assignment to verify that ReconcileCloudMetrics implements reconcile.Reconciler
var _ reconcile.Reconciler = &CloudMetricsReconciler{}

// PostgresReconciler reconciles a Postgres object
type CloudMetricsReconciler struct {
	k8sclient.Client
	scheme               *runtime.Scheme
	logger               *logrus.Entry
	postgresProviderList []providers.PostgresMetricsProvider
	redisProviderList    []providers.RedisMetricsProvider
}

// New returns a new reconcile.Reconciler
func New(mgr manager.Manager) *CloudMetricsReconciler {
	c := mgr.GetClient()
	logger := logrus.WithFields(logrus.Fields{"controller": "controller_cloudmetrics"})
	postgresProviderList := []providers.PostgresMetricsProvider{aws.NewAWSPostgresMetricsProvider(c, logger)}
	redisProviderList := []providers.RedisMetricsProvider{aws.NewAWSRedisMetricsProvider(c, logger)}

	// we only wish to register metrics once when the new reconciler is created
	// as the metrics we want to expose are known in advance we can register them all
	// they will only be exposed if there is a value returned for the vector for a provider
	registerGaugeVectorMetrics(logger)
	return &CloudMetricsReconciler{
		Client:               mgr.GetClient(),
		scheme:               mgr.GetScheme(),
		logger:               logger,
		postgresProviderList: postgresProviderList,
		redisProviderList:    redisProviderList,
	}
}

func (r *CloudMetricsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Set up a GenericEvent channel that will be used
	// as the event source to trigger the controller's
	// reconcile loop
	events := make(chan event.GenericEvent)

	// Send a generic event to the channel to kick
	// off the first reconcile loop
	go func() {
		events <- event.GenericEvent{
			Meta:   &integreatlyv1alpha1.Redis{},
			Object: &integreatlyv1alpha1.Redis{},
		}
	}()

	return ctrl.NewControllerManagedBy(mgr).
		For(&integreatlyv1alpha1.Redis{}).
		For(&integreatlyv1alpha1.Postgres{}).
		Watches(&source.Channel{Source: events}, &handler.EnqueueRequestForObject{}).
		Complete(r)
}

// ClusterRole permissions

// +kubebuilder:rbac:groups="config.openshift.io",resources=infrastructures;networks,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumes;configmaps,verbs="*"
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=prometheusrules,verbs="*"

// Role permissions

// +kubebuilder:rbac:groups="",resources=pods;pods/exec;services;services/finalizers;endpoints;persistentVolumeclaims;events;configmaps;secrets,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resources=deployments;daemonsets;replicasets;statefulsets,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=servicemonitors,verbs=get;create,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="cloud-resource-operator",resources=deployments/finalizers,verbs=update,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="",resources=pods,verbs=get,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resources="*",verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resources="replicasets",verbs=get,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="apps",resourceNames="cloud-resource-operator",resources="deployments/finalizers",verbs=update,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="integreatly",resources="*",verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="integreatly.org",resources="*";smtpcredentialset;redis;postgres;redissnapshots;postgressnapshots,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=prometheusrules,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="config.openshift.io",resources="*";infrastructures;schedulers;featuregates;networks;ingresses;clusteroperators;authentications;builds,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups="cloudcredential.openshift.io",resources=credentialsrequests,verbs="*",namespace=cloud-resource-operator
// +kubebuilder:rbac:groups=integreatly.org,resources=blobstorages,verbs=get;list;watch;create;update;patch;delete,namespace=cloud-resource-operator
// +kubebuilder:rbac:groups=integreatly.org,resources=blobstorages/status,verbs=get;update;patch,namespace=cloud-resource-operator

func (r *CloudMetricsReconciler) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	r.logger.Info("reconciling CloudMetrics")
	ctx := context.Background()

	// scrapedMetrics stores the GenericCloudMetric which are returned from the providers
	var scrapedMetrics []*providers.GenericCloudMetric

	// fetch all redis crs
	redisInstances := &integreatlyv1alpha1.RedisList{}
	err := r.Client.List(ctx, redisInstances)
	if err != nil {
		return ctrl.Result{}, err
	}

	// loop through the redis crs and scrape the related provider specific metrics
	for _, redis := range redisInstances.Items {
		r.logger.Infof("beginning to scrape metrics for redis cr: %s", redis.Name)
		for _, p := range r.redisProviderList {
			// only scrape metrics on supported strategies
			if !p.SupportsStrategy(redis.Status.Strategy) {
				continue
			}
			var redisMetricTypes []providers.CloudProviderMetricType
			for _, gaugeMetric := range redisGaugeMetrics {
				for provider, metricType := range gaugeMetric.ProviderType {
					if provider == redis.Status.Strategy {
						redisMetricTypes = append(redisMetricTypes, metricType)
						continue
					}
				}
			}

			// all redis scrapedMetric providers inherit the same interface
			// scrapeMetrics returns scraped metrics output which contains a list of GenericCloudMetrics
			scrapedMetricsOutput, err := p.ScrapeRedisMetrics(ctx, &redis, redisMetricTypes)
			if err != nil {
				r.logger.Errorf("failed to scrape metrics for redis %v", err)
				continue
			}

			scrapedMetrics = append(scrapedMetrics, scrapedMetricsOutput.Metrics...)
		}
	}
	// for each scraped metric value we check redisGaugeMetrics for a match and set the value and labels
	r.setGaugeMetrics(redisGaugeMetrics, scrapedMetrics)

	// Fetch all postgres crs
	postgresInstances := &integreatlyv1alpha1.PostgresList{}
	err = r.Client.List(ctx, postgresInstances)
	if err != nil {
		r.logger.Error(err)
	}
	for _, postgres := range postgresInstances.Items {
		r.logger.Infof("beginning to scrape metrics for postgres cr: %s", postgres.Name)
		for _, p := range r.postgresProviderList {
			// only scrape metrics on supported strategies
			if !p.SupportsStrategy(postgres.Status.Strategy) {
				continue
			}

			// filter out the provider specific metric from the postgresGaugeMetrics map which defines the metrics we want to scrape
			var postgresMetricTypes []providers.CloudProviderMetricType
			for _, gaugeMetric := range postgresGaugeMetrics {
				for provider, metricType := range gaugeMetric.ProviderType {
					if provider == postgres.Status.Strategy {
						postgresMetricTypes = append(postgresMetricTypes, metricType)
						continue
					}
				}
			}

			// all postgres scrapedMetric providers inherit the same interface
			// scrapeMetrics returns scraped metrics output which contains a list of GenericCloudMetrics
			scrapedMetricsOutput, err := p.ScrapePostgresMetrics(ctx, &postgres, postgresMetricTypes)
			if err != nil {
				r.logger.Errorf("failed to scrape metrics for postgres %v", err)
				continue
			}

			// add the returned scraped metrics to the list of metrics
			scrapedMetrics = append(scrapedMetrics, scrapedMetricsOutput.Metrics...)
		}
	}

	// for each scraped metric value we check postgresGaugeMetrics for a match and set the value and labels
	r.setGaugeMetrics(postgresGaugeMetrics, scrapedMetrics)

	// we want full control over when we scrape metrics
	// to allow for this we only have a single requeue
	// this ensures regardless of errors or return times
	// all metrics are scraped and exposed at the same time
	return ctrl.Result{
		RequeueAfter: resources.GetMetricReconcileTimeOrDefault(resources.MetricsWatchDuration),
	}, nil
}

func registerGaugeVectorMetrics(logger *logrus.Entry) {
	for _, metric := range postgresGaugeMetrics {
		logger.Infof("registering metric: %s ", metric.Name)
		customMetrics.Registry.MustRegister(metric.GaugeVec)
	}
	for _, metric := range redisGaugeMetrics {
		logger.Infof("registering metric: %s ", metric.Name)
		customMetrics.Registry.MustRegister(metric.GaugeVec)
	}
}

// func setGaugeMetrics sets the value on exposed metrics with labels
func (r *CloudMetricsReconciler) setGaugeMetrics(gaugeMetrics []CroGaugeMetric, scrapedMetrics []*providers.GenericCloudMetric) {
	for _, scrapedMetric := range scrapedMetrics {
		for _, croMetric := range gaugeMetrics {
			if scrapedMetric.Name == croMetric.Name {
				croMetric.GaugeVec.WithLabelValues(
					scrapedMetric.Labels[labelClusterIDKey],
					scrapedMetric.Labels[labelResourceIDKey],
					scrapedMetric.Labels[labelNamespaceKey],
					scrapedMetric.Labels[labelInstanceIDKey],
					scrapedMetric.Labels[labelProductNameKey],
					scrapedMetric.Labels[labelStrategyKey]).Set(scrapedMetric.Value)
				r.logger.Infof("successfully set metric value for %s", croMetric.Name)
				continue
			}
		}
	}
}
