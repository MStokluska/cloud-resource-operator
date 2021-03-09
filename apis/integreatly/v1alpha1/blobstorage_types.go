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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// BlobStorageSpec defines the desired state of BlobStorage
type BlobStorageSpec types.ResourceTypeSpec

// BlobStorageStatus defines the observed state of BlobStorage
type BlobStorageStatus types.ResourceTypeStatus

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// BlobStorage is the Schema for the blobstorages API
type BlobStorage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BlobStorageSpec   `json:"spec,omitempty"`
	Status BlobStorageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BlobStorageList contains a list of BlobStorage
type BlobStorageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BlobStorage `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BlobStorage{}, &BlobStorageList{})
}
