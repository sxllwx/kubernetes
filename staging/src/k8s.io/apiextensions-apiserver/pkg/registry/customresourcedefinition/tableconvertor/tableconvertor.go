/*
Copyright 2018 The Kubernetes Authors.

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

package tableconvertor

import (
	"context"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// New creates a new table convertor for the provided CRD column definition. If the printer definition cannot be parsed,
// error will be returned along with a default table convertor.
func New() rest.TableConvertor {

	headers := []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Description: swaggerMetadataDescriptions["name"]},
		{Name: "Created At", Type: "date", Description: swaggerMetadataDescriptions["creationTimestamp"]},
		{Name: "Status", Type: "string", Description: "The aggregate status of the conditions in this crd."},
	}
	c := &convertor{
		headers: headers,
	}
	return c
}

type convertor struct {
	headers []metav1.TableColumnDefinition
}

func (c *convertor) ConvertToTable(ctx context.Context, obj runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	table := &metav1.Table{}
	opt, ok := tableOptions.(*metav1.TableOptions)
	noHeaders := ok && opt != nil && opt.NoHeaders
	if !noHeaders {
		table.ColumnDefinitions = c.headers
	}

	if m, err := meta.ListAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := meta.CommonAccessor(obj); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}

	if !meta.IsListType(obj) {
		rows, err := printCustomResourceDefinition(obj)
		if err != nil {
			return nil, err
		}
		table.Rows = append(table.Rows, rows...)
		return table, nil
	}

	list, _ := obj.(*apiextensionsv1.CustomResourceDefinitionList)

	rows := make([]metav1.TableRow, 0, len(list.Items))
	for i := range list.Items {
		r, err := printCustomResourceDefinition(&list.Items[i])
		if err != nil {
			return nil, err
		}
		rows = append(rows, r...)
	}
	table.Rows = rows
	return table, nil
}

func printCustomResourceDefinition(obj runtime.Object) ([]metav1.TableRow, error) {
	crd, _ := obj.(*apiextensionsv1.CustomResourceDefinition)
	row := metav1.TableRow{
		Object: runtime.RawExtension{Object: obj},
	}
	print(crd.GetName())
	estableshedStatus := extractCRDConditions(crd.Status)
	row.Cells = append(row.Cells, crd.Name, crd.CreationTimestamp, estableshedStatus)
	return []metav1.TableRow{row}, nil
}

func extractCRDConditions(status apiextensionsv1.CustomResourceDefinitionStatus) string {

	var accepted, established, terminating bool
	if len(status.Conditions) == 0 {
		return "<unknown>"
	}

	for _, c := range status.Conditions {
		switch c.Type {
		case apiextensionsv1.NamesAccepted:
			if c.Status == apiextensionsv1.ConditionTrue {
				accepted = true
			}
		case apiextensionsv1.Established:
			if c.Status == apiextensionsv1.ConditionTrue {
				established = true
			}
		case apiextensionsv1.Terminating:
			if c.Status == apiextensionsv1.ConditionTrue {
				terminating = true
			}
		}
	}

	if terminating {
		return "Terminating"
	}

	var ret string
	if accepted {
		ret += "NamesAccepted"
	}
	if established {
		ret += ",Established"
	} else {
		ret += ",Pending"
	}
	if terminating {
		ret += ", Terminating"
	}
	return ret
}
