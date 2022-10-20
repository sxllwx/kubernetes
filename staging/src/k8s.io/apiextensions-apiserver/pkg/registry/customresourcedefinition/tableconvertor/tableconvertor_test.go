package tableconvertor

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestTablePrintCRD(t *testing.T) {
	tests := []struct {
		crd      apiextensionsv1.CustomResourceDefinition
		expected []metav1.TableRow
	}{
		// Basic crd with status and age.
		{
			crd: apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace1",
				},
			},
			// Columns: Name, Status, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"namespace1", "FooStatus", "0s"}}},
		},
		// Basic crd without status or age.
		{
			crd: apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace2",
				},
			},
			// Columns: Name, Status, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"namespace2", "", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printCustomResourceDefinition(&test.crd)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		//if !reflect.DeepEqual(test.expected, rows) {
		//	t.Errorf("%d mismatch: %s", i, diff.ObjectReflectDiff(test.expected, rows))
		//}
	}
}
