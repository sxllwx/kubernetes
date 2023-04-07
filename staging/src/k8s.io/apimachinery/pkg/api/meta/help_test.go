/*
Copyright 2015 The Kubernetes Authors.

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

package meta

import (
	"context"
	"reflect"
	goruntime "runtime"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	fakeObjectItemsNum = 1000
	exemptObjectIndex  = fakeObjectItemsNum / 4
)

type SampleSpec struct {
	Flied int
}

type FooSpec struct {
	Flied int
}

type FooList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Foo
}

func (s *FooList) DeepCopyObject() runtime.Object { return nil }

type SampleList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Sample
}

func (s *SampleList) DeepCopyObject() runtime.Object { return nil }

type RawExtensionList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []runtime.RawExtension
}

func (l RawExtensionList) DeepCopyObject() runtime.Object { return nil }

// NOTE: Foo struct itself is the implementer of runtime.Object.
type Foo struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Spec FooSpec
}

func (f Foo) GetObjectKind() schema.ObjectKind {
	tm := f.TypeMeta
	return &tm
}

func (f Foo) DeepCopyObject() runtime.Object { return nil }

// NOTE: the pointer of Sample that is the implementer of runtime.Object.
// the behavior is similar to our corev1.Pod. corev1.Node
type Sample struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Spec SampleSpec
}

func (s *Sample) GetObjectKind() schema.ObjectKind {
	tm := s.TypeMeta
	return &tm
}

func (s *Sample) DeepCopyObject() runtime.Object { return nil }

func fakeSampleList(numItems int) *SampleList {
	out := &SampleList{
		Items: make([]Sample, numItems),
	}

	for i := range out.Items {
		out.Items[i] = Sample{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "foo.org/v1",
				Kind:       "Sample",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"label-key-1": "label-value-1",
				},
				Annotations: map[string]string{
					"annotations-key-1": "annotations-value-1",
				},
			},
			Spec: SampleSpec{
				Flied: i,
			},
		}
	}
	return out
}

func fakeExtensionList(numItems int) *RawExtensionList {
	out := &RawExtensionList{
		Items: make([]runtime.RawExtension, numItems),
	}

	for i := range out.Items {
		out.Items[i] = runtime.RawExtension{
			Object: &Foo{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "sample.org/v1",
					Kind:       "Foo",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      strconv.Itoa(i),
					Namespace: "default",
					Labels: map[string]string{
						"label-key-1": "label-value-1",
					},
					Annotations: map[string]string{
						"annotations-key-1": "annotations-value-1",
					},
				},
				Spec: FooSpec{
					Flied: i,
				},
			},
		}
	}
	return out
}

func fakeUnstructuredList(numItems int) runtime.Unstructured {
	out := &unstructured.UnstructuredList{
		Items: make([]unstructured.Unstructured, numItems),
	}

	for i := range out.Items {
		out.Items[i] = unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"creationTimestamp": nil,
					"name":              strconv.Itoa(i),
				},
				"spec": map[string]interface{}{
					"hostname": "example.com",
				},
				"status": map[string]interface{}{},
			},
		}
	}
	return out
}

func fakeFooList(numItems int) *FooList {
	out := &FooList{
		Items: make([]Foo, numItems),
	}

	for i := range out.Items {
		out.Items[i] = Foo{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "sample.org/v1",
				Kind:       "Foo",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"label-key-1": "label-value-1",
				},
				Annotations: map[string]string{
					"annotations-key-1": "annotations-value-1",
				},
			},
			Spec: FooSpec{
				Flied: i,
			},
		}
	}
	return out
}

func TestMemoryBehaviorExtractList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object, firstObjectPointer interface{})
		numObject       int
		shouldBeCleared bool
	}{
		{
			name: "FooList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeFooList(num)
				return out, &out.Items[0]
			},
			numObject:       fakeObjectItemsNum,
			shouldBeCleared: true,
		},
		{
			name: "SampleList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeSampleList(num)
				return out, &out.Items[0]
			},
			numObject:       fakeObjectItemsNum,
			shouldBeCleared: false,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeExtensionList(num)
				return out, &out.Items[0]
			},
			numObject:       fakeObjectItemsNum,
			shouldBeCleared: true,
			// NOTE:
			// although EachListItem takes the address of the elements of items,
			// but it uses items.Object instead of items.
			// so the items slice will be cleared.
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeUnstructuredList(num)
				unstructuredList := out.(*unstructured.UnstructuredList)
				return out, &unstructuredList.Items[0]
			},
			numObject:       fakeObjectItemsNum,
			shouldBeCleared: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer cancel()
			fakeContainer := map[int]interface{}{}
			cleared := make(chan struct{})
			func() {
				list, firstObjectPointer := tc.generateFunc(tc.numObject)
				goruntime.SetFinalizer(firstObjectPointer, func(obj interface{}) {
					close(cleared)
				})
				items, err := ExtractList(list)
				if err != nil {
					t.Errorf("extract list %#v: %v", list, err)
				}
				for i, item := range items {
					fakeContainer[i] = item
				}
			}()

			for k := range fakeContainer {
				if k == exemptObjectIndex {
					continue
				}
				// clear the object
				delete(fakeContainer, k)
				goruntime.GC()
			}
			select {
			case <-cleared:
				if tc.shouldBeCleared {
					return
				}
				t.Fatal("items first object cleared unexpect")
			case <-ctx.Done():
				return
			}
		})
	}
}

func TestMemoryBehaviorEachList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object, firstObjectPointer interface{})
		expectObjectNum int
		shouldBeCleared bool
	}{
		{
			name: "FooList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeFooList(num)
				return out, &out.Items[0]
			},
			expectObjectNum: fakeObjectItemsNum,
			shouldBeCleared: true,
		},
		{
			name: "SampleList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeSampleList(num)
				return out, &out.Items[0]
			},
			expectObjectNum: fakeObjectItemsNum,
			shouldBeCleared: false,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeExtensionList(num)
				return out, &out.Items[0]
			},
			expectObjectNum: fakeObjectItemsNum,
			// NOTE:
			// although EachListItem takes the address of the elements of items,
			// but it uses items.Object instead of items.
			// so the items slice will be cleared.
			shouldBeCleared: true,
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeUnstructuredList(num)
				unstructuredList := out.(*unstructured.UnstructuredList)
				return out, &unstructuredList.Items[0]
			},
			expectObjectNum: fakeObjectItemsNum,
			shouldBeCleared: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer cancel()
			fakeContainer := map[int]interface{}{}
			cleared := make(chan struct{})
			func() {
				list, firstObjectPointer := tc.generateFunc(tc.expectObjectNum)
				goruntime.SetFinalizer(firstObjectPointer, func(obj interface{}) {
					close(cleared)
				})

				var index = 0
				err := EachListItem(list, func(object runtime.Object) error {
					fakeContainer[index] = object
					index++
					return nil
				})
				if err != nil {
					t.Errorf("extract list %#v: %v", list, err)
				}
			}()

			for k := range fakeContainer {
				if k == exemptObjectIndex {
					continue
				}
				// clear the object
				delete(fakeContainer, k)
				goruntime.GC()
			}
			select {
			case <-cleared:
				if tc.shouldBeCleared {
					return
				}
				t.Fatal("items first object cleared unexpect")
			case <-ctx.Done():
				return
			}
		})
	}
}

func TestMemoryBehaviorExtractListWithAlloc(t *testing.T) {
	tests := []struct {
		name         string
		generateFunc func() (list runtime.Object, firstObjectPointer interface{})
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeExtensionList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer cancel()
			fakeContainer := map[int]interface{}{}
			cleared := make(chan struct{})
			func() {
				list, firstObjectPointer := tc.generateFunc()
				goruntime.SetFinalizer(firstObjectPointer, func(obj interface{}) {
					close(cleared)
				})
				items, err := ExtractListWithAlloc(list)
				if err != nil {
					t.Errorf("extract list %#v: %v", list, err)
				}
				for i, item := range items {
					fakeContainer[i] = item
				}
			}()

			for k := range fakeContainer {
				if k == exemptObjectIndex {
					continue
				}
				// clear the object
				delete(fakeContainer, k)
				goruntime.GC()
			}
			select {
			case <-cleared:
				return
			case <-ctx.Done():
				t.Errorf("ExtractListWithAlloc %s can't be cleared", tc.name)
			}
		})
	}
}

func TestMemoryBehaviorEachListWithAlloc(t *testing.T) {
	tests := []struct {
		name         string
		generateFunc func() (list runtime.Object, firstObjectPointer interface{})
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeExtensionList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "UnstructuredList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := fakeUnstructuredList(fakeObjectItemsNum)
				unstructuredList := out.(*unstructured.UnstructuredList)
				return out, &unstructuredList.Items[0]
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer cancel()
			fakeContainer := map[int]interface{}{}
			cleared := make(chan struct{})
			func() {
				list, firstObjectPointer := tc.generateFunc()
				goruntime.SetFinalizer(firstObjectPointer, func(obj interface{}) {
					close(cleared)
				})

				var index = 0
				err := EachListItemWithAlloc(list, func(object runtime.Object) error {
					fakeContainer[index] = object
					index++
					return nil
				})
				if err != nil {
					t.Errorf("extract list %#v: %v", list, err)
				}
			}()

			for k := range fakeContainer {
				if k == exemptObjectIndex {
					continue
				}
				// clear the object
				delete(fakeContainer, k)
				goruntime.GC()
			}
			select {
			case <-cleared:
				return
			case <-ctx.Done():
				t.Errorf("EachListItemWithAlloc %s can't be cleared", tc.name)
			}
		})
	}
}

func TestEachList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object)
		expectObjectNum int
	}{
		{
			name: "FooList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeFooList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "SampleList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeSampleList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeExtensionList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeUnstructuredList(fakeObjectItemsNum)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("EachListItem", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				err := EachListItem(list, func(object runtime.Object) error {
					o, err := Accessor(object)
					if err != nil {
						return err
					}
					delete(expectObjectNames, o.GetName())
					return nil
				})
				if err != nil {
					t.Errorf("each list item %#v: %v", list, err)
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
			t.Run("EachListItemWithAlloc", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				err := EachListItemWithAlloc(list, func(object runtime.Object) error {
					o, err := Accessor(object)
					if err != nil {
						return err
					}
					delete(expectObjectNames, o.GetName())
					return nil
				})
				if err != nil {
					t.Errorf("each list %#v with alloc: %v", list, err)
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
		})
	}
}

func TestExtractList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object)
		expectObjectNum int
	}{
		{
			name: "FooList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeFooList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "SampleList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeSampleList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeExtensionList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeUnstructuredList(fakeObjectItemsNum)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("ExtractList", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				objs, err := ExtractList(list)
				if err != nil {
					t.Fatalf("extract list %#v: %v", list, err)
				}
				for i := range objs {
					var (
						o   metav1.Object
						err error
						obj = objs[i]
					)

					if reflect.TypeOf(obj).Kind() == reflect.Struct {
						copy := reflect.New(reflect.TypeOf(obj))
						copy.Elem().Set(reflect.ValueOf(obj))
						o, err = Accessor(copy.Interface())
					} else {
						o, err = Accessor(obj)
					}
					if err != nil {
						t.Fatalf("Accessor object %#v: %v", obj, err)
					}
					delete(expectObjectNames, o.GetName())
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
			t.Run("ExtractListWithAlloc", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				objs, err := ExtractListWithAlloc(list)
				if err != nil {
					t.Fatalf("extract list with alloc: %v", err)
				}
				for i := range objs {
					var (
						o   metav1.Object
						err error
						obj = objs[i]
					)
					if reflect.TypeOf(obj).Kind() == reflect.Struct {
						copy := reflect.New(reflect.TypeOf(obj))
						copy.Elem().Set(reflect.ValueOf(obj))
						o, err = Accessor(copy.Interface())
					} else {
						o, err = Accessor(obj)
					}
					if err != nil {
						t.Fatalf("Accessor object %#v: %v", obj, err)
					}
					delete(expectObjectNames, o.GetName())
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
		})
	}
}

func BenchmarkExtractListItem(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "FooList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ExtractList(tc.list)
				if err != nil {
					b.Fatalf("ExtractList: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkEachListItem(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "FooList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := EachListItem(tc.list, func(object runtime.Object) error {
					return nil
				})
				if err != nil {
					b.Fatalf("EachListItem: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkExtractListItemWithAlloc(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "FooList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ExtractListWithAlloc(tc.list)
				if err != nil {
					b.Fatalf("ExtractListWithAlloc: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkEachListItemWithAlloc(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "FooList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := EachListItemWithAlloc(tc.list, func(object runtime.Object) error {
					return nil
				})
				if err != nil {
					b.Fatalf("EachListItemWithAlloc: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}
