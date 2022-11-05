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
	"fmt"
	goruntime "runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	fakeObjectItemsNum = 1000
	exemptObjectIndex  = fakeObjectItemsNum / 4
)

type FooSpec struct {
	Flied int
}

type FooList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Foo
}

func (s *FooList) DeepCopyObject() runtime.Object { return nil }

// The difference between Sample and Foo is that the pointer of Sample
// is the implementer of runtime.Object, while the Foo struct itself is
// the implementer of runtime.Object. This difference affects the
// behavior of ExtractList.
type Sample struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Spec SampleSpec
}

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

type SampleSpec struct {
	Flied int
}

func (s *Sample) DeepCopyObject() runtime.Object { return nil }

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

func (l RawExtensionList) DeepCopyObject() runtime.Object {
	return nil
}

func getSampleList(numItems int) *SampleList {
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
				Name:      fmt.Sprintf("sample-%d", i),
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

func getRawExtensionList(numItems int) *RawExtensionList {
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
					Name:      fmt.Sprintf("foo-%d", i),
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

func getFooList(numItems int) *FooList {
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
				Name:      fmt.Sprintf("foo-%d", i),
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

func BenchmarkExtractListItem(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "FooList",
			list: getFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: getSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: getRawExtensionList(fakeObjectItemsNum),
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
			list: getFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: getSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: getRawExtensionList(fakeObjectItemsNum),
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
			list: getFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: getSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: getRawExtensionList(fakeObjectItemsNum),
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
			list: getFooList(fakeObjectItemsNum),
		},
		{
			name: "SampleList",
			list: getSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: getRawExtensionList(fakeObjectItemsNum),
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

func TestExtractList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func() (list runtime.Object, firstObjectPointer interface{})
		shouldBeCleared bool
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			shouldBeCleared: true,
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			shouldBeCleared: false,
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getRawExtensionList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			shouldBeCleared: true,
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
					select {
					case <-ctx.Done():
						// test-case already finished
						return
					default:
					}
					if tc.shouldBeCleared {
						close(cleared)
						return
					}
					t.Errorf("object %#v unepxect cleared", obj)
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

func TestEachList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func() (list runtime.Object, firstObjectPointer interface{})
		shouldBeCleared bool
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			shouldBeCleared: false,
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			shouldBeCleared: false,
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getRawExtensionList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
			// NOTE:
			// although EachListItem takes the address of the elements of items,
			// but it uses items.Object instead of items.
			// so the items slice will be cleared.
			shouldBeCleared: true,
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
					select {
					case <-ctx.Done():
						// test-case already finished
						return
					default:
					}
					if tc.shouldBeCleared {
						close(cleared)
						return
					}
					t.Errorf("object %#v unepxect cleared", obj)
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

func TestExtractListWithAlloc(t *testing.T) {
	tests := []struct {
		name         string
		generateFunc func() (list runtime.Object, firstObjectPointer interface{})
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getRawExtensionList(fakeObjectItemsNum)
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
				t.Errorf("ExtractList %s can't be cleared", tc.name)
			}
		})
	}
}

func TestEachListWithAlloc(t *testing.T) {
	tests := []struct {
		name         string
		generateFunc func() (list runtime.Object, firstObjectPointer interface{})
	}{
		{
			name: "FooList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getFooList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "SampleList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getSampleList(fakeObjectItemsNum)
				return out, &out.Items[0]
			},
		},
		{
			name: "RawExtensionList",
			generateFunc: func() (list runtime.Object, firstObjectPointer interface{}) {
				out := getRawExtensionList(fakeObjectItemsNum)
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
				t.Errorf("EachListItem %s can't be cleared", tc.name)
			}
		})
	}
}
