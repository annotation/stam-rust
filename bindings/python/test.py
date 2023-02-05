#!/usr/bin/env python3

import unittest

from stam.stam import AnnotationStore, Offset, AnnotationData, AnnotationDataBuilder, Selector, TextResource, DataKey, DataValue, AnnotationDataSet


class Test1(unittest.TestCase):
    def setUp(self):
        self.store = AnnotationStore(id="test")
        resource = self.store.add_resource(id="testres", text="Hello world")
        dataset = self.store.add_annotationset(id="testdataset")
        dataset.add_key("pos")
        data = dataset.add_data("pos","noun","D1")
        self.store.annotate(id="A1", 
                            target=Selector.text(resource, Offset.simple(6,11)),
                            data=[AnnotationDataBuilder.link(data)])

    def test_sanity_1(self):
        self.assertIsInstance( self.store, AnnotationStore)
        self.assertEqual(self.store.id, "test")
        self.assertEqual(self.store.annotations_len(), 1)
        self.assertEqual(self.store.annotationsets_len(), 1)
        self.assertEqual(self.store.resources_len(), 1)

    def test_sanity_2(self):
        resource = self.store.resource("testres")
        self.assertIsInstance( resource, TextResource)
        self.assertEqual(resource.id, "testres")
        self.assertTrue(resource.has_id("testres")) #quicker than the above (no copy)

    def test_sanity_3(self):
        dataset = self.store.annotationset("testdataset")
        self.assertIsInstance( dataset, AnnotationDataSet)
        key = dataset.key("pos")
        self.assertIsInstance( key, DataKey)
        self.assertTrue(str(key), "pos")
        data = dataset.annotationdata("D1")
        self.assertIsInstance( data, AnnotationData)
        self.assertTrue(data.has_id("D1"))

class Test2(unittest.TestCase):
    def setUp(self):
        self.store = AnnotationStore(id="test")
        resource = self.store.add_resource(id="testres", text="Hello world")
        self.store.annotate(id="A1", 
                            target=Selector.text(resource, Offset.simple(6,11)),
                            data=[AnnotationDataBuilder(id="D1", key="pos", value="noun", annotationset="testdataset")])

    def test_sanity_1(self):
        self.assertIsInstance( self.store, AnnotationStore)
        self.assertEqual(self.store.id, "test")
        self.assertEqual(self.store.annotations_len(), 1)
        self.assertEqual(self.store.annotationsets_len(), 1)
        self.assertEqual(self.store.resources_len(), 1)

    def test_sanity_2(self):
        resource = self.store.resource("testres")
        self.assertIsInstance( resource, TextResource)
        self.assertEqual(resource.id, "testres")
        self.assertTrue(resource.has_id("testres")) #quicker than the above (no copy)

    def test_sanity_3(self):
        dataset = self.store.annotationset("testdataset")
        self.assertIsInstance( dataset, AnnotationDataSet)
        key = dataset.key("pos")
        self.assertIsInstance( key, DataKey)
        self.assertTrue(str(key), "pos")
        data = dataset.annotationdata("D1")
        self.assertIsInstance( data, AnnotationData)
        self.assertTrue(data.has_id("D1"))

if __name__ == "__main__":
    unittest.main()

