#!/usr/bin/env python3

import unittest

import stam.stam


class Test1(unittest.TestCase):
    def setUp(self):
        self.store = stam.AnnotationStore(id="test")
        self.store.add_resource(id="testres", text="Hello world")
        dataset = self.store.add_annotationset(id="testdataset")
        dataset.add_key("pos")
        dataset.add_data("pos","noun","D1")

    def test_sanity_1(self):
        self.assertIsInstance( self.store, stam.AnnotationStore)
        self.assertEqual(self.store.id, "test")

    def test_sanity_2(self):
        resource = self.store.resource("testres")
        self.assertIsInstance( resource, stam.TextResource)
        self.assertEqual(resource.id, "testres")
        self.assertTrue(resource.has_id("testres")) #quicker than the above (no copy)

    def test_sanity_3(self):
        dataset = self.store.annotationset("testdataset")
        self.assertIsInstance( dataset, stam.AnnotationDataSet)
        key = dataset.key("pos")
        self.assertIsInstance( key, stam.DataKey)
        self.assertTrue(str(key), "pos")
        data = dataset.annotationdata("D1")
        self.assertIsInstance( data, stam.AnnotationData)
        self.assertTrue(data.has_id("D1"))

if __name__ == "__main__":
    unittest.main()

