#!/usr/bin/env python3

import unittest

from stam.stam import AnnotationStore, Offset, AnnotationData, AnnotationDataBuilder, Selector, TextResource, DataKey, DataValue, AnnotationDataSet, Annotation


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
        self.assertEqual(str(key), "pos")
        data = dataset.annotationdata("D1")
        self.assertIsInstance( data, AnnotationData)
        self.assertTrue(data.has_id("D1"))

    def test_iter_data(self):
        """Iterates over the data in an annotation"""
        annotation = self.store.annotation("A1")
        self.assertIsInstance(annotation, Annotation)
        count = 0
        for annotationdata in annotation:
            count += 1
            #we can test in loop body because we only have one:
            self.assertIsInstance(annotationdata, AnnotationData)
            self.assertTrue(annotationdata.has_id("D1"))
            self.assertTrue(annotationdata.key().has_id("pos")) #this is the most performant in comparisons, it doesn't make a copy of the key
            self.assertEqual(str(annotationdata.key()), "pos") #force a string

            self.assertEqual(annotationdata.value().get(), "noun")
            self.assertTrue(annotationdata.test_value(DataValue("noun"))) #this is the most performant in comparisons, it doesn't make a copy of the value
            self.assertEqual(str(annotationdata.value()), "noun") #force a string
        self.assertEqual(count,1)

    def test_resource_text(self):
        """Get the text of an entire resource"""
        resource = self.store.resource("testres")
        self.assertIsInstance(resource, TextResource)
        self.assertEqual(str(resource), "Hello world")

    def test_annotation_text(self):
        """Get the text of an annotation"""
        annotation = self.store.annotation("A1")
        count = 0
        for text in annotation.text():
            count += 1
            self.assertEqual(text, "world")
        self.assertEqual(count,1)

        #shortcut, will concatenate multiple text slices if needed
        self.assertEqual(str(annotation), "world")
            
    def test_annotation_textselections(self):
        """Get the textselections of an annotation"""
        annotation = self.store.annotation("A1")
        count = 0
        for textselection in annotation.textselections():
            count += 1
            self.assertEqual(str(textselection), "world")
            self.assertEqual(textselection.resource(), self.store.resource("testres"))
        self.assertEqual(count,1)


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
        self.assertEqual(str(key), "pos")
        data = dataset.annotationdata("D1")
        self.assertIsInstance( data, AnnotationData)
        self.assertTrue(data.has_id("D1"))

if __name__ == "__main__":
    unittest.main()

