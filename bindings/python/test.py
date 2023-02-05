#!/usr/bin/env python3

from os import environ
import os.path
import unittest

from stam.stam import AnnotationStore, Offset, AnnotationData, AnnotationDataBuilder, Selector, TextResource, DataKey, DataValue, AnnotationDataSet, Annotation, StamError, TextSelection, Cursor


class Test0(unittest.TestCase):
    def test_sanity_no_constructors(self):
        """Most stam types are references and can't be instantiated directly ('No constructor defined')"""
        with self.assertRaises(TypeError):
            Annotation()
        with self.assertRaises(TypeError):
            AnnotationDataSet()
        with self.assertRaises(TypeError):
            AnnotationData()
        with self.assertRaises(TypeError):
            TextResource()
        with self.assertRaises(TypeError):
            DataKey()
        with self.assertRaises(TypeError):
            TextSelection()

    def test_offset(self):
        offset = Offset.simple(0,5)
        self.assertEqual( offset.begin(), Cursor(0))
        self.assertEqual( offset.end(), Cursor(5))

    def test_offset_endaligned(self):
        offset = Offset(Cursor(0) , Cursor(0, endaligned=True) )
        self.assertEqual( offset.begin(), Cursor(0))
        self.assertEqual( offset.end(), Cursor(0, endaligned=True)) 

        offset2 = Offset.whole() #shortcut
        self.assertEqual( offset, offset2)

class Test1(unittest.TestCase):
    def setUp(self):
        """Create some data from scratch"""
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

    def test_sanity_4_id_error(self):
        """Exceptions should be raised if IDs don't exist"""
        with self.assertRaises(StamError):
            self.store.annotationset("non-existent-id")
        with self.assertRaises(StamError):
            self.store.annotation("non-existent-id")
        with self.assertRaises(StamError):
            self.store.resource("non-existent-id")

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
            self.assertTrue(annotationdata.annotationset().has_id("testdataset"))
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

    def test_resource_text_slice(self):
        """Get the text of a slice of a resource"""
        resource = self.store.resource("testres")
        text = resource.text(Offset.simple(0,5))
        self.assertEqual( text, "Hello")

    def test_resource_text_slice_outofbounds(self):
        """Get the text of a slice of a resource"""
        resource = self.store.resource("testres")
        with self.assertRaises(StamError):
            resource.text(Offset.simple(0,999))

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

    def test_annotationset_iter(self):
        """Iterate over all data in an annotationset"""
        annotationset = self.store.annotationset("testdataset")
        count = 0
        for annotationdata in annotationset:
            count += 1
            #we can test in loop body because we only have one:
            self.assertIsInstance(annotationdata, AnnotationData)
            self.assertTrue(annotationdata.has_id("D1"))
            self.assertTrue(annotationdata.key().has_id("pos")) #this is the most performant in comparisons, it doesn't make a copy of the key
            self.assertEqual(str(annotationdata.key()), "pos") #force a string
            self.assertEqual(annotationdata.annotationset(), annotationset)

            self.assertEqual(annotationdata.value().get(), "noun")
            self.assertTrue(annotationdata.test_value(DataValue("noun"))) #this is the most performant in comparisons, it doesn't make a copy of the value
            self.assertEqual(str(annotationdata.value()), "noun") #force a string
        self.assertEqual(count,1)

    def test_annotationset_iter_keys(self):
        """Iterate over all keys in an annotationset"""
        annotationset = self.store.annotationset("testdataset")
        count = 0
        for key in annotationset.keys():
            count += 1
            #we can test in loop body because we only have one:
            self.assertIsInstance(key, DataKey)
            self.assertTrue(key.has_id("pos")) #this is the most performant in comparisons, it doesn't make a copy of the key
            self.assertEqual(key.annotationset(), annotationset)
        self.assertEqual(count,1)


class Test2(unittest.TestCase):
    def setUp(self):
        """Create some data from scratch"""
        #this is the very same data as in Test1, but constructed more implicitly via annotate()
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

    def test_serialisation_file(self):
        TMPDIR = environ.get('TMPDIR', "/tmp")
        filename = os.path.join(TMPDIR, "testoutput.stam.json")
        #doesn't test the actual output!
        self.store.to_file(filename)

    def test_serialisation_string(self):
        self.assertTrue(self.store.to_string()) #doesn't test the actual output!
 
EXAMPLE3JSON = """{
    "@type": "AnnotationStore",
    "annotationsets": {
        "@type": "AnnotationDataSet",
        "@id": "testdataset",
        "keys": [
            {
              "@type": "DataKey",
              "@id": "pos"
            }
        ],
        "data": [
            {
                "@type": "AnnotationData",
                "@id": "D1",
                "key": "pos",
                "value": {
                    "@type": "String",
                    "value": "noun"
                }
            }
        ]
    },
    "resources": [{
        "@id": "testres",
        "text": "Hello world"
    }],
    "annotations": [{
        "@type": "Annotation",
        "@id": "A1",
        "target": {
            "@type": "TextSelector",
            "resource": "testres",
            "offset": {
                "begin": {
                    "@type": "BeginAlignedCursor",
                    "value": 6
                },
                "end": {
                    "@type": "BeginAlignedCursor",
                    "value": 11
                }
            }
        },
        "data": [{
            "@type": "AnnotationData",
            "@id": "D1",
            "set": "testdataset"
        }]
    }]
}"""

def common_sanity(self): 
    self.assertIsInstance( self.store, AnnotationStore)
    self.assertEqual(self.store.annotations_len(), 1)
    self.assertEqual(self.store.annotationsets_len(), 1)
    self.assertEqual(self.store.resources_len(), 1)

    resource = self.store.resource("testres")
    self.assertIsInstance( resource, TextResource)
    self.assertEqual(resource.id, "testres")
    self.assertTrue(resource.has_id("testres")) #quicker than the above (no copy)

    dataset = self.store.annotationset("testdataset")
    self.assertIsInstance( dataset, AnnotationDataSet)
    key = dataset.key("pos")
    self.assertIsInstance( key, DataKey)
    self.assertEqual(str(key), "pos")
    data = dataset.annotationdata("D1")
    self.assertIsInstance( data, AnnotationData)
    self.assertTrue(data.has_id("D1"))

class Test3a(unittest.TestCase):
    def test_parse_file(self):
        TMPDIR = environ.get('TMPDIR', "/tmp")
        filename = os.path.join(TMPDIR, "test.stam.json")
        with open(filename, 'w',encoding='utf-8') as f:
            f.write(EXAMPLE3JSON)
        self.store = AnnotationStore(file=filename)

        #test all sanity
        common_sanity(self)


class Test3b(unittest.TestCase):
    def test_parse_file(self):
        self.store = AnnotationStore(string=EXAMPLE3JSON)

        #test all sanity
        common_sanity(self)

if __name__ == "__main__":
    unittest.main()

