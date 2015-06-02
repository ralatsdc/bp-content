# ====================================================================
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# ====================================================================

import sys, lucene, unittest
from PyLuceneTestCase import PyLuceneTestCase

from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, TextField
from org.apache.lucene.index import Term
from org.apache.lucene.search import BooleanClause, BooleanQuery, TermQuery
from org.apache.lucene.util import Version


class BooleanOrTestCase(PyLuceneTestCase):
    """
    Unit tests ported from Java Lucene
    """

    def __init__(self, *args):

        super(BooleanOrTestCase, self).__init__(*args)

        self.FIELD_T = "T"
        self.FIELD_C = "C"

        self.t1 = TermQuery(Term(self.FIELD_T, "files"))
        self.t2 = TermQuery(Term(self.FIELD_T, "deleting"))
        self.c1 = TermQuery(Term(self.FIELD_C, "production"))
        self.c2 = TermQuery(Term(self.FIELD_C, "optimize"))

        self.searcher = None

    def setUp(self):
        super(BooleanOrTestCase, self).setUp()

        # add the doc to a ram index
        writer = self.getWriter(analyzer=StandardAnalyzer(Version.LUCENE_CURRENT))
        d = Document()
        d.add(Field(self.FIELD_T, "Optimize not deleting all files",
                    TextField.TYPE_STORED))
        d.add(Field(self.FIELD_C,
                    "Deleted When I run an optimize in our production environment.",
                    TextField.TYPE_STORED))

        writer.addDocument(d)
        writer.close()

        self.searcher = self.getSearcher()

    def search(self, q):
        return self.searcher.search(q, 50).totalHits

    def testElements(self):

        self.assertEqual(1, self.search(self.t1))
        self.assertEqual(1, self.search(self.t2))
        self.assertEqual(1, self.search(self.c1))
        self.assertEqual(1, self.search(self.c2))

    def testFlat(self):

        q = BooleanQuery()
        q.add(BooleanClause(self.t1, BooleanClause.Occur.SHOULD))
        q.add(BooleanClause(self.t2, BooleanClause.Occur.SHOULD))
        q.add(BooleanClause(self.c1, BooleanClause.Occur.SHOULD))
        q.add(BooleanClause(self.c2, BooleanClause.Occur.SHOULD))
        self.assertEqual(1, self.search(q))

    def testParenthesisMust(self):

        q3 = BooleanQuery()
        q3.add(BooleanClause(self.t1, BooleanClause.Occur.SHOULD))
        q3.add(BooleanClause(self.t2, BooleanClause.Occur.SHOULD))
        q4 = BooleanQuery()
        q4.add(BooleanClause(self.c1, BooleanClause.Occur.MUST))
        q4.add(BooleanClause(self.c2, BooleanClause.Occur.MUST))
        q2 = BooleanQuery()
        q2.add(q3, BooleanClause.Occur.SHOULD)
        q2.add(q4, BooleanClause.Occur.SHOULD)
        self.assertEqual(1, self.search(q2))

    def testParenthesisMust2(self):

        q3 = BooleanQuery()
        q3.add(BooleanClause(self.t1, BooleanClause.Occur.SHOULD))
        q3.add(BooleanClause(self.t2, BooleanClause.Occur.SHOULD))
        q4 = BooleanQuery()
        q4.add(BooleanClause(self.c1, BooleanClause.Occur.SHOULD))
        q4.add(BooleanClause(self.c2, BooleanClause.Occur.SHOULD))
        q2 = BooleanQuery()
        q2.add(q3, BooleanClause.Occur.SHOULD)
        q2.add(q4, BooleanClause.Occur.MUST)
        self.assertEqual(1, self.search(q2))

    def testParenthesisShould(self):

        q3 = BooleanQuery()
        q3.add(BooleanClause(self.t1, BooleanClause.Occur.SHOULD))
        q3.add(BooleanClause(self.t2, BooleanClause.Occur.SHOULD))
        q4 = BooleanQuery()
        q4.add(BooleanClause(self.c1, BooleanClause.Occur.SHOULD))
        q4.add(BooleanClause(self.c2, BooleanClause.Occur.SHOULD))
        q2 = BooleanQuery()
        q2.add(q3, BooleanClause.Occur.SHOULD)
        q2.add(q4, BooleanClause.Occur.SHOULD)
        self.assertEqual(1, self.search(q2))


if __name__ == "__main__":
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    if '-loop' in sys.argv:
        sys.argv.remove('-loop')
        while True:
            try:
                unittest.main()
            except:
                pass
    else:
         unittest.main()
