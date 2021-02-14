import unittest


import pyarrow
import ballista
from ballista import lit, col

class TestCase(unittest.TestCase):
    def test_expr(self):
        positive_pressure: ballista.Expression = col("pressure") > 100.0
        self.assertEqual(1,1)

if __name__ == '__main__':
    unittest.main()