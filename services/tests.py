import unittest
import APIv1
import APIv2


class TestAPI1(unittest.TestCase):
    def setup(self):
        APIv1.app.testing = True

    def test_get_24horas(self):
        response = APIv1.app.test_client().get('/servicio/v1/prediccion/24horas')
        self.assertEqual(response.status_code, 200)

    def test_get_48horas(self):
        response = APIv1.app.test_client().get('/servicio/v1/prediccion/48horas')
        self.assertEqual(response.status_code, 200)

    def test_get_72horas(self):
        response = APIv1.app.test_client().get('/servicio/v1/prediccion/72horas')
        self.assertEqual(response.status_code, 200)


class TestAPIV2(unittest.TestCase):
    def setup(self):
        APIv2.app.testing = True

    def test_get_24horas(self):
        response = APIv2.app.test_client().get('/servicio/v2/prediccion/24horas')
        self.assertEqual(response.status_code, 200)

    def test_get_48horas(self):
        response = APIv2.app.test_client().get('/servicio/v2/prediccion/24horas')
        self.assertEqual(response.status_code, 200)

    def test_get_72h(self):
        response = APIv2.app.test_client().get('/servicio/v2/prediccion/72horas')
        self.assertEqual(response.status_code, 200)
