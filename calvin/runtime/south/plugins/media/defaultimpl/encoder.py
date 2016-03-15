import cv2
import numpy
import base64


def json_numpy_obj_hook(dct):
    """Decodes a previously encoded numpy ndarray with proper shape and dtype.

    :param dct: (dict) json encoded ndarray
    :return: (ndarray) if input was an encoded ndarray
    """
    if isinstance(dct, dict) and 'ndarray' in dct:
        data = base64.b64decode(dct['ndarray'])
        return numpy.frombuffer(data, dct['dtype']).reshape(dct['shape'])
    return dct


class Encoder(object):

    """
    Encoder object
    """
    def encode(self, frame, encoding):
        image = json_numpy_obj_hook(frame)
        ret, jpg = cv2.imencode(".jpg", image)
        return jpg.tobytes()
