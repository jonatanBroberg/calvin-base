import cv2
import numpy
import base64


class Video(object):

    """
    Read frames from video
    """

    def __init__(self, device, width, height):
        """
        Initialize video capture
        """
        self.cap = cv2.VideoCapture(device)
        self.cap.set(cv2.cv.CV_CAP_PROP_FRAME_WIDTH, width)
        self.cap.set(cv2.cv.CV_CAP_PROP_FRAME_HEIGHT, height)

    def get_image(self):
        """
        Captures an image
        returns: frame, None if no frame
        """
        ret, frame = self.cap.read()
        if ret:
            #ret, jpeg = cv2.imencode(".jpg", frame)
            #if ret:
            #    data = numpy.array(jpeg)
            #    return data.tostring()
            data_b64 = base64.b64encode(frame.data)
            return {
                "ndarray": data_b64,
                "dtype": str(frame.dtype),
                "shape": frame.shape
            }

    def close(self):
        """
        Uninitialize camera
        """
        self.cap.release()
