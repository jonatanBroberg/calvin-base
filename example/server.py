import socket
import time
import cv2

host = "localhost"
port = 8090
video = cv2.VideoCapture("short.mp4")

while True:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    success, image = video.read()
    if success:
        ret, jpeg = cv2.imencode(".jpg", image)
        s.sendall(jpeg.tobytes())
    s.close()
