# -*- coding:utf-8 -*-
import os
import sys
from PIL import Image
import io
import cv2
import numpy as np
from hdfs import InsecureClient

from pyspark import SparkContext, SparkConf
APP_NAME = "cut-video"
MASTER_IP = 'yarn'
conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER_IP)
sc = SparkContext(conf=conf)


# Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)

hdfs_url = 'http://gpu20:50070'
client_hdfs = InsecureClient(hdfs_url)

video_path_local = 'roman.mp4'
image_path_hdfs = '/David/img2/'

start_num = 5000
end_num = 9000
if_print= True

if __name__ == 'main':
    start_num = int(sys.argv[1])
    end_num = int(sys.argv[2])

    video = cv2.VideoCapture(video_path_local)

    # for i in range(start_num-1):
    #     video.read()
    video.set(propId = cv2.CAP_PROP_POS_FRAMES, value=start_num)

    for i in range(start_num, end_num+1):
        success, capture = video.read()
        img_name = '/David/img2/{:06d}.png'.format(i)
        if success:
            contents = []
            im = Image.fromarray(capture)
            with io.BytesIO() as output:
                im.save(output, format="PNG")
                contents = output.getvalue()
            try:
                with client_hdfs.write(img_name) as writer:
                    writer.write(contents)
                if if_print:
                    print('Successfully transfer image to HDFS.', img_name)
            except Exception as e:
                if if_print:
                    print(e)
                    print('Error in transfering image file to HDFS.', img_name)
        else:
            print('Failed', img_name)

    video.release()
