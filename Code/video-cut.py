# -*- coding:utf-8 -*-
import os
import sys
from PIL import Image
import io
import cv2
import numpy as np
from hdfs import InsecureClient
# Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)

hdfs_url = 'http://gpu20:50070'
client_hdfs = InsecureClient(hdfs_url)

video_path_local = 'roman.mp4'
image_path_hdfs = '/David/img2/'

start_num = 5000
end_num = 9000
if_print= True

if __name__ == '__main__':
    start_num = int(sys.argv[1])
    end_num = int(sys.argv[2])
    print(start_num, end_num)
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


'''

# -*- coding:utf-8 -*-

import os
import sys
from PIL import Image
import io
import cv2
import numpy as np

start_num = 5000
end_num = 5010
if_print= True
video_path_local = 'Test.mp4'

if __name__ == "__main__":
    start_num = int(sys.argv[1])
    end_num = int(sys.argv[2])
    print(start_num, end_num)
    video = cv2.VideoCapture(video_path_local)
    video.set(propId = cv2.CAP_PROP_POS_FRAMES, value = start_num)

    for i in range(start_num, end_num+1):
        success, capture = video.read()
        im = Image.fromarray(capture)
        with io.BytesIO() as output:
            im.save(output, format="PNG")
            contents = output.getvalue()
        if success:
            img_name = '/Users/david/Downloads/img/{:06d}-test.png'.format(i)
            try:
                with open(img_name, 'wb') as fw:
                    fw.write(contents)
                if if_print:
                    print('Successfully transfer image to HDFS.', img_name)
            except Exception as e:
                if if_print:
                    print(e)
                    print('Error in transfering image file to HDFS.', img_name)
        else:
            print('Failed', img_name)

    video.release()
'''
