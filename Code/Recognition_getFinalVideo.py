# -*- coding:utf-8 -*-
import datetime
from hdfs import InsecureClient
from pyspark import SparkContext
import numpy as np
import io 
from PIL import Image
import os
import tensorflow as tf
import cv2
import dlib
os.environ['PYSPARK_PYTHON']='/home/hduser/anaconda3/bin/python'
os.environ['SPARK_HOME']='/opt/spark-2.4.0-bin-hadoop2.7'



#client = HdfsClient(hosts = u'localhost:50070')
#print(client.list_status('/'))
print('ooooooooooo')


#hdfs_url = Client('http://localhost:50070')
#print(hdfs_url.list_status('/'))
#client_hdfs = InsecureClient(hdfs_url)


#
#
#
def cutvideo(akk,enterpath='tomcat/website_1/roman.mp4',outpath='tomcat/website_1/result3000-7000.mp4',thr=0.7,nstart=3000): #path: the original video path, arr: the frame position
    videoCapture = cv2.VideoCapture(enterpath)
    #videoCapture2 = cv2.VideoCapture(enterpath)
    if (videoCapture.isOpened()):  
        print ('Open')  
    else:  
        print ('Fail to open!')
    fps = int(videoCapture.get(cv2.CAP_PROP_FPS))
    nfps=videoCapture.get(cv2.CAP_PROP_FRAME_COUNT)
    
    print('the number of frames:',len(akk))
    
    ak=[]
    ak2=[]
    n=nstart#the point of start of the video
    for i in akk:
        if float(i)>thr:
            ak.append(n)
        else:
            ak2.append(n)
        n+=1

    m=nstart
    
    #ak=buqiarr(ak,5)
    
    
 
   
    size = (int(videoCapture.get(cv2.CAP_PROP_FRAME_WIDTH)),int(videoCapture.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    print('size:',size)
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    videoWriter = cv2.VideoWriter(outpath, fourcc, fps, size) 
    videoWriter2 =cv2.VideoWriter('tomcat/website_1/error.mp4', fourcc, fps, size) 
    for i in ak:
        if m % 100 == 0:
            print('the current frame:',m)
        m+=1
        if i > nfps:
            continue
        videoCapture.set(cv2.CAP_PROP_POS_FRAMES,i)
        ret,frame = videoCapture.read()
        videoWriter.write(frame)
        
    for i in ak2:
        if m % 100 == 0:
            print('the current frame:',m)
        m+=1
        if i > nfps:
            continue
        videoCapture.set(cv2.CAP_PROP_POS_FRAMES,i)
        ret,frame = videoCapture.read()
        videoWriter2.write(frame)
        
    videoCapture.release() 
    videoWriter.release()
    videoWriter2.release()
    print('cut video end')
    

#    
def buqiarr(arr,fps=5):  
    brr=list(arr)
    crr=list(brr)   
    n=0    
    for i in range(len(crr)-1):
        if crr[i+1]-crr[i] > fps :
            continue
        if crr[i+1]-crr[i] == 1:
            continue
        for j in range(crr[i]+1,crr[i+1]):
            brr.insert(i+n+1,j)
            n+=1
    return brr

#
def preprocess(arr):
    brr=set(arr)
    crr=list(brr)
    crr.sort()
    return  crr

#
def addarr(arr,fps=23,maxlen=56000):
    brr=list(arr)
    crr=list(brr)   
    n=0 
    for m in range(max(crr[0]-fps,0),crr[0]):
        brr.insert(0+n,m)
        n+=1
    for i in range(1,len(crr)-1):
        if crr[i]-crr[i-1] != 1 :
            for j in range(crr[i]-fps,crr[i]-1):
                brr.insert(i+n,j)
                n+=1
        if crr[i+1]-crr[i] != 1 :
            for j in range(crr[i]+1,crr[i]+fps):
                brr.insert(i+n+1,j)
                n+=1            
    for m in range(crr[len(crr)-1]+1,min(crr[i]+fps,maxlen)):
        brr.insert(i+n+2,m)
        n+=1
    return brr

#
def cancel(arr,fps=23):
    brr=list(arr)
    crr=list(brr)
    n=0
    sta=crr[0]
    end=crr[0]
    staid = 0
    endid = 0
    
    if crr[1]-crr[0]!=1:
        del brr[0]     
        n+=1

    for i in range(1,len(crr)-1):
        if crr[i]-crr[i-1] != 1 :
            sta=crr[i]
            staid=i
        if crr[i+1]-crr[i] != 1:
            end = crr[i]
            endid = i
            if end - sta < 2*fps-1:
                del brr[staid-n:endid-n+1]
                n=n+endid-staid+1     
    if crr[len(crr)-1]-crr[len(crr)-2]!=1:
        del brr[len(crr)-1-n]
        n+=1
  
    return brr

def printData(x):
    print (x[0])
    print ('lineline--------------')
    for line in x[1].split('\n'):
        print ('line--------------',line)
        
        
#
def filetolist(filepath='prob_list2019-04-18-12-36-20',thr=0.7):
    
    ak=[]
    n=0
    line = []
    print('filetolist start')
    #file = client.open("/prob_list2019-04-18-12-36-20/part-00000")
    #print(file)
    #for oo in file:
        #print(oo)
        
    sc = SparkContext(appName="testyly")
    #images = sc.binaryFiles("hdfs://gpu20:9000/David/img/003[0-1]*")
    #image_to_array = lambda rawdata: np.asarray(Image.open(io.BytesIO(rawdata)))
    #imagerdd = images.values().map(image_to_array)
    #timestring=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    #imagerdd1 = imagerdd.map(get_prob).saveAsTextFile("hdfs://gpu20:9000/prob_list"+timestring)
    
    a=sc.textFile("hdfs://gpu20:9000/"+filepath+"/part-000*")
    b=a.collect()
    print('success')
    print('b:',b)

    #with hdfs_url.read('/prob_list4.txt/part-00000') as reader:
        #print('kkkkkkkkkkkkkkkkkkkkkkk1') 
        #kk=dir(reader)
        #print('kkkkkkkkkkkkkkkkkkkkkkk',kk)      

    #rootdir = filepath
    #listx = os.listdir(rootdir)
    #print(listx)
    #print(len(listx))
    
    #a.foreach(printData)
    #dataRdd.foreachPartition(printDataPartition)


    print(len(b))
    
    for i in b:
        if float(i)>0.97:
            ak.append(n)
        n+=1
    print('ak',ak)
    
    print('ori_ak:',len(ak))
    
    '''
    for i in range(0,len(listx)-1):
        if i <10:
            path=filepath+'/part-0000'+str(i)
        else:
            path = filepath+'/part-000'+str(i)
            #os.path.join(rootdir,listx[i])
        print(path)
        #if os.path.isfile(path):
        with hdfs_url.read(path) as f: 
         #print('isfile:')
            #f=open(path)
            for i in f:
                if float(i)>thr:
                    ak.append(n)
                n+=1
            #print('filesize:',len(f))

            f.close()
        #print('rightsize:',len(ak))
    print(ak)
    print(len(ak))
    '''
    return ak



#____-------------------------------------------------------








def get_prob(image_df):
    tf.reset_default_graph()
    #my_faces_path = './HB'
    #other_faces_path = './notHB'
    size = 64
    
    x = tf.placeholder(tf.float32, [None, size, size, 3])
    y_ = tf.placeholder(tf.float32, [None, 2])
    
    keep_prob_5 = tf.placeholder(tf.float32)
    keep_prob_75 = tf.placeholder(tf.float32)
    
    def weightVariable(shape):
        init = tf.random_normal(shape, stddev=0.01)
        return tf.Variable(init)
    
    def biasVariable(shape):
        init = tf.random_normal(shape)
        return tf.Variable(init)
    
    def conv2d(x, W):
        return tf.nn.conv2d(x, W, strides=[1,1,1,1], padding='SAME')
    
    def maxPool(x):
        return tf.nn.max_pool(x, ksize=[1,2,2,1], strides=[1,2,2,1], padding='SAME')
    
    def dropout(x, keep):
        return tf.nn.dropout(x, keep)
    
    def cnnLayer():
        # 第一层
        W1 = weightVariable([3,3,3,32]) # 卷积核大小(3,3)， 输入通道(3)， 输出通道(32)
        b1 = biasVariable([32])
        # 卷积
        conv1 = tf.nn.relu(conv2d(x, W1) + b1)
        # 池化
        pool1 = maxPool(conv1)
        # 减少过拟合，随机让某些权重不更新
        drop1 = dropout(pool1, keep_prob_5)
    
        # 第二层
        W2 = weightVariable([3,3,32,64])
        b2 = biasVariable([64])
        conv2 = tf.nn.relu(conv2d(drop1, W2) + b2)
        pool2 = maxPool(conv2)
        drop2 = dropout(pool2, keep_prob_5)
    
        # 第三层
        W3 = weightVariable([3,3,64,64])
        b3 = biasVariable([64])
        conv3 = tf.nn.relu(conv2d(drop2, W3) + b3)
        pool3 = maxPool(conv3)
        drop3 = dropout(pool3, keep_prob_5)
    
        # 全连接层
        Wf = weightVariable([8*16*32, 512])
        bf = biasVariable([512])
        drop3_flat = tf.reshape(drop3, [-1, 8*16*32])
        dense = tf.nn.relu(tf.matmul(drop3_flat, Wf) + bf)
        dropf = dropout(dense, keep_prob_75)
    
        # 输出层
        Wout = weightVariable([512,2])
        bout = biasVariable([2])
        out = tf.add(tf.matmul(dropf, Wout), bout)
        
        out = tf.nn.softmax(out)
        return out
    
    output = cnnLayer()  
    predict = tf.argmax(output, 1)  
    
    saver = tf.train.Saver()  
    sess = tf.Session()  
    saver.restore(sess, tf.train.latest_checkpoint('/home/hduser/FaceRecognition-tensorflow-master'))  
       
    def is_my_face(image):  
        res = sess.run(output, feed_dict={x: [image/255.0], keep_prob_5:1.0, keep_prob_75: 1.0})  
        return res[0][1]
    
    
    
    detector = dlib.get_frontal_face_detector()
    

    img = image_df
      

    gray_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    dets = detector(gray_image, 1)
    max_prob = 0


    for i, d in enumerate(dets):
        x1 = d.top() if d.top() > 0 else 0
        y1 = d.bottom() if d.bottom() > 0 else 0
        x2 = d.left() if d.left() > 0 else 0
        y2 = d.right() if d.right() > 0 else 0
        face = img[x1:y1,x2:y2]
        # 调整图片的尺寸
        face = cv2.resize(face, (size,size))
        
        flag = is_my_face(face)
        if flag>max_prob:
            max_prob = flag
   
    sess.close()
    #print('pppppppppppppppppppppppppppp:',max_prob)
    return(max_prob)

if __name__=="__main__":
    sc = SparkContext(appName="VIDEO CLIPPING FOR LOVE")
    '''image1 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/003[0-3]*")
    image2 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/003[4-6]*")
    image3 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/003[7-9]*")
    image4 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/004[0-3]*")
    image5 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/004[4-6]*")
    image6 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/004[7-9]*")
    image7 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/005[0-3]*")
    image8 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/005[4-6]*")
    image9 = sc.binaryFiles("hdfs://gpu20:9000/David/img2/005[7-9]*")
    images = (((((((image1.union(image2)).union(image3)).union(image4)).union(image5)).union(image6)).union(image7)).union(image8)).union(image9)
    '''
    images = sc.binaryFiles("hdfs://gpu20:9000/David/img2/*")
    image_to_array = lambda rawdata: np.asarray(Image.open(io.BytesIO(rawdata)))
    #imagerdd1 = image1.values().map(image_to_array)
    #imagerdd2 = image2.values().map(image_to_array)
    #imagerdd3 = image3.values().map(image_to_array)
    #imagerdd4 = image4.values().map(image_to_array)
    #imagerdd5 = image5.values().map(image_to_array)
    #imagerdd6 = image6.values().map(image_to_array)
    #imagerdd7 = image7.values().map(image_to_array)
    #imagerdd8 = image8.values().map(image_to_array)
    #imagerdd9 = image9.values().map(image_to_array)
    imagerdd = images.values().map(image_to_array)
    timestring=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    #imagerdd1 = imagerdd.map(get_prob).collect()
    #list1 = imagerdd1.map(get_prob).collect()
    #list2 = imagerdd2.map(get_prob).collect()
    #list3 = imagerdd3.map(get_prob).collect()
    #list4 = imagerdd4.map(get_prob).collect()
    #list5 = imagerdd5.map(get_prob).collect()
    #list6 = imagerdd6.map(get_prob).collect()
    #list7 = imagerdd7.map(get_prob).collect()
    #list8 = imagerdd8.map(get_prob).collect()
    #list9 = imagerdd9.map(get_prob).collect()
    #lists = list1+list2+list3+list4+list5+list6+list7+list8+list9
    lists = imagerdd.map(get_prob).collect()
    ##print(lists)
    #saveAsTextFile("hdfs://gpu20:9000/"+timestring)
    #list1 = imagerdd.map(get_prob).collect()
    #imagerdd1 = imagerdd.map(get_prob).saveAsTextFile("hdfs://gpu20:9000/prob_list"+timestring)
    #print(list1)

    oldvideo_path_local = '3000-6000.mp4'
    #listfile_path='prob_list2019-04-18-15-31-27'
    newvideo_path_local = 'tomcat/website_1/result3000-6000.mp4'

    #print('enter cutvideo')
    ##cutvideo(lists,oldvideo_path_local,newvideo_path_local,0.7,0)
    #cutvideo()
    print('cutvideo complete')
